package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/alecthomas/kingpin"
	"github.ibm.com/Yacov-Manevich/ARMA/node"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path"
	"runtime/debug"
)

var defaultConfig = `
Parties:
  - ID: 1
    AssemblerEndpoint: "127.0.0.1:7050"
    ConsenterEndpoint: "127.0.0.1:7051"
    RouterEndpoint: "127.0.0.1:7052"
    BatchersEndpoints:
      - "127.0.0.1:7053"
      - "127.0.0.1:7054"
  - ID: 2
    AssemblerEndpoint: "127.0.0.1:7055"
    ConsenterEndpoint: "127.0.0.1:7056"
    RouterEndpoint: "127.0.0.1:7057"
    BatchersEndpoints:
      - "127.0.0.1:7058"
      - "127.0.0.1:7059"
  - ID: 3
    AssemblerEndpoint: "127.0.0.1:7060"
    ConsenterEndpoint: "127.0.0.1:7061"
    RouterEndpoint: "127.0.0.1:7062"
    BatchersEndpoints:
      - "127.0.0.1:7063"
      - "127.0.0.1:7064"
  - ID: 4
    AssemblerEndpoint: "127.0.0.1:7065"
    ConsenterEndpoint: "127.0.0.1:7066"
    RouterEndpoint: "127.0.0.1:7067"
    BatchersEndpoints:
      - "127.0.0.1:7068"
      - "127.0.0.1:7069"
`

type Network struct {
	Parties []Party `yaml:"Parties"`
}

type Party struct {
	ID                uint16   `yaml:"ID"`
	AssemblerEndpoint string   `yaml:"AssemblerEndpoint"`
	ConsenterEndpoint string   `yaml:"ConsenterEndpoint"`
	RouterEndpoint    string   `yaml:"RouterEndpoint"`
	BatchersEndpoints []string `yaml:"BatchersEndpoints"`
}

type NetworkCryptoConfig struct {
	// map from party to its crypto config
	PartyToCryptoConfig map[uint16]CryptoConfigPerParty
}

type CryptoConfigPerParty struct {
	CAs                   []tlsgen.CA
	AssemblerCertKeyPair  *tlsgen.CertKeyPair
	ConsenterCertsAndKeys CertsAndKeys
	RouterCertKeyPair     *tlsgen.CertKeyPair
	// map from batcher's endpoint to its (cert,key) Pair
	BatchersCertsAndKeys map[string]CertsAndKeys
}

type CertsAndKeys struct {
	TLSCertKeyPair *tlsgen.CertKeyPair
	PrivateKey     []byte
	PublicKey      []byte
}

type NetworkConfig struct {
	PartiesConfig []PartyConfig
}

type PartyConfig struct {
	RouterConfig    node.RouterNodeConfig
	BatchersConfig  []node.BatcherNodeConfig
	ConsenterConfig node.ConsenterNodeConfig
	AssemblerConfig node.AssemblerNodeConfig
}

type SharedConfig struct {
	Shards     []node.ShardInfo
	Consenters []node.ConsenterInfo
}

// command line flags
var (
	app = kingpin.New("armageddon", "Utility for generating Arma config material")

	gen           = app.Command("generate", "Generate config material")
	outputDir     = gen.Flag("output", "The output directory in which to place config files").Default("arma-config").String()
	genConfigFile = gen.Flag("config", "The configuration template to use").File()

	showtemplate = app.Command("showtemplate", "Show the default configuration template needed to build Arma config material")

	version = app.Command("version", "Show version information")
)

func main() {
	kingpin.Version("0.0.1")
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	// "generate" command
	case gen.FullCommand():
		generate()

	// "showtemplate" command
	case showtemplate.FullCommand():
		fmt.Print(defaultConfig)
		os.Exit(0)

	// "version" command
	case version.FullCommand():
		printVersion()
	}
}

func generate() {
	// get config file content given as argument
	networkConfigFileContent, err := getConfigFileContent()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// create crypto config material for each party
	networkCryptoConfig := createNetworkCryptoConfig(networkConfigFileContent)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating network crypto config: %s", err)
		os.Exit(-1)
	}

	// parse the file and use the crypto material to create the network config
	networkConfig := parseNetworkConfig(networkConfigFileContent, networkCryptoConfig)

	// create config material for each party in a folder structure
	createConfigMaterial(networkConfig)
}

func getConfigFileContent() (*Network, error) {
	var configFileContent string
	if *genConfigFile != nil {
		data, err := io.ReadAll(*genConfigFile)
		if err != nil {
			return nil, fmt.Errorf("error reading configuration template: %s", err)
		}
		configFileContent = string(data)
	} else {
		configFileContent = defaultConfig
	}

	network := Network{}
	err := yaml.Unmarshal([]byte(configFileContent), &network)
	if err != nil {
		return nil, fmt.Errorf("error Unmarshalling YAML: %s", err)
	}

	return &network, nil
}

func createNetworkCryptoConfig(network *Network) *NetworkCryptoConfig {
	var partyToCryptoConfig = make(map[uint16]CryptoConfigPerParty)

	for _, party := range network.Parties {
		// create CA for the party
		ca, err := tlsgen.NewCA()
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating CA for party %d", err, party.ID)
			os.Exit(2)
		}
		listOfCAs := []tlsgen.CA{ca}

		// create crypto material for each party's nodes
		// crypto for assembler
		assemblerCertKeyPair, err := ca.NewServerCertKeyPair(party.AssemblerEndpoint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for assembler node", err)
			os.Exit(2)
		}

		// crypto for consenter
		// (cert,key) pair for consenter
		consenterCertKeyPair, err := ca.NewServerCertKeyPair(party.ConsenterEndpoint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for consenter node", err)
			os.Exit(2)
		}

		// private and public key for consenter
		privateKeyConsenter, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating private key for consenter node %s", err, party.ConsenterEndpoint)
			os.Exit(2)
		}
		privateKeyBytesConsenter, err := x509.MarshalPKCS8PrivateKey(privateKeyConsenter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed marshaling private key for consenter node %s", err, party.ConsenterEndpoint)
			os.Exit(2)
		}
		privateKeyPEMConsenter := pem.EncodeToMemory(&pem.Block{
			Bytes: privateKeyBytesConsenter, Type: "PRIVATE KEY",
		})

		publicKeyConsenter := privateKeyConsenter.PublicKey
		publicKeyBytesConsenter, err := x509.MarshalPKIXPublicKey(&publicKeyConsenter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed marshaling public key for consenter node %s", err, party.ConsenterEndpoint)
			os.Exit(2)
		}
		publicKeyPEMConsenter := pem.EncodeToMemory(&pem.Block{
			Bytes: publicKeyBytesConsenter, Type: "PUBLIC KEY",
		})

		consenterCertsAndKeys := CertsAndKeys{
			TLSCertKeyPair: consenterCertKeyPair,
			PrivateKey:     privateKeyPEMConsenter,
			PublicKey:      publicKeyPEMConsenter,
		}

		// crypto for router
		routerCertKeyPair, err := ca.NewServerCertKeyPair(party.RouterEndpoint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for assembler node", err)
			os.Exit(2)
		}

		// crypto for batchers
		var batcherEndpointToCertsAndKeys = make(map[string]CertsAndKeys)
		for _, batcherEndpoint := range party.BatchersEndpoints {
			// (cert,key) pair for a batcher
			batcherCertKeyPair, err := ca.NewServerCertKeyPair(party.RouterEndpoint)
			if err != nil {
				fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for batcher node %s", err, batcherEndpoint)
				os.Exit(2)
			}

			// private and public key for a batcher
			privateKeyBatcher, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				fmt.Fprintf(os.Stderr, "err: %s, failed creating private key for batcher node %s", err, batcherEndpoint)
				os.Exit(2)
			}
			privateKeyBytesBatcher, err := x509.MarshalPKCS8PrivateKey(privateKeyBatcher)
			if err != nil {
				fmt.Fprintf(os.Stderr, "err: %s, failed marshaling private key for batcher node %s", err, batcherEndpoint)
				os.Exit(2)
			}
			privateKeyPEMBatcher := pem.EncodeToMemory(&pem.Block{
				Bytes: privateKeyBytesBatcher, Type: "PRIVATE KEY",
			})

			publicKeyBatcher := privateKeyBatcher.PublicKey
			publicKeyBytesBatcher, err := x509.MarshalPKIXPublicKey(&publicKeyBatcher)
			if err != nil {
				fmt.Fprintf(os.Stderr, "err: %s, failed marshaling public key for batcher node %s", err, batcherEndpoint)
				os.Exit(2)
			}
			publicKeyPEMBatcher := pem.EncodeToMemory(&pem.Block{
				Bytes: publicKeyBytesBatcher, Type: "PUBLIC KEY",
			})

			batcherEndpointToCertsAndKeys[batcherEndpoint] = CertsAndKeys{
				TLSCertKeyPair: batcherCertKeyPair,
				PrivateKey:     privateKeyPEMBatcher,
				PublicKey:      publicKeyPEMBatcher,
			}
		}

		partyCryptoConfig := CryptoConfigPerParty{
			CAs:                   listOfCAs,
			AssemblerCertKeyPair:  assemblerCertKeyPair,
			ConsenterCertsAndKeys: consenterCertsAndKeys,
			RouterCertKeyPair:     routerCertKeyPair,
			BatchersCertsAndKeys:  batcherEndpointToCertsAndKeys,
		}
		partyToCryptoConfig[party.ID] = partyCryptoConfig
	}

	networkCryptoConfig := &NetworkCryptoConfig{
		PartyToCryptoConfig: partyToCryptoConfig,
	}

	return networkCryptoConfig
}

func parseNetworkConfig(network *Network, networkCryptoConfig *NetworkCryptoConfig) *NetworkConfig {
	// construct shared config
	sharedConfig := constructSharedConfig(network, networkCryptoConfig)

	var partiesConfig []PartyConfig

	for _, party := range network.Parties {
		partyConfig := PartyConfig{
			RouterConfig:    constructRouterNodeConfigPerParty(party.ID, sharedConfig.Shards, networkCryptoConfig.PartyToCryptoConfig[party.ID].RouterCertKeyPair),
			BatchersConfig:  constructBatchersNodeConfigPerParty(party.ID, party.BatchersEndpoints, sharedConfig, networkCryptoConfig.PartyToCryptoConfig[party.ID].BatchersCertsAndKeys),
			ConsenterConfig: constructConsenterNodeConfigPerParty(party.ID, sharedConfig, networkCryptoConfig.PartyToCryptoConfig[party.ID].ConsenterCertsAndKeys),
			AssemblerConfig: constructAssemblerNodeConfigPerParty(party.ID, sharedConfig.Shards, party.ConsenterEndpoint, networkCryptoConfig),
		}
		partiesConfig = append(partiesConfig, partyConfig)
	}

	networkConfig := &NetworkConfig{
		PartiesConfig: partiesConfig,
	}

	return networkConfig
}

func createConfigMaterial(networkConfig *NetworkConfig) {
	for i, partyConfig := range networkConfig.PartiesConfig {
		rootDir := path.Join(*outputDir, fmt.Sprintf("Party%d", i+1))
		os.MkdirAll(rootDir, 0755)

		configPath := path.Join(rootDir, "router_node_config.yaml")
		err := node.NodeConfigToYAML(partyConfig.RouterConfig, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating router node config yaml file, err: %v", err)
			os.Exit(1)
		}

		for j, batcherConfig := range partyConfig.BatchersConfig {
			configPath = path.Join(rootDir, fmt.Sprintf("batcher_node_%d_config.yaml", j+1))
			err = node.NodeConfigToYAML(batcherConfig, configPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error creating batcher node %d config yaml file, err: %v", j, err)
				os.Exit(1)
			}
		}

		configPath = path.Join(rootDir, "consenter_node_config.yaml")
		err = node.NodeConfigToYAML(partyConfig.ConsenterConfig, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating consenter node config yaml file, err: %v", err)
			os.Exit(1)
		}

		configPath = path.Join(rootDir, "assembler_node_config.yaml")
		err = node.NodeConfigToYAML(partyConfig.AssemblerConfig, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating assembler node config yaml file, err: %v", err)
			os.Exit(1)
		}
	}
}

// constructRouterNodeConfig construct the Router node configuration which is the same for all parties.
func constructRouterNodeConfigPerParty(partyID uint16, shards []node.ShardInfo, certKeyPair *tlsgen.CertKeyPair) node.RouterNodeConfig {
	return node.RouterNodeConfig{
		ListenAddress:                 "0.0.0.0",
		PartyID:                       partyID,
		TLSCertificateFile:            certKeyPair.Cert,
		TLSPrivateKeyFile:             certKeyPair.Key,
		Shards:                        shards,
		NumOfConnectionsForBatcher:    10,
		NumOfgRPCStreamsPerConnection: 5,
	}
}

func constructBatchersNodeConfigPerParty(partyId uint16, batcherEndpoints []string, sharedConfig SharedConfig, batchersCertsAndKeys map[string]CertsAndKeys) []node.BatcherNodeConfig {
	var batchers []node.BatcherNodeConfig
	for i, batcherEndpoint := range batcherEndpoints {
		batcherCertsAndKeys := batchersCertsAndKeys[batcherEndpoint]
		batcher := node.BatcherNodeConfig{
			ListenAddress:      "0.0.0.0",
			Shards:             sharedConfig.Shards,
			Consenters:         sharedConfig.Consenters,
			Directory:          "",
			PartyId:            partyId,
			ShardId:            uint16(i + 1),
			TLSPrivateKeyFile:  batcherCertsAndKeys.TLSCertKeyPair.Key,
			TLSCertificateFile: batcherCertsAndKeys.TLSCertKeyPair.Cert,
			SigningPrivateKey:  batcherCertsAndKeys.PrivateKey,
		}

		batchers = append(batchers, batcher)
	}
	return batchers
}

func constructConsenterNodeConfigPerParty(partyId uint16, sharedConfig SharedConfig, consenterCertsAndKeys CertsAndKeys) node.ConsenterNodeConfig {
	return node.ConsenterNodeConfig{
		ListenAddress:      "0.0.0.0",
		Shards:             sharedConfig.Shards,
		Consenters:         sharedConfig.Consenters,
		Directory:          "",
		PartyId:            partyId,
		TLSPrivateKeyFile:  consenterCertsAndKeys.TLSCertKeyPair.Key,
		TLSCertificateFile: consenterCertsAndKeys.TLSCertKeyPair.Cert,
		SigningPrivateKey:  consenterCertsAndKeys.PrivateKey,
	}
}
func constructAssemblerNodeConfigPerParty(partyId uint16, shards []node.ShardInfo, consenterEndpoint string, networkCryptoConfig *NetworkCryptoConfig) node.AssemblerNodeConfig {
	partyCryptoConfig := networkCryptoConfig.PartyToCryptoConfig[partyId]
	var tlsCACertsCollection []node.RawBytes
	for _, ca := range partyCryptoConfig.CAs {
		tlsCACertsCollection = append(tlsCACertsCollection, ca.CertBytes())
	}

	return node.AssemblerNodeConfig{
		ListenAddress:      "0.0.0.0",
		TLSPrivateKeyFile:  partyCryptoConfig.AssemblerCertKeyPair.Key,
		TLSCertificateFile: partyCryptoConfig.AssemblerCertKeyPair.Cert,
		PartyId:            partyId,
		Directory:          "",
		Shards:             shards,
		Consenter: node.ConsenterInfo{
			PartyID:    partyId,
			Endpoint:   consenterEndpoint,
			PublicKey:  partyCryptoConfig.ConsenterCertsAndKeys.PublicKey,
			TLSCACerts: tlsCACertsCollection,
		},
	}
}

func constructShards(network *Network, networkCryptoConfig *NetworkCryptoConfig) []node.ShardInfo {
	// construct a map that maps between shardId and batchers
	var shardToBatchers = make(map[uint16][]node.BatcherInfo)
	for _, party := range network.Parties {
		for idx, batcherEndpoint := range party.BatchersEndpoints {
			shardId := uint16(idx + 1)

			partyCryptoConfig := networkCryptoConfig.PartyToCryptoConfig[party.ID]
			var tlsCACertsCollection []node.RawBytes
			for _, ca := range partyCryptoConfig.CAs {
				tlsCACertsCollection = append(tlsCACertsCollection, ca.CertBytes())
			}
			batcherCertsAndKeys := partyCryptoConfig.BatchersCertsAndKeys[batcherEndpoint]

			batcher := node.BatcherInfo{
				PartyID:    party.ID,
				Endpoint:   batcherEndpoint,
				TLSCACerts: tlsCACertsCollection,
				PublicKey:  batcherCertsAndKeys.PublicKey,
				TLSCert:    batcherCertsAndKeys.TLSCertKeyPair.Cert,
			}
			shardToBatchers[shardId] = append(shardToBatchers[shardId], batcher)
		}
	}

	// build Shards from the map
	var shards []node.ShardInfo
	for shardId, batchers := range shardToBatchers {
		shardInfo := node.ShardInfo{
			ShardId:  shardId,
			Batchers: batchers,
		}
		shards = append(shards, shardInfo)
	}

	return shards
}

func constructConsenters(network *Network, networkCryptoConfig *NetworkCryptoConfig) []node.ConsenterInfo {
	var consenters []node.ConsenterInfo
	for _, party := range network.Parties {
		partyCryptoConfig := networkCryptoConfig.PartyToCryptoConfig[party.ID]
		var tlsCACertsCollection []node.RawBytes
		for _, ca := range partyCryptoConfig.CAs {
			tlsCACertsCollection = append(tlsCACertsCollection, ca.CertBytes())
		}
		consenterCertsAndKeys := partyCryptoConfig.ConsenterCertsAndKeys

		consenterInfo := node.ConsenterInfo{
			PartyID:    party.ID,
			Endpoint:   party.ConsenterEndpoint,
			PublicKey:  consenterCertsAndKeys.PublicKey,
			TLSCACerts: tlsCACertsCollection,
		}
		consenters = append(consenters, consenterInfo)
	}
	return consenters
}

func constructSharedConfig(network *Network, networkCryptoConfig *NetworkCryptoConfig) SharedConfig {
	shards := constructShards(network, networkCryptoConfig)
	consenters := constructConsenters(network, networkCryptoConfig)

	return SharedConfig{
		Shards:     shards,
		Consenters: consenters,
	}
}

func printVersion() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Errorf("failed to read build info")
	}

	fmt.Printf("Armageddon version is: %+v\n", bi.Main.Version)
}
