package armageddon

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/csv"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"arma/common/types"
	"arma/node/comm"
	"arma/node/comm/tlsgen"
	"arma/node/config"

	"github.com/alecthomas/kingpin"
	"github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"gopkg.in/yaml.v3"
)

var defaultConfig = `Parties:
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

func init() {
	// set the gRPC logger to a logger that discards the log output.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
}

type Network struct {
	Parties []Party `yaml:"Parties"`
}

type Party struct {
	ID                types.PartyID `yaml:"ID"`
	AssemblerEndpoint string        `yaml:"AssemblerEndpoint"`
	ConsenterEndpoint string        `yaml:"ConsenterEndpoint"`
	RouterEndpoint    string        `yaml:"RouterEndpoint"`
	BatchersEndpoints []string      `yaml:"BatchersEndpoints"`
}

type NetworkCryptoConfig struct {
	// map from party to its crypto config
	PartyToCryptoConfig map[types.PartyID]CryptoConfigPerParty
}

type CryptoConfigPerParty struct {
	CAs                   []tlsgen.CA
	AssemblerCertKeyPair  *tlsgen.CertKeyPair
	ConsenterCertsAndKeys CertsAndKeys
	RouterCertKeyPair     *tlsgen.CertKeyPair
	// map from batcher's endpoint to its (cert,key) Pair
	BatchersCertsAndKeys map[string]CertsAndKeys
	UserInfo             UserInfo
}

// UserInfo holds the user information needed for connection to routers and assemblers
// Note: a user will be created for each party. One of the users will be chosen as a grpc client that sends tx to all router and receives blocks from the assemblers.
type UserInfo struct {
	TLSPrivateKeyFile  config.RawBytes
	TLSCertificateFile config.RawBytes
	RouterEndpoints    []string
	AssemblerEndpoints []string
	TLSCACerts         []config.RawBytes
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
	RouterConfig    config.RouterNodeConfig
	BatchersConfig  []config.BatcherNodeConfig
	ConsenterConfig config.ConsenterNodeConfig
	AssemblerConfig config.AssemblerNodeConfig
}

type SharedConfig struct {
	Shards     []config.ShardInfo
	Consenters []config.ConsenterInfo
}

type protectedMap struct {
	keyValMap map[string]bool
	mutex     sync.Mutex
}

func (pm *protectedMap) Add(key string) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.keyValMap[key] = true
}

func (pm *protectedMap) Remove(key string) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	delete(pm.keyValMap, key)
}

func (pm *protectedMap) IsEmpty() bool {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	return len(pm.keyValMap) == 0
}

type CLI struct {
	app      *kingpin.Application
	commands map[string]*kingpin.CmdClause
	// generate command flags
	outputDir     *string
	genConfigFile **os.File
	// submit command flags
	userConfigFile **os.File
	transactions   *int // transactions is the number of txs to be sent
	rate           *int // rate is the number of transaction per second to be sent
	txSize         *int // txSize is the required transaction size
	// load command flags
	loadUserConfigFile **os.File
	loadTransactions   *int
	loadRate           *int
	loadTxSize         *int
	// receive command flags
	receiveUserConfigFile   **os.File
	receiveExpectedNumOfTxs *int
	receiveOutputDir        *string
	pullFromPartyId         *int
}

func NewCLI() *CLI {
	app := kingpin.New("armageddon", "Utility for generating Arma config material")
	cli := &CLI{app: app}
	cli.configureCommands()
	return cli
}

func (cli *CLI) configureCommands() {
	commands := make(map[string]*kingpin.CmdClause)
	gen := cli.app.Command("generate", "Generate config material")
	commands["generate"] = gen

	cli.outputDir = gen.Flag("output", "The output directory in which to place config files").Default("arma-config").String()
	cli.genConfigFile = gen.Flag("config", "The configuration template to use").File()

	showtemplate := cli.app.Command("showtemplate", "Show the default configuration template needed to build Arma config material")
	commands["showtemplate"] = showtemplate

	version := cli.app.Command("version", "Show version information")
	commands["version"] = version

	submit := cli.app.Command("submit", "Submit txs to routers and verify the submission")
	cli.userConfigFile = submit.Flag("config", "The user configuration needed to connection with routers and assemblers").File()
	cli.transactions = submit.Flag("transactions", "The number of transactions to be sent").Int()
	cli.rate = submit.Flag("rate", "The rate specify the number of transactions per second to be sent").Int()
	cli.txSize = submit.Flag("txSize", "The required transaction size in bytes").Default("512").Int()
	commands["submit"] = submit

	load := cli.app.Command("load", "Submit txs to routers and verify the routers have received the txs")
	cli.loadUserConfigFile = load.Flag("config", "The user configuration needed to connection with routers").File()
	cli.loadTransactions = load.Flag("transactions", "The number of transactions to be sent").Int()
	cli.loadRate = load.Flag("rate", "The rate specify the number of transactions per second to be sent").Int()
	cli.loadTxSize = load.Flag("txSize", "The required transaction size in bytes").Int()
	commands["load"] = load

	receive := cli.app.Command("receive", "Pull txs from some assembler and report statistics")
	cli.receiveUserConfigFile = receive.Flag("config", "The user configuration needed to connection with assemblers").File()
	cli.receiveExpectedNumOfTxs = receive.Flag("expectedTxs", "The expected number of transactions the assembler should received").Default("-1").Int()
	cli.receiveOutputDir = receive.Flag("output", "The output directory in which to place statistics file").Default(".").String()
	cli.pullFromPartyId = receive.Flag("pullFromPartyId", "The party id of the assembler to pull blocks from").Int()
	commands["receive"] = receive

	cli.commands = commands
}

func (cli *CLI) Run(args []string) {
	switch kingpin.MustParse(cli.app.Parse(args)) {

	// "generate" command
	case cli.commands["generate"].FullCommand():
		generate(cli.genConfigFile, cli.outputDir)

	// "showtemplate" command
	case cli.commands["showtemplate"].FullCommand():
		showtemplate()

	// "version" command
	case cli.commands["version"].FullCommand():
		printVersion()

	// "submit" command
	case cli.commands["submit"].FullCommand():
		submit(cli.userConfigFile, cli.transactions, cli.rate, cli.txSize)

	// "load" command
	case cli.commands["load"].FullCommand():
		load(cli.loadUserConfigFile, cli.loadTransactions, cli.loadRate, cli.loadTxSize)

	// "receive" command
	case cli.commands["receive"].FullCommand():
		receive(cli.receiveUserConfigFile, cli.pullFromPartyId, cli.receiveOutputDir, cli.receiveExpectedNumOfTxs)
	}
}

func generate(genConfigFile **os.File, outputDir *string) {
	// get config file content given as argument
	networkConfigFileContent, err := getConfigFileContent(genConfigFile)
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
	createConfigMaterial(networkConfig, networkCryptoConfig, outputDir)
}

func getConfigFileContent(genConfigFile **os.File) (*Network, error) {
	var configFileContent string
	if *genConfigFile != nil {
		data, err := io.ReadAll(*genConfigFile)
		if err != nil {
			return nil, fmt.Errorf("error reading configuration template: %s", err)
		}
		configFileContent = string(data)
	} else {
		// no configuration template has been provided, hence the default one is chosen
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
	// collect router and assembler endpoints, required for defining a user for each party
	var routerEndpoints []string
	var assemblerEndpoints []string
	for _, party := range network.Parties {
		routerEndpoints = append(routerEndpoints, party.RouterEndpoint)
		assemblerEndpoints = append(assemblerEndpoints, party.AssemblerEndpoint)
	}

	// create CA for each party
	partiesCAs := make(map[types.PartyID][]tlsgen.CA)
	var tlsCACertsBytesPartiesCollection []config.RawBytes
	for _, party := range network.Parties {
		// create CA for the party
		// NOTE: a party can have several CA's, meanwhile armageddon creates only one CA for each party.
		ca, err := tlsgen.NewCA()
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating CA for party %d", err, party.ID)
			os.Exit(2)
		}
		partiesCAs[party.ID] = []tlsgen.CA{ca}
		// user will be able to connect to each of the routers only if it receives for each router the CA that signed the certificate of that router.
		// therefore, the CA created per party must be collected for each party, to which the router is associated.
		tlsCACertsBytesPartiesCollection = append(tlsCACertsBytesPartiesCollection, ca.CertBytes())
	}

	partyToCryptoConfig := make(map[types.PartyID]CryptoConfigPerParty)

	for _, party := range network.Parties {
		// ca's of the party
		listOfCAs := partiesCAs[party.ID]
		ca := listOfCAs[0]

		// create crypto material for each party's nodes
		// crypto for assembler
		assemblerCertKeyPair, err := ca.NewServerCertKeyPair(trimPortFromEndpoint(party.AssemblerEndpoint))
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for assembler node", err)
			os.Exit(2)
		}

		// crypto for consenter
		// (cert,key) pair for consenter
		consenterCertKeyPair, err := ca.NewServerCertKeyPair(trimPortFromEndpoint(party.ConsenterEndpoint))
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
		routerCertKeyPair, err := ca.NewServerCertKeyPair(trimPortFromEndpoint(party.RouterEndpoint))
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for assembler node", err)
			os.Exit(2)
		}

		// crypto for batchers
		batcherEndpointToCertsAndKeys := make(map[string]CertsAndKeys)
		for _, batcherEndpoint := range party.BatchersEndpoints {
			// (cert,key) pair for a batcher
			batcherCertKeyPair, err := ca.NewServerCertKeyPair(trimPortFromEndpoint(batcherEndpoint))
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

		// crypto for user
		// (cert, key) pair for user
		userTlsCertKayPair, err := ca.NewClientCertKeyPair()
		if err != nil {
			fmt.Fprintf(os.Stderr, "err: %s, failed creating (cert,key) pair for the user of party %d", err, party.ID)
			os.Exit(2)
		}

		userInfo := UserInfo{
			TLSPrivateKeyFile:  userTlsCertKayPair.Key,
			TLSCertificateFile: userTlsCertKayPair.Cert,
			RouterEndpoints:    routerEndpoints,
			AssemblerEndpoints: assemblerEndpoints,
			TLSCACerts:         tlsCACertsBytesPartiesCollection,
		}

		partyCryptoConfig := CryptoConfigPerParty{
			CAs:                   listOfCAs,
			AssemblerCertKeyPair:  assemblerCertKeyPair,
			ConsenterCertsAndKeys: consenterCertsAndKeys,
			RouterCertKeyPair:     routerCertKeyPair,
			BatchersCertsAndKeys:  batcherEndpointToCertsAndKeys,
			UserInfo:              userInfo,
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
			RouterConfig:    constructRouterNodeConfigPerParty(party.ID, sharedConfig.Shards, networkCryptoConfig.PartyToCryptoConfig[party.ID].RouterCertKeyPair, trimHostFromEndpoint(party.RouterEndpoint)),
			BatchersConfig:  constructBatchersNodeConfigPerParty(party.ID, party.BatchersEndpoints, sharedConfig, networkCryptoConfig.PartyToCryptoConfig[party.ID].BatchersCertsAndKeys),
			ConsenterConfig: constructConsenterNodeConfigPerParty(party.ID, sharedConfig, networkCryptoConfig.PartyToCryptoConfig[party.ID].ConsenterCertsAndKeys, trimHostFromEndpoint(party.ConsenterEndpoint)),
			AssemblerConfig: constructAssemblerNodeConfigPerParty(party.ID, sharedConfig.Shards, party.ConsenterEndpoint, networkCryptoConfig, trimHostFromEndpoint(party.AssemblerEndpoint)),
		}
		partiesConfig = append(partiesConfig, partyConfig)
	}

	networkConfig := &NetworkConfig{
		PartiesConfig: partiesConfig,
	}

	return networkConfig
}

func constructRouterNodeConfigPerParty(partyID types.PartyID, shards []config.ShardInfo, certKeyPair *tlsgen.CertKeyPair, port string) config.RouterNodeConfig {
	return config.RouterNodeConfig{
		ListenAddress:                 "0.0.0.0:" + port,
		PartyID:                       partyID,
		TLSCertificateFile:            certKeyPair.Cert,
		TLSPrivateKeyFile:             certKeyPair.Key,
		Shards:                        shards,
		NumOfConnectionsForBatcher:    10,
		NumOfgRPCStreamsPerConnection: 5,
	}
}

func constructBatchersNodeConfigPerParty(partyId types.PartyID, batcherEndpoints []string, sharedConfig SharedConfig, batchersCertsAndKeys map[string]CertsAndKeys) []config.BatcherNodeConfig {
	var batchers []config.BatcherNodeConfig
	for i, batcherEndpoint := range batcherEndpoints {
		batcherCertsAndKeys := batchersCertsAndKeys[batcherEndpoint]
		batcher := config.BatcherNodeConfig{
			ListenAddress:      "0.0.0.0:" + trimHostFromEndpoint(batcherEndpoint),
			Shards:             sharedConfig.Shards,
			Consenters:         sharedConfig.Consenters,
			Directory:          "",
			PartyId:            partyId,
			ShardId:            types.ShardID(i + 1),
			TLSPrivateKeyFile:  batcherCertsAndKeys.TLSCertKeyPair.Key,
			TLSCertificateFile: batcherCertsAndKeys.TLSCertKeyPair.Cert,
			SigningPrivateKey:  batcherCertsAndKeys.PrivateKey,
		}

		batchers = append(batchers, batcher)
	}
	return batchers
}

func constructConsenterNodeConfigPerParty(partyId types.PartyID, sharedConfig SharedConfig, consenterCertsAndKeys CertsAndKeys, port string) config.ConsenterNodeConfig {
	return config.ConsenterNodeConfig{
		ListenAddress:      "0.0.0.0:" + port,
		Shards:             sharedConfig.Shards,
		Consenters:         sharedConfig.Consenters,
		Directory:          "",
		PartyId:            partyId,
		TLSPrivateKeyFile:  consenterCertsAndKeys.TLSCertKeyPair.Key,
		TLSCertificateFile: consenterCertsAndKeys.TLSCertKeyPair.Cert,
		SigningPrivateKey:  consenterCertsAndKeys.PrivateKey,
	}
}

func constructAssemblerNodeConfigPerParty(partyId types.PartyID, shards []config.ShardInfo, consenterEndpoint string, networkCryptoConfig *NetworkCryptoConfig, port string) config.AssemblerNodeConfig {
	partyCryptoConfig := networkCryptoConfig.PartyToCryptoConfig[partyId]
	var tlsCACertsCollection []config.RawBytes
	for _, ca := range partyCryptoConfig.CAs {
		tlsCACertsCollection = append(tlsCACertsCollection, ca.CertBytes())
	}

	return config.AssemblerNodeConfig{
		ListenAddress:      "0.0.0.0:" + port,
		TLSPrivateKeyFile:  partyCryptoConfig.AssemblerCertKeyPair.Key,
		TLSCertificateFile: partyCryptoConfig.AssemblerCertKeyPair.Cert,
		PartyId:            partyId,
		Directory:          "",
		Shards:             shards,
		Consenter: config.ConsenterInfo{
			PartyID:    partyId,
			Endpoint:   consenterEndpoint,
			PublicKey:  partyCryptoConfig.ConsenterCertsAndKeys.PublicKey,
			TLSCACerts: tlsCACertsCollection,
		},
	}
}

func constructShards(network *Network, networkCryptoConfig *NetworkCryptoConfig) []config.ShardInfo {
	// construct a map that maps between shardId and batchers
	shardToBatchers := make(map[types.ShardID][]config.BatcherInfo)
	for _, party := range network.Parties {
		for idx, batcherEndpoint := range party.BatchersEndpoints {
			shardId := types.ShardID(idx + 1)

			partyCryptoConfig := networkCryptoConfig.PartyToCryptoConfig[party.ID]
			var tlsCACertsCollection []config.RawBytes
			for _, ca := range partyCryptoConfig.CAs {
				tlsCACertsCollection = append(tlsCACertsCollection, ca.CertBytes())
			}
			batcherCertsAndKeys := partyCryptoConfig.BatchersCertsAndKeys[batcherEndpoint]

			batcher := config.BatcherInfo{
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
	var shards []config.ShardInfo
	for shardId, batchers := range shardToBatchers {
		shardInfo := config.ShardInfo{
			ShardId:  shardId,
			Batchers: batchers,
		}
		shards = append(shards, shardInfo)
	}

	return shards
}

func constructConsenters(network *Network, networkCryptoConfig *NetworkCryptoConfig) []config.ConsenterInfo {
	var consenters []config.ConsenterInfo
	for _, party := range network.Parties {
		partyCryptoConfig := networkCryptoConfig.PartyToCryptoConfig[party.ID]
		var tlsCACertsCollection []config.RawBytes
		for _, ca := range partyCryptoConfig.CAs {
			tlsCACertsCollection = append(tlsCACertsCollection, ca.CertBytes())
		}
		consenterCertsAndKeys := partyCryptoConfig.ConsenterCertsAndKeys

		consenterInfo := config.ConsenterInfo{
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

func createConfigMaterial(networkConfig *NetworkConfig, networkCryptoConfig *NetworkCryptoConfig, outputDir *string) {
	for i, partyConfig := range networkConfig.PartiesConfig {
		rootDir := path.Join(*outputDir, fmt.Sprintf("Party%d", i+1))
		os.MkdirAll(rootDir, 0o755)

		configPath := path.Join(rootDir, "router_node_config.yaml")
		err := config.NodeConfigToYAML(partyConfig.RouterConfig, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating router node config yaml file, err: %v", err)
			os.Exit(1)
		}

		for j, batcherConfig := range partyConfig.BatchersConfig {
			configPath = path.Join(rootDir, fmt.Sprintf("batcher_node_%d_config.yaml", j+1))
			err = config.NodeConfigToYAML(batcherConfig, configPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error creating batcher node %d config yaml file, err: %v", j, err)
				os.Exit(1)
			}
		}

		configPath = path.Join(rootDir, "consenter_node_config.yaml")
		err = config.NodeConfigToYAML(partyConfig.ConsenterConfig, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating consenter node config yaml file, err: %v", err)
			os.Exit(1)
		}

		configPath = path.Join(rootDir, "assembler_node_config.yaml")
		err = config.NodeConfigToYAML(partyConfig.AssemblerConfig, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating assembler node config yaml file, err: %v", err)
			os.Exit(1)
		}

		userInfo := networkCryptoConfig.PartyToCryptoConfig[types.PartyID(i+1)].UserInfo
		configPath = path.Join(rootDir, "user_config.yaml")
		uca, err := yaml.Marshal(&userInfo)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error marshaling user config yaml file, err: %v", err)
			os.Exit(1)
		}

		err = os.WriteFile(configPath, uca, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing user config yaml file, err: %v", err)
			os.Exit(1)
		}
	}
}

func showtemplate() {
	fmt.Print(defaultConfig)
	os.Exit(0)
}

func printVersion() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Println(fmt.Errorf("failed to read build info"))
	}

	fmt.Printf("Armageddon version is: %+v\n", bi.Main.Version)
}

func getUserConfigFileContent(userConfigFile **os.File) (*UserInfo, error) {
	var configFileContent string
	if *userConfigFile != nil {
		data, err := io.ReadAll(*userConfigFile)
		if err != nil {
			return nil, fmt.Errorf("error reading configuration template: %s", err)
		}
		configFileContent = string(data)
	} else {
		// no configuration template has been provided
		fmt.Fprintf(os.Stderr, "user config yaml file is missing")
		os.Exit(1)
	}

	userConfig := UserInfo{}
	err := yaml.Unmarshal([]byte(configFileContent), &userConfig)
	if err != nil {
		return nil, fmt.Errorf("error Unmarshalling YAML: %s", err)
	}

	return &userConfig, nil
}

// submit command makes txs and sends them to all routers (assuming there are 4 routers)
// it also asks for blocks from some assembler (no matter who it is) to validate the txs appear in some block
func submit(userConfigFile **os.File, transactions *int, rate *int, txSize *int) {
	// check transaction size
	txMinimumSize := 16 + 8 + 8
	if *txSize < txMinimumSize {
		fmt.Fprintf(os.Stderr, "the required tx size: %d is less than the minimum size: %d", *txSize, txMinimumSize)
		os.Exit(3)
	}

	// get user config file content given as argument
	userConfigFileContent, err := getUserConfigFileContent(userConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// send txs to the routers
	start := time.Now()
	txsMap := &protectedMap{
		keyValMap: make(map[string]bool),
		mutex:     sync.Mutex{},
	}
	var waitForTxToBeSentAndReceived sync.WaitGroup
	waitForTxToBeSentAndReceived.Add(2)
	go func() {
		sendTxToRouters(userConfigFileContent, *transactions, *rate, *txSize, txsMap)
		waitForTxToBeSentAndReceived.Done()
	}()

	// receive blocks from some assembler
	var numOfBlocks int
	var txDelayTimes float64
	go func() {
		numOfBlocks, txDelayTimes = receiveResponseFromAssembler(userConfigFileContent, txsMap, *transactions)
		waitForTxToBeSentAndReceived.Done()
	}()

	waitForTxToBeSentAndReceived.Wait()
	elapsed := time.Since(start)

	// report results
	reportResults(*transactions, elapsed, txDelayTimes, numOfBlocks, *txSize)
}

// load command makes txs and sends them to all routers (assuming there are 4 routers)
func load(userConfigFile **os.File, transactions *int, rate *int, txSize *int) {
	// check transaction size
	txMinimumSize := 16 + 8 + 8
	if *txSize < txMinimumSize {
		fmt.Fprintf(os.Stderr, "the required tx size: %d is less than the minimum size: %d", *txSize, txMinimumSize)
		os.Exit(3)
	}

	// get user config file content given as argument
	userConfigFileContent, err := getUserConfigFileContent(userConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// send txs to the routers
	start := time.Now()
	sendTxToRouters(userConfigFileContent, *transactions, *rate, *txSize, nil)
	elapsed := time.Since(start)

	reportLoadResults(*transactions, elapsed, *txSize)
}

// receive command pull blocks from the assembler and report statistics
func receive(userConfigFile **os.File, pullFromPartyId *int, receiveOutputDir *string, expectedNumOfTxs *int) {
	// get user config file content given as argument
	userConfigFileContent, err := getUserConfigFileContent(userConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// pull blocks from the assembler and report statistics to statistics.csv file
	pullBlocksFromAssemblerAndCollectStatistics(userConfigFileContent, *pullFromPartyId, *receiveOutputDir, *expectedNumOfTxs)
	fmt.Printf("Receive command finished, statistics can be found in: %v\n", path.Join(*receiveOutputDir, "statistics.csv"))
}

func trimPortFromEndpoint(endpoint string) string {
	if strings.Contains(endpoint, ":") {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return host
	}

	return endpoint
}

func trimHostFromEndpoint(endpoint string) string {
	if strings.Contains(endpoint, ":") {
		_, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return port
	}

	return endpoint
}

func nextSeekInfo(startSeq uint64) *ab.SeekInfo {
	return &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: startSeq}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT,
	}
}

// sendTxToRouters assumes there are 4 routers
func sendTxToRouters(userConfigFileContent *UserInfo, numOfTxs int, rate int, txSize int, txsMap *protectedMap) {
	var serverRootCAs [][]byte
	for _, rawBytes := range userConfigFileContent.TLSCACerts {
		byteSlice := []byte(rawBytes)
		serverRootCAs = append(serverRootCAs, byteSlice)
	}

	var gRPCRouterClientsConn []*grpc.ClientConn
	var streams []ab.AtomicBroadcast_BroadcastClient

	// create gRPC clients and streams to the routers
	for i := 0; i < 4; i++ {
		// create a gRPC connection to the router
		gRPCRouterClient := comm.ClientConfig{
			KaOpts: comm.KeepaliveOptions{
				ClientInterval: time.Hour,
				ClientTimeout:  time.Hour,
			},
			SecOpts: comm.SecureOptions{
				Key:               userConfigFileContent.TLSPrivateKeyFile,
				Certificate:       userConfigFileContent.TLSCertificateFile,
				RequireClientCert: true,
				UseTLS:            true,
				ServerRootCAs:     serverRootCAs,
			},
			DialTimeout: time.Second * 5,
		}

		gRPCRouterClientConn, err := gRPCRouterClient.Dial(userConfigFileContent.RouterEndpoints[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create a gRPC client connection to router %d: %v", i+1, err)
			os.Exit(3)
		}

		gRPCRouterClientsConn = append(gRPCRouterClientsConn, gRPCRouterClientConn)

		// open a broadcast stream
		stream, err := ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open a broadcast stream to router %d: %v", i+1, err)
			os.Exit(3)
		}

		streams = append(streams, stream)
	}

	// open a go routine to check for acknowledgment
	var wgRecv sync.WaitGroup
	for n, s := range streams {
		wgRecv.Add(1)
		go func(n int, stream ab.AtomicBroadcast_BroadcastClient) {
			defer wgRecv.Done()
			numOfAcks := 0
			for {
				ack, err := stream.Recv()
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to receive acknowledgment from router %d: %v", n+1, err)
					os.Exit(3)
				}
				if ack.Status.String() != "SUCCESS" {
					fmt.Fprintf(os.Stderr, "failed to receive ack with success status from router %d: %v", n+1, err)
					os.Exit(3)
				}
				numOfAcks = numOfAcks + 1
				if numOfAcks == numOfTxs {
					break
				}
			}
		}(n, s)
	}

	// create a session number (16 bytes)
	sessionNumber := make([]byte, 16)
	_, err := rand.Read(sessionNumber)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a session number, %v", err)
		os.Exit(3)
	}

	// send txs to all routers, using the rate limiter bucket
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	capacity := rate / fillFrequency
	rl, err := NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}
	for i := 0; i < numOfTxs; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		sendTx(txsMap, streams, i, txSize, sessionNumber)
	}
	rl.Stop()

	wgRecv.Wait()

	// close gRPC connections
	for i, conn := range gRPCRouterClientsConn {
		if err := conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close gRPC connection to router %d: %v", i+1, err)
			os.Exit(3)
		}
	}
}

func pullBlocksFromAssemblerAndCollectStatistics(userConfigFileContent *UserInfo, pullFromPartyId int, receiveOutputDir string, expectedNumOfTxs int) {
	// choose randomly the first assembler
	i := pullFromPartyId - 1
	var serverRootCAs [][]byte
	for _, rawBytes := range userConfigFileContent.TLSCACerts {
		byteSlice := []byte(rawBytes)
		serverRootCAs = append(serverRootCAs, byteSlice)
	}

	// create a gRPC connection to the assembler
	gRPCAssemblerClient := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               userConfigFileContent.TLSPrivateKeyFile,
			Certificate:       userConfigFileContent.TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		nil,
		nextSeekInfo(0),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed create a request envelope")
		os.Exit(3)
	}

	var stream ab.AtomicBroadcast_DeliverClient
	var gRPCAssemblerClientConn *grpc.ClientConn
	endpointToPullFrom := userConfigFileContent.AssemblerEndpoints[i]

	gRPCAssemblerClientConn, err = gRPCAssemblerClient.Dial(endpointToPullFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a gRPC client connection to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver stream
	stream, err = abc.Deliver(context.TODO())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a deliver stream to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// send request envelope
	err = stream.Send(requestEnvelope)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to send a request envelope to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// pull blocks from assembler, every second pack statistics and send it to the manageStatistics
	statisticsAggregator := &StatisticsAggregator{}

	statisticChan := make(chan Statistics, 60)
	blockChan := make(chan BlockWithTime)
	stopChan := make(chan bool)

	var waitToFinish sync.WaitGroup
	waitToFinish.Add(4)

	statisticsAggregator.startTime = time.Now().UnixMilli()
	startTimeS := float64(statisticsAggregator.startTime) / 1000
	timeIntervalToSampleStat := 1 * time.Second

	// handle statistics channel messages
	go func() {
		manageStatistics(receiveOutputDir, statisticChan, stopChan, startTimeS, expectedNumOfTxs, pullFromPartyId, timeIntervalToSampleStat)
		waitToFinish.Done()
	}()

	// every second read the statistics and send it to the manageStatistics
	go func() {
		ticker := time.NewTicker(timeIntervalToSampleStat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// send the accumulated statistics to the channel
				lastStat := statisticsAggregator.ReadAndReset()
				statisticChan <- lastStat
			case <-stopChan:
				waitToFinish.Done()
				return
			}
		}
	}()

	// pull blocks from the assembler
	// if a flag is given, stop when finish receiving all txs
	go func() {
		var txsTotal int
		for {
			block, err := pullBlock(stream, endpointToPullFrom, gRPCAssemblerClientConn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to pull block from assembler %d: %v", i+1, err)
				os.Exit(3)
			}
			blockWithTime := BlockWithTime{
				block:        block,
				acceptedTime: time.Now(),
			}
			blockChan <- blockWithTime
			txsTotal += len(blockWithTime.block.Data.Data)

			if expectedNumOfTxs > 0 && expectedNumOfTxs == txsTotal {
				waitToFinish.Done()
				return
			}
		}
	}()

	// parse blocks and make statistics on each block
	go func() {
		var sumOfDelayTimes float64
		var txs int
		var txsTotal int
		var sumOfTxsSize int
		for {
			blockWithTime := <-blockChan
			sumOfDelayTimes = 0.0
			sumOfTxsSize = 0
			txs = len(blockWithTime.block.Data.Data)
			txsTotal += len(blockWithTime.block.Data.Data)
			// iterate over txs in block
			for j := 0; j < txs; j++ {
				env, err := protoutil.GetEnvelopeFromBlock(blockWithTime.block.Data.Data[j])
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to get envelope from block: %v", err)
					os.Exit(3)
				}

				// extract the tx size, sending time and calculate the delay, add the delay to sumOfDelayTimes
				sumOfTxsSize += len(env.Payload)
				delay := calculateDelayOfTx(env, blockWithTime.acceptedTime)
				sumOfDelayTimes = sumOfDelayTimes + delay.Seconds()
			}
			statisticsAggregator.Add(txs, 1, sumOfDelayTimes, sumOfTxsSize)

			if expectedNumOfTxs > 0 && expectedNumOfTxs == txsTotal {
				close(stopChan)
				waitToFinish.Done()
				return
			}
		}
	}()

	waitToFinish.Wait()
}

func calculateDelayOfTx(env *common.Envelope, acceptedTime time.Time) time.Duration {
	readPayload := bytes.NewBuffer(env.Payload)
	startPosition := 16 + 8
	readPayload.Next(startPosition)
	var extractedSendTime uint64
	binary.Read(readPayload, binary.BigEndian, &extractedSendTime)
	sendTime := time.Unix(0, int64(extractedSendTime))
	delayTime := acceptedTime.Sub(sendTime)
	return delayTime
}

func pullBlock(stream ab.AtomicBroadcast_DeliverClient, endpointToPullFrom string, gRPCAssemblerClientConn *grpc.ClientConn) (*common.Block, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive a deliver response from %s", endpointToPullFrom)
	}

	block := resp.GetBlock()

	if block == nil {
		stream.CloseSend()
		gRPCAssemblerClientConn.Close()
		return nil, fmt.Errorf("received a non block message from %s: %v", endpointToPullFrom, resp)
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		stream.CloseSend()
		gRPCAssemblerClientConn.Close()
		return nil, fmt.Errorf("received empty block from %s", endpointToPullFrom)
	}

	return block, nil
}

func sendTx(txsMap *protectedMap, streams []ab.AtomicBroadcast_BroadcastClient, i int, txSize int, sessionNumber []byte) {
	payload := prepareTx(i, txSize, sessionNumber)
	if txsMap != nil {
		txsMap.Add(string(payload[:32]))
	}
	for j := 0; j < 4; j++ {
		err := streams[j].Send(&common.Envelope{Payload: payload})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to send tx to router %d: %v", j+1, err)
			os.Exit(3)
		}
	}
}

func prepareTx(txNumber int, txSize int, sessionNumber []byte) []byte {
	// create timestamp (8 bytes)
	timeStamp := uint64(time.Now().UnixNano())

	// prepare the payload
	buffer := make([]byte, txSize)
	buff := bytes.NewBuffer(buffer[:0])
	buff.Write(sessionNumber)
	binary.Write(buff, binary.BigEndian, uint64(txNumber))
	binary.Write(buff, binary.BigEndian, timeStamp)
	result := buff.Bytes()
	if len(buff.Bytes()) < txSize {
		padding := make([]byte, txSize-len(result))
		result = append(result, padding...)
	}
	return result
}

func reportResults(transactions int, elapsed time.Duration, txDelayTimesResult float64, numOfBlocksResult int, txSize int) {
	avgTxRate := float64(transactions) / elapsed.Seconds()
	avgTxDelay := txDelayTimesResult / float64(transactions)
	avgBlockRate := float64(numOfBlocksResult) / elapsed.Seconds()
	avgBlockSize := transactions / numOfBlocksResult
	fmt.Printf("SUCCESS: number of txs: %d, tx size: %d bytes, elapsed time: %v, avg. tx rate: %.2f, avg. tx delay: %vs, num of blocks: %d, avg. block rate: %v, avg. block size: %v txs\n", transactions, txSize, elapsed, avgTxRate, avgTxDelay, numOfBlocksResult, avgBlockRate, avgBlockSize)
}

func reportLoadResults(transactions int, elapsed time.Duration, txSize int) {
	avgTxSendingRate := float64(transactions) / elapsed.Seconds()
	fmt.Printf("Load command finished, sent %d TXs in %v seconds, TX size %d, avg. tx sending rate: %.2f\n", transactions, elapsed, txSize, avgTxSendingRate)
}

// manageStatistics manages a statistics queue and every hour writes the queue to a CSV file
func manageStatistics(receiveOutputDir string, statisticChan <-chan Statistics, stopChan <-chan bool, startTime float64, expectedTxs int, pullFrom int, timeIntervalToSampleStat time.Duration) {
	filePath := path.Join(receiveOutputDir, "statistics.csv")
	fmt.Printf("the file path is: %v\n", filePath)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open a csv file: %v", err)
		os.Exit(3)
	}
	defer file.Close()

	// write CSV header if file is new
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to stat the file: %v", err)
		os.Exit(3)
	}
	if fileInfo.Size() == 0 {
		writer := csv.NewWriter(file)
		// Write the description of the experiment
		desc := "Experiment Description: " + fmt.Sprintf("Time: %.2fs, ", startTime) + fmt.Sprintf("Receiver from assembler%d, ", pullFrom)
		if expectedTxs >= 0 {
			desc = desc + fmt.Sprintf("Expected number of txs: %d", expectedTxs)
		}

		writer.Write([]string{desc, "", "", "", "", "", "", "", ""})
		writer.Write([]string{""})
		writer.Write([]string{"Time Since Start (s)", "Number of txs", "Number of blocks", "Avg. tx rate", "Sum of txs size", "Avg. tx size (byte)", "Sum of txs delay (s)", "Avg. tx delay (s)", "Avg. block rate", "Avg. block size (byte)", "Avg. number of txs in block"})
		writer.Flush()
	}

	for {
		select {
		case statistic := <-statisticChan:
			writeStatisticsToCSV(file, statistic, timeIntervalToSampleStat)

		case <-stopChan:
			for {
				select {
				case statistic := <-statisticChan:
					writeStatisticsToCSV(file, statistic, timeIntervalToSampleStat)
				default:
					return
				}
			}
		}
	}
}

func writeStatisticsToCSV(file *os.File, statistic Statistics, timeIntervalToSampleStat time.Duration) {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	var avgTxSize int
	var avgTxDelay float64
	var avgBlockSize int
	var avgNumOfTxsInBlock int
	if statistic.numOfTxs != 0 {
		avgTxSize = statistic.sumOfTxsSize / statistic.numOfTxs
		avgTxDelay = statistic.sumOfTxsDelay / float64(statistic.numOfTxs)
		avgBlockSize = statistic.sumOfTxsSize / statistic.numOfBlocks
		avgNumOfTxsInBlock = statistic.numOfTxs / statistic.numOfBlocks
	}

	err := writer.Write([]string{
		fmt.Sprintf("%.f", statistic.timeStamp),
		fmt.Sprintf("%d", statistic.numOfTxs),
		fmt.Sprintf("%d", statistic.numOfBlocks),
		fmt.Sprintf("%.2f", float64(statistic.numOfTxs)/timeIntervalToSampleStat.Seconds()),
		fmt.Sprintf("%d", statistic.sumOfTxsSize),
		fmt.Sprintf("%d", avgTxSize),
		fmt.Sprintf("%.2f", statistic.sumOfTxsDelay),
		fmt.Sprintf("%.2f", avgTxDelay),
		fmt.Sprintf("%.2f", float64(statistic.numOfBlocks)/timeIntervalToSampleStat.Seconds()),
		fmt.Sprintf("%d", avgBlockSize),
		fmt.Sprintf("%d", avgNumOfTxsInBlock),
	})
	if err != nil {
		fmt.Printf("failed to write to CSV: %v", err)
	}
}

func receiveResponseFromAssembler(userConfigFileContent *UserInfo, txsMap *protectedMap, expectedNumOfTxs int) (int, float64) {
	// choose randomly the first assembler
	i := 0
	var serverRootCAs [][]byte
	for _, rawBytes := range userConfigFileContent.TLSCACerts {
		byteSlice := []byte(rawBytes)
		serverRootCAs = append(serverRootCAs, byteSlice)
	}

	// create a gRPC connection to the assembler
	gRPCAssemblerClient := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               userConfigFileContent.TLSPrivateKeyFile,
			Certificate:       userConfigFileContent.TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		nil,
		nextSeekInfo(0),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed create a request envelope")
		os.Exit(3)
	}

	var stream ab.AtomicBroadcast_DeliverClient
	var gRPCAssemblerClientConn *grpc.ClientConn
	endpointToPullFrom := userConfigFileContent.AssemblerEndpoints[i]

	gRPCAssemblerClientConn, err = gRPCAssemblerClient.Dial(endpointToPullFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a gRPC client connection to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver stream
	stream, err = abc.Deliver(context.TODO())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a deliver stream to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// send request envelope
	err = stream.Send(requestEnvelope)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to send a request envelope to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// pull blocks from assembler
	numOfBlocksCalculated := 0
	numOfTxsCalculated := 0
	var sumOfDelayTimes float64
	for {
		block, err := pullBlock(stream, endpointToPullFrom, gRPCAssemblerClientConn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to pull block from assembler %d: %v", i+1, err)
			os.Exit(3)
		}
		currentTime := time.Now()
		numOfBlocksCalculated += 1

		// iterate over txs in block
		for j := 0; j < len(block.Data.Data); j++ {
			numOfTxsCalculated += 1
			env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[j])
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get envelope from block: %v", err)
				os.Exit(3)
			}

			// 1. extract the sending time and calculate the delay, add the delay to sumOfDelayTimes
			readPayload := bytes.NewBuffer(env.Payload)
			startPosition := 16 + 8
			readPayload.Next(startPosition)
			var extractedSendTime uint64
			binary.Read(readPayload, binary.BigEndian, &extractedSendTime)
			sendTime := time.Unix(0, int64(extractedSendTime))
			delayTime := currentTime.Sub(sendTime)
			sumOfDelayTimes = sumOfDelayTimes + delayTime.Seconds()

			// 2. delete the tx from the map
			if txsMap != nil {
				txsMap.Remove(string(env.Payload))
			}
		}

		// if the map is empty it means we received all txs, then we stop asking for blocks from the assembler
		// NOTE: the map is relevant when using the submit command. Load and receive commands don't maintain a map.
		if expectedNumOfTxs < 0 {
			continue
		}

		if (txsMap != nil && txsMap.IsEmpty()) || numOfTxsCalculated == expectedNumOfTxs {
			break
		}
	}

	return numOfBlocksCalculated, sumOfDelayTimes
}
