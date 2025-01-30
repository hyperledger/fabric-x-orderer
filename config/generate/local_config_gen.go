package generate

import (
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"arma/common/utils"

	"arma/common/types"
	"arma/config"
	"arma/node/comm"
)

const (
	DefaultMaxRecvMsgSize                = 100 * 1024 * 1024
	DefaultMaxSendMsgSize                = 100 * 1024 * 1024
	DefaultSendBufferSize                = 2000
	DefaultPrefetchBufferMemoryMB        = 1024
	DefaultNumberOfConnectionsPerBatcher = 10
	DefaultNumberOfStreamsPerConnection  = 20
)

var (
	DefaultKeepaliveOptions = comm.KeepaliveOptions{
		ClientInterval:    time.Duration(1) * time.Minute,
		ClientTimeout:     time.Duration(20) * time.Second,
		ServerInterval:    time.Duration(2) * time.Hour,
		ServerTimeout:     time.Duration(20) * time.Second,
		ServerMinInterval: time.Duration(1) * time.Minute,
	}
	DefaultBackoffOptions = comm.BackoffOptions{
		BaseDelay:  time.Second,
		Multiplier: 1.6,
		MaxDelay:   time.Minute * 2,
	}

	DefaultConnectionTimeout = 5 * time.Second
)

func NewGeneralConfig(partyID types.PartyID, shardID types.ShardID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, role string, logLevel string, baseDir string) *config.GeneralConfig {
	nodeRole := role
	if role == "batcher" {
		nodeRole = fmt.Sprintf("batcher%d", shardID)
	}

	generalConfig := &config.GeneralConfig{
		ListenAddress: listenAddress,
		ListenPort:    listenPort,
		TLSConfig: config.TLSConfig{
			Enabled:            tlsEnabled,
			PrivateKey:         filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeRole, "key.pem"),
			Certificate:        filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeRole, "tls-cert.pem"),
			RootCAs:            []string{filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("Org%d", partyID), "tlsca", "cacert.pem")},
			ClientAuthRequired: false,
		},
		KeepaliveSettings: DefaultKeepaliveOptions,
		BackoffSettings:   DefaultBackoffOptions,
		MaxRecvMsgSize:    DefaultMaxRecvMsgSize,
		MaxSendMsgSize:    DefaultMaxSendMsgSize,
		Bootstrap: config.Bootstrap{
			Method: "yaml",
			File:   "/var/dec-trust/production/orderer/bootstrap/bootstrap.yaml",
		},
		LocalMSPDir: "/var/dec-trust/production/orderer/crypto",
		LocalMSPID:  "OrdererOrg",
		BCCSP:       config.BCCSP{},
		LogSpec:     logLevel,
	}

	if role == "consenter" {
		generalConfig.Cluster = config.Cluster{
			SendBufferSize:    DefaultSendBufferSize,
			ClientCertificate: filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeRole, "tls-cert.pem"),
			ClientPrivateKey:  filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeRole, "key.pem"),
			ReplicationPolicy: "",
		}
	}
	return generalConfig
}

func NewRouterLocalConfig(partyID types.PartyID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *config.NodeLocalConfig {
	return &config.NodeLocalConfig{
		PartyID:       partyID,
		GeneralConfig: NewGeneralConfig(partyID, 0, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "router", logLevel, baseDir),
		RouterParams: &config.RouterParams{
			NumberOfConnectionsPerBatcher: DefaultNumberOfConnectionsPerBatcher,
			NumberOfStreamsPerConnection:  DefaultNumberOfStreamsPerConnection,
		},
	}
}

func createBatcherLocalConfig(partyID types.PartyID, shardID types.ShardID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *config.NodeLocalConfig {
	return &config.NodeLocalConfig{
		PartyID:       partyID,
		GeneralConfig: NewGeneralConfig(partyID, shardID, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "batcher", logLevel, baseDir),
		FileStore:     &config.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		BatcherParams: &config.BatcherParams{ShardID: shardID},
	}
}

func NewBatchersLocalConfigPerParty(partyID types.PartyID, batcherEndpoints []string, baseDir string) []*config.NodeLocalConfig {
	var batchers []*config.NodeLocalConfig
	for i, batcherEndpoint := range batcherEndpoints {
		batcher := createBatcherLocalConfig(partyID, types.ShardID(i+1), trimPortFromEndpoint(batcherEndpoint), getPortFromEndpoint(batcherEndpoint), true, false, "info", baseDir)
		batchers = append(batchers, batcher)
	}
	return batchers
}

func NewConsensusLocalConfig(partyID types.PartyID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *config.NodeLocalConfig {
	return &config.NodeLocalConfig{
		PartyID:         partyID,
		GeneralConfig:   NewGeneralConfig(partyID, 0, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "consenter", logLevel, baseDir),
		FileStore:       &config.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		ConsensusParams: &config.ConsensusParams{WALDir: "/var/dec-trust/production/orderer/store/smartbft/wal"},
	}
}

func NewAssemblerLocalConfig(partyID types.PartyID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *config.NodeLocalConfig {
	return &config.NodeLocalConfig{
		PartyID:         partyID,
		GeneralConfig:   NewGeneralConfig(partyID, 0, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "assembler", logLevel, baseDir),
		FileStore:       &config.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		AssemblerParams: &config.AssemblerParams{PrefetchBufferMemoryMB: DefaultPrefetchBufferMemoryMB},
	}
}

func createNetworkLocalConfig(network Network, baseDir string) *NetworkLocalConfig {
	var partiesLocalConfig []PartyLocalConfig

	for _, party := range network.Parties {
		partyLocalConfig := PartyLocalConfig{
			RouterLocalConfig:    NewRouterLocalConfig(party.ID, trimPortFromEndpoint(party.RouterEndpoint), getPortFromEndpoint(party.RouterEndpoint), true, false, "info", baseDir),
			BatchersLocalConfig:  NewBatchersLocalConfigPerParty(party.ID, party.BatchersEndpoints, baseDir),
			ConsenterLocalConfig: NewConsensusLocalConfig(party.ID, trimPortFromEndpoint(party.ConsenterEndpoint), getPortFromEndpoint(party.ConsenterEndpoint), true, false, "info", baseDir),
			AssemblerLocalConfig: NewAssemblerLocalConfig(party.ID, trimPortFromEndpoint(party.AssemblerEndpoint), getPortFromEndpoint(party.AssemblerEndpoint), true, false, "info", baseDir),
		}
		partiesLocalConfig = append(partiesLocalConfig, partyLocalConfig)
	}

	networkLocalConfig := &NetworkLocalConfig{
		PartiesLocalConfig: partiesLocalConfig,
	}

	return networkLocalConfig
}

func createArmaConfigFiles(networkLocalConfig *NetworkLocalConfig, outputDir string) error {
	for i, partyLocalConfig := range networkLocalConfig.PartiesLocalConfig {
		rootDir := path.Join(outputDir, "config", fmt.Sprintf("Party%d", i+1))
		os.MkdirAll(rootDir, 0o755)

		configPath := path.Join(rootDir, "local_config_router.yaml")
		err := utils.WriteToYAML(partyLocalConfig.RouterLocalConfig, configPath)
		if err != nil {
			return fmt.Errorf("error creating router local config yaml file, err: %v", err)
		}

		for j, batcherConfig := range partyLocalConfig.BatchersLocalConfig {
			configPath = path.Join(rootDir, fmt.Sprintf("local_config_batcher%d.yaml", j+1))
			err = utils.WriteToYAML(batcherConfig, configPath)
			if err != nil {
				return fmt.Errorf("error creating batcher%d local config yaml file, err: %v", j, err)
			}
		}

		configPath = path.Join(rootDir, "local_config_consenter.yaml")
		err = utils.WriteToYAML(partyLocalConfig.ConsenterLocalConfig, configPath)
		if err != nil {
			return fmt.Errorf("error creating consenter local config yaml file, err: %v", err)
		}

		configPath = path.Join(rootDir, "local_config_assembler.yaml")
		err = utils.WriteToYAML(partyLocalConfig.AssemblerLocalConfig, configPath)
		if err != nil {
			return fmt.Errorf("error creating assembler local config yaml file, err: %v", err)
		}
	}
	return nil
}

func CreateArmaLocalConfig(network Network, outputDir string) (*NetworkLocalConfig, error) {
	networkLocalConfig := createNetworkLocalConfig(network, outputDir)
	err := createArmaConfigFiles(networkLocalConfig, outputDir)
	if err != nil {
		return nil, err
	}
	return networkLocalConfig, nil
}

// LoadArmaLocalConfig loads the local configuration of all nodes for all parties.
func LoadArmaLocalConfig(path string) (*NetworkLocalConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("load arma config failed, path is empty")
	}

	partiesDirs, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read arma config failed, err: %s", err)
	}

	var partiesLocalConfig []PartyLocalConfig

	for _, party := range partiesDirs {
		partyPath := filepath.Join(path, party.Name())

		nodesYamls, err := os.ReadDir(partyPath)
		if err != nil {
			return nil, fmt.Errorf("read arma config failed, err: %s", err)
		}

		numOfShards := len(nodesYamls) - 3

		routerConfigPath := filepath.Join(partyPath, "local_config_router.yaml")
		routerLocalConfig, err := config.Load(routerConfigPath)
		if err != nil {
			return nil, err
		}

		var batchersLocalConfig []*config.NodeLocalConfig
		for j := 1; j <= numOfShards; j++ {
			batcherConfigPath := filepath.Join(partyPath, fmt.Sprintf("local_config_batcher%d.yaml", j))
			batcherLocalConfig, err := config.Load(batcherConfigPath)
			if err != nil {
				return nil, err
			}
			batchersLocalConfig = append(batchersLocalConfig, batcherLocalConfig)
		}

		consenterConfigPath := filepath.Join(partyPath, "local_config_consenter.yaml")
		consenterLocalConfig, err := config.Load(consenterConfigPath)
		if err != nil {
			return nil, err
		}

		assemblerConfigPath := filepath.Join(partyPath, "local_config_assembler.yaml")
		assemblerLocalConfig, err := config.Load(assemblerConfigPath)
		if err != nil {
			return nil, err
		}

		partyLocalConfig := PartyLocalConfig{
			RouterLocalConfig:    routerLocalConfig,
			BatchersLocalConfig:  batchersLocalConfig,
			ConsenterLocalConfig: consenterLocalConfig,
			AssemblerLocalConfig: assemblerLocalConfig,
		}

		partiesLocalConfig = append(partiesLocalConfig, partyLocalConfig)
	}
	return &NetworkLocalConfig{PartiesLocalConfig: partiesLocalConfig}, nil
}

func getPortFromEndpoint(endpoint string) int {
	if strings.Contains(endpoint, ":") {
		_, portS, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		port, err := strconv.Atoi(portS)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return port
	}

	return -1
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
