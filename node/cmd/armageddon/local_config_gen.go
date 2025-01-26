package armageddon

import (
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"arma/common/types"
	localconfig "arma/config"
	"arma/node/comm"
	"arma/node/config"

	"gopkg.in/yaml.v3"
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

func NewGeneralConfig(partyID types.PartyID, shardID types.ShardID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, role string, logLevel string, baseDir string) *localconfig.GeneralConfig {
	nodeRole := role
	if role == "batcher" {
		nodeRole = fmt.Sprintf("batcher%d", shardID)
	}

	generalConfig := &localconfig.GeneralConfig{
		ListenAddress: listenAddress,
		ListenPort:    listenPort,
		TLSConfig: localconfig.TLSConfig{
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
		Bootstrap: localconfig.Bootstrap{
			Method: "yaml",
			File:   "/var/dec-trust/production/orderer/bootstrap/bootstrap.yaml",
		},
		LocalMSPDir: "/var/dec-trust/production/orderer/crypto",
		LocalMSPID:  "OrdererOrg",
		BCCSP:       localconfig.BCCSP{},
		LogSpec:     logLevel,
	}

	if role == "consenter" {
		generalConfig.Cluster = localconfig.Cluster{
			SendBufferSize:    DefaultSendBufferSize,
			ClientCertificate: filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeRole, "tls-cert.pem"),
			ClientPrivateKey:  filepath.Join(baseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeRole, "key.pem"),
			ReplicationPolicy: "",
		}
	}
	return generalConfig
}

func NewRouterLocalConfig(partyID types.PartyID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *localconfig.NodeLocalConfig {
	return &localconfig.NodeLocalConfig{
		PartyID:       partyID,
		GeneralConfig: NewGeneralConfig(partyID, 0, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "router", logLevel, baseDir),
		RouterParams: localconfig.RouterParams{
			NumberOfConnectionsPerBatcher: DefaultNumberOfConnectionsPerBatcher,
			NumberOfStreamsPerConnection:  DefaultNumberOfStreamsPerConnection,
		},
	}
}

func createBatcherLocalConfig(partyID types.PartyID, shardID types.ShardID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *localconfig.NodeLocalConfig {
	return &localconfig.NodeLocalConfig{
		PartyID:       partyID,
		GeneralConfig: NewGeneralConfig(partyID, shardID, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "batcher", logLevel, baseDir),
		FileStore:     &localconfig.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		BatcherParams: localconfig.BatcherParams{ShardID: shardID},
	}
}

func NewBatchersLocalConfigPerParty(partyID types.PartyID, batcherEndpoints []string, baseDir string) []*localconfig.NodeLocalConfig {
	var batchers []*localconfig.NodeLocalConfig
	for i, batcherEndpoint := range batcherEndpoints {
		batcher := createBatcherLocalConfig(partyID, types.ShardID(i+1), trimPortFromEndpoint(batcherEndpoint), getPortFromEndpoint(batcherEndpoint), true, false, "info", baseDir)
		batchers = append(batchers, batcher)
	}
	return batchers
}

func NewConsensusLocalConfig(partyID types.PartyID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *localconfig.NodeLocalConfig {
	return &localconfig.NodeLocalConfig{
		PartyID:         partyID,
		GeneralConfig:   NewGeneralConfig(partyID, 0, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "consenter", logLevel, baseDir),
		FileStore:       &localconfig.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		ConsensusParams: localconfig.ConsensusParams{WALDir: "/var/dec-trust/production/orderer/store/smartbft/wal"},
	}
}

func NewAssemblerLocalConfig(partyID types.PartyID, listenAddress string, listenPort int, tlsEnabled bool, clientAuthRequired bool, logLevel string, baseDir string) *localconfig.NodeLocalConfig {
	return &localconfig.NodeLocalConfig{
		PartyID:         partyID,
		GeneralConfig:   NewGeneralConfig(partyID, 0, listenAddress, listenPort, tlsEnabled, clientAuthRequired, "assembler", logLevel, baseDir),
		FileStore:       &localconfig.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		AssemblerParams: localconfig.AssemblerParams{PrefetchBufferMemoryMB: DefaultPrefetchBufferMemoryMB},
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
		err := config.NodeConfigToYAML(partyLocalConfig.RouterLocalConfig, configPath)
		if err != nil {
			return fmt.Errorf("error creating router local config yaml file, err: %v", err)
		}

		for j, batcherConfig := range partyLocalConfig.BatchersLocalConfig {
			configPath = path.Join(rootDir, fmt.Sprintf("local_config_batcher%d.yaml", j+1))
			err = config.NodeConfigToYAML(batcherConfig, configPath)
			if err != nil {
				return fmt.Errorf("error creating batcher%d local config yaml file, err: %v", j, err)
			}
		}

		configPath = path.Join(rootDir, "local_config_consenter.yaml")
		err = config.NodeConfigToYAML(partyLocalConfig.ConsenterLocalConfig, configPath)
		if err != nil {
			return fmt.Errorf("error creating consenter local config yaml file, err: %v", err)
		}

		configPath = path.Join(rootDir, "local_config_assembler.yaml")
		err = config.NodeConfigToYAML(partyLocalConfig.AssemblerLocalConfig, configPath)
		if err != nil {
			return fmt.Errorf("error creating assembler local config yaml file, err: %v", err)
		}
	}
	return nil
}

func CreateArmaLocalConfig(network Network, outputDir string) error {
	networkLocalConfig := createNetworkLocalConfig(network, outputDir)
	err := createArmaConfigFiles(networkLocalConfig, outputDir)
	return err
}

func NodeConfigToYAML(config interface{}, path string) error {
	rnc, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, rnc, 0o644)
	if err != nil {
		return err
	}

	return nil
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
