/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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

	"github.com/hyperledger/fabric-x-orderer/common/utils"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

const (
	DefaultMaxRecvMsgSize = 100 * 1024 * 1024
	DefaultMaxSendMsgSize = 100 * 1024 * 1024
	DefaultSendBufferSize = 2000
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
)

type GeneralConfigParams struct {
	listenAddress      string
	role               string
	logLevel           string
	cryptoBaseDir      string
	configBaseDir      string
	listenPort         uint32
	partyID            types.PartyID
	shardID            types.ShardID
	tlsEnabled         bool
	clientAuthRequired bool
}

// CreateArmaLocalConfig creates a config directory that includes the local config yaml files for all nodes for all parties.
func CreateArmaLocalConfig(network Network, cryptoBaseDir string, configBaseDir string) (*NetworkLocalConfig, error) {
	networkLocalConfig := createNetworkLocalConfig(network, cryptoBaseDir, configBaseDir)
	err := createArmaConfigFiles(networkLocalConfig, configBaseDir)
	if err != nil {
		return nil, err
	}
	return networkLocalConfig, nil
}

func createNetworkLocalConfig(network Network, cryptoBaseDir string, configBaseDir string) *NetworkLocalConfig {
	var partiesLocalConfig []PartyLocalConfig

	validUseTLSOptions := map[string]struct{}{
		"none": {},
		"TLS":  {},
		"mTLS": {},
	}

	if _, ok := validUseTLSOptions[network.UseTLSRouter]; !ok {
		panic("invalid UseTLSRouter option, choose one of: none, TLS, mTLS")
	}

	if _, ok := validUseTLSOptions[network.UseTLSAssembler]; !ok {
		panic("invalid UseTLSAssembler option, choose one of: none, TLS, mTLS")
	}

	useTLSRouter := network.UseTLSRouter != "none"
	clientAuthRequiredRouter := network.UseTLSRouter == "mTLS"

	useTLSAssembler := network.UseTLSAssembler != "none"
	clientAuthRequiredAssembler := network.UseTLSAssembler == "mTLS"

	redundantShardID := types.ShardID(0)
	for _, party := range network.Parties {
		routerGeneralParams := NewGeneralConfigParams(party.ID, redundantShardID, "router", trimPortFromEndpoint(party.RouterEndpoint), getPortFromEndpoint(party.RouterEndpoint), useTLSRouter, clientAuthRequiredRouter, "info", cryptoBaseDir, configBaseDir)
		consensusGeneralParams := NewGeneralConfigParams(party.ID, redundantShardID, "consenter", trimPortFromEndpoint(party.ConsenterEndpoint), getPortFromEndpoint(party.ConsenterEndpoint), true, false, "info", cryptoBaseDir, configBaseDir)
		assemblerGeneralParams := NewGeneralConfigParams(party.ID, redundantShardID, "assembler", trimPortFromEndpoint(party.AssemblerEndpoint), getPortFromEndpoint(party.AssemblerEndpoint), useTLSAssembler, clientAuthRequiredAssembler, "info", cryptoBaseDir, configBaseDir)
		partyLocalConfig := PartyLocalConfig{
			RouterLocalConfig:    NewRouterLocalConfig(routerGeneralParams),
			BatchersLocalConfig:  NewBatchersLocalConfigPerParty(party.ID, party.BatchersEndpoints, cryptoBaseDir, configBaseDir),
			ConsenterLocalConfig: NewConsensusLocalConfig(consensusGeneralParams),
			AssemblerLocalConfig: NewAssemblerLocalConfig(assemblerGeneralParams),
		}
		partiesLocalConfig = append(partiesLocalConfig, partyLocalConfig)
	}

	networkLocalConfig := &NetworkLocalConfig{
		PartiesLocalConfig: partiesLocalConfig,
	}

	return networkLocalConfig
}

func createArmaConfigFiles(networkLocalConfig *NetworkLocalConfig, configBaseDir string) error {
	for i, partyLocalConfig := range networkLocalConfig.PartiesLocalConfig {
		err := createPartyConfigFiles(partyLocalConfig, configBaseDir, types.PartyID(uint16(i+1)))
		if err != nil {
			return err
		}
	}
	return nil
}

func createPartyConfigFiles(partyLocalConfig PartyLocalConfig, configBaseDir string, partyID types.PartyID) error {
	rootDir := path.Join(configBaseDir, "config", fmt.Sprintf("party%d", partyID))
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

	return nil
}

func NewGeneralConfigParams(partyID types.PartyID, shardID types.ShardID, role string, listenAddress string, listenPort uint32, tlsEnabled bool, clientAuthRequired bool, logLevel string, cryptoBaseDir string, configBaseDir string) GeneralConfigParams {
	return GeneralConfigParams{
		partyID:            partyID,
		shardID:            shardID,
		listenAddress:      listenAddress,
		role:               role,
		logLevel:           logLevel,
		cryptoBaseDir:      cryptoBaseDir,
		configBaseDir:      configBaseDir,
		listenPort:         listenPort,
		tlsEnabled:         tlsEnabled,
		clientAuthRequired: clientAuthRequired,
	}
}

func NewGeneralConfig(generalConfigParams GeneralConfigParams) *config.GeneralConfig {
	nodeRole := generalConfigParams.role
	if generalConfigParams.role == "batcher" {
		nodeRole = fmt.Sprintf("batcher%d", generalConfigParams.shardID)
	}

	partyPath := filepath.Join(generalConfigParams.cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", generalConfigParams.partyID), "orderers", fmt.Sprintf("party%d", generalConfigParams.partyID))
	orgPath := filepath.Join(generalConfigParams.cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", generalConfigParams.partyID))

	generalConfig := &config.GeneralConfig{
		ListenAddress: generalConfigParams.listenAddress,
		ListenPort:    generalConfigParams.listenPort,
		TLSConfig: config.TLSConfigYaml{
			Enabled:            generalConfigParams.tlsEnabled,
			PrivateKey:         filepath.Join(partyPath, nodeRole, "tls", "key.pem"),
			Certificate:        filepath.Join(partyPath, nodeRole, "tls", "tls-cert.pem"),
			RootCAs:            []string{filepath.Join(orgPath, "tlsca", "tlsca-cert.pem")},
			ClientAuthRequired: generalConfigParams.clientAuthRequired,
		},
		KeepaliveSettings: DefaultKeepaliveOptions,
		BackoffSettings:   DefaultBackoffOptions,
		MaxRecvMsgSize:    DefaultMaxRecvMsgSize,
		MaxSendMsgSize:    DefaultMaxSendMsgSize,
		Bootstrap: config.Bootstrap{
			Method: "block",
			File:   filepath.Join(generalConfigParams.configBaseDir, "bootstrap", "bootstrap.block"),
		},
		LocalMSPDir: filepath.Join(partyPath, nodeRole, "msp"),
		LocalMSPID:  fmt.Sprintf("OrdererOrg%d", generalConfigParams.partyID),
		BCCSP:       config.BCCSP{},
		LogSpec:     generalConfigParams.logLevel,
	}

	if generalConfigParams.role == "consenter" {
		generalConfig.Cluster = config.ClusterYaml{
			SendBufferSize:    DefaultSendBufferSize,
			ClientCertificate: filepath.Join(partyPath, nodeRole, "tls", "tls-cert.pem"),
			ClientPrivateKey:  filepath.Join(partyPath, nodeRole, "tls", "key.pem"),
			ReplicationPolicy: "",
		}
	}
	return generalConfig
}

func NewRouterLocalConfig(routerGeneralParams GeneralConfigParams) *config.NodeLocalConfig {
	params := config.DefaultRouterParams
	return &config.NodeLocalConfig{
		PartyID:       routerGeneralParams.partyID,
		GeneralConfig: NewGeneralConfig(routerGeneralParams),
		FileStore:     &config.FileStore{},
		RouterParams:  &params,
	}
}

func createBatcherLocalConfig(batcherGeneralParams GeneralConfigParams) *config.NodeLocalConfig {
	return &config.NodeLocalConfig{
		PartyID:       batcherGeneralParams.partyID,
		GeneralConfig: NewGeneralConfig(batcherGeneralParams),
		FileStore:     &config.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		BatcherParams: &config.BatcherParams{
			ShardID:          batcherGeneralParams.shardID,
			BatchSequenceGap: config.DefaultBatcherParams.BatchSequenceGap,
			MemPoolMaxSize:   config.DefaultBatcherParams.MemPoolMaxSize,
			SubmitTimeout:    config.DefaultBatcherParams.SubmitTimeout,
		},
	}
}

func NewBatchersLocalConfigPerParty(partyID types.PartyID, batcherEndpoints []string, cryptoBaseDir string, configBaseDir string) []*config.NodeLocalConfig {
	var batchers []*config.NodeLocalConfig
	for i, batcherEndpoint := range batcherEndpoints {
		batcherGeneralParams := NewGeneralConfigParams(partyID, types.ShardID(uint16(i+1)), "batcher", trimPortFromEndpoint(batcherEndpoint), getPortFromEndpoint(batcherEndpoint), true, false, "info", cryptoBaseDir, configBaseDir)
		batcher := createBatcherLocalConfig(batcherGeneralParams)
		batchers = append(batchers, batcher)
	}
	return batchers
}

func NewConsensusLocalConfig(consensusGeneralParams GeneralConfigParams) *config.NodeLocalConfig {
	fileStorePath := "/var/dec-trust/production/orderer/store"
	return &config.NodeLocalConfig{
		PartyID:         consensusGeneralParams.partyID,
		GeneralConfig:   NewGeneralConfig(consensusGeneralParams),
		FileStore:       &config.FileStore{Path: fileStorePath},
		ConsensusParams: &config.ConsensusParams{WALDir: config.DefaultConsenterNodeConfigParams(fileStorePath).WALDir},
	}
}

func NewAssemblerLocalConfig(assemblerGeneralParams GeneralConfigParams) *config.NodeLocalConfig {
	params := config.DefaultAssemblerParams
	return &config.NodeLocalConfig{
		PartyID:         assemblerGeneralParams.partyID,
		GeneralConfig:   NewGeneralConfig(assemblerGeneralParams),
		FileStore:       &config.FileStore{Path: "/var/dec-trust/production/orderer/store"},
		AssemblerParams: &params,
	}
}

func getPortFromEndpoint(endpoint string) uint32 {
	if strings.Contains(endpoint, ":") {
		_, portS, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		port, err := strconv.ParseUint(portS, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return uint32(port)
	}

	return 0
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
