/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

const (
	DefaultMaxRecvMsgSize                      = 100 * 1024 * 1024
	DefaultMaxSendMsgSize                      = 100 * 1024 * 1024
	DefaultSendBufferSize                      = 2000
	DefaultRouterMonitoringPort                = 0
	DefaultBatcherMonitoringBasePort           = 0
	DefaultConsenterMonitoringPort             = 0
	DefaultAssemblerMonitoringPort             = 0
	DefaultClientSignatureVerificationRequired = false
	DefaultMetricsLogInterval                  = time.Duration(10) * time.Second
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
	listenAddress                       string
	role                                string
	logLevel                            string
	cryptoBaseDir                       string
	configBaseDir                       string
	listenPort                          uint32
	partyID                             types.PartyID
	shardID                             types.ShardID
	tlsEnabled                          bool
	clientAuthRequired                  bool
	monitoringListenPort                uint32
	metricsLogInterval                  time.Duration
	clientSignatureVerificationRequired bool
}

// CreateArmaLocalConfig creates a config directory that includes the local config yaml files for all nodes for all parties.
func CreateArmaLocalConfig(network Network, cryptoBaseDir string, configBaseDir string, clientSignatureVerificationRequired bool) (*NetworkLocalConfig, error) {
	networkLocalConfig := createNetworkLocalConfig(network, cryptoBaseDir, configBaseDir, clientSignatureVerificationRequired)
	err := createArmaConfigFiles(networkLocalConfig, configBaseDir)
	if err != nil {
		return nil, err
	}
	return networkLocalConfig, nil
}

func createNetworkLocalConfig(network Network, cryptoBaseDir string, configBaseDir string, clientSignatureVerificationRequired bool) *NetworkLocalConfig {
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
		routerGeneralParams := NewGeneralConfigParams(party.ID, redundantShardID, "router", utils.TrimPortFromEndpoint(party.RouterEndpoint), utils.GetPortFromEndpoint(party.RouterEndpoint), DefaultRouterMonitoringPort, 10*time.Second, useTLSRouter, clientAuthRequiredRouter, "info", cryptoBaseDir, configBaseDir, clientSignatureVerificationRequired)
		consensusGeneralParams := NewGeneralConfigParams(party.ID, redundantShardID, "consenter", utils.TrimPortFromEndpoint(party.ConsenterEndpoint), utils.GetPortFromEndpoint(party.ConsenterEndpoint), DefaultConsenterMonitoringPort, DefaultMetricsLogInterval, true, false, "info", cryptoBaseDir, configBaseDir, clientSignatureVerificationRequired)
		assemblerGeneralParams := NewGeneralConfigParams(party.ID, redundantShardID, "assembler", utils.TrimPortFromEndpoint(party.AssemblerEndpoint), utils.GetPortFromEndpoint(party.AssemblerEndpoint), DefaultAssemblerMonitoringPort, DefaultMetricsLogInterval, useTLSAssembler, clientAuthRequiredAssembler, "info", cryptoBaseDir, configBaseDir, clientSignatureVerificationRequired)
		partyLocalConfig := PartyLocalConfig{
			RouterLocalConfig:    NewRouterLocalConfig(routerGeneralParams),
			BatchersLocalConfig:  NewBatchersLocalConfigPerParty(party.ID, party.BatchersEndpoints, cryptoBaseDir, configBaseDir, clientSignatureVerificationRequired),
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

func NewGeneralConfigParams(partyID types.PartyID, shardID types.ShardID, role string, listenAddress string, listenPort uint32, monitoringListenPort uint32, metricsLogInterval time.Duration, tlsEnabled bool, clientAuthRequired bool, logLevel string, cryptoBaseDir string, configBaseDir string, clientSignatureVerificationRequired bool) GeneralConfigParams {
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
		// set default monitoring values
		monitoringListenPort:                monitoringListenPort,
		metricsLogInterval:                  metricsLogInterval,
		clientSignatureVerificationRequired: clientSignatureVerificationRequired,
	}
}

func NewGeneralConfig(generalConfigParams GeneralConfigParams) *config.GeneralConfig {
	nodeRole := generalConfigParams.role
	if generalConfigParams.role == "batcher" {
		nodeRole = fmt.Sprintf("batcher%d", generalConfigParams.shardID)
	}

	partyPath := filepath.Join(generalConfigParams.cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", generalConfigParams.partyID), "orderers", fmt.Sprintf("party%d", generalConfigParams.partyID))
	orgPath := filepath.Join(generalConfigParams.cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", generalConfigParams.partyID))

	bccsp := &factory.FactoryOpts{
		Default: "SW",
		SW: &factory.SwOpts{
			Hash:     "SHA2",
			Security: 256,
		},
	}

	generalConfig := &config.GeneralConfig{
		ListenAddress:        generalConfigParams.listenAddress,
		ListenPort:           generalConfigParams.listenPort,
		MonitoringListenPort: generalConfigParams.monitoringListenPort,
		TLSConfig: config.TLSConfigYaml{
			Enabled:            generalConfigParams.tlsEnabled,
			PrivateKey:         filepath.Join(partyPath, nodeRole, "tls", "key.pem"),
			Certificate:        filepath.Join(partyPath, nodeRole, "tls", "tls-cert.pem"),
			RootCAs:            []string{filepath.Join(orgPath, "msp", "tlscacerts", "tlsca-cert.pem")},
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
		LocalMSPDir:                         filepath.Join(partyPath, nodeRole, "msp"),
		LocalMSPID:                          fmt.Sprintf("org%d", generalConfigParams.partyID),
		BCCSP:                               bccsp,
		LogSpec:                             generalConfigParams.logLevel,
		ClientSignatureVerificationRequired: generalConfigParams.clientSignatureVerificationRequired,
		MetricsLogInterval:                  generalConfigParams.metricsLogInterval,
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
		FileStore:     &config.FileStore{Path: "/var/dec-trust/production/orderer/store"},
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

func NewBatchersLocalConfigPerParty(partyID types.PartyID, batcherEndpoints []string, cryptoBaseDir string, configBaseDir string, clientSignatureVerificationRequired bool) []*config.NodeLocalConfig {
	var batchers []*config.NodeLocalConfig
	for i, batcherEndpoint := range batcherEndpoints {
		batcherGeneralParams := NewGeneralConfigParams(partyID, types.ShardID(uint16(i+1)), "batcher", utils.TrimPortFromEndpoint(batcherEndpoint), utils.GetPortFromEndpoint(batcherEndpoint), DefaultBatcherMonitoringBasePort, DefaultMetricsLogInterval, true, false, "info", cryptoBaseDir, configBaseDir, clientSignatureVerificationRequired)
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
