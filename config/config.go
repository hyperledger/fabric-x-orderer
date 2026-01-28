/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/node"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Configuration holds the complete configuration of a database node.
type Configuration struct {
	LocalConfig  *LocalConfig
	SharedConfig *protos.SharedConfig
}

// ReadConfig reads the configurations from the config file and returns it. The configuration includes both local and shared.
func ReadConfig(configFilePath string, logger types.Logger) (*Configuration, *common.Block, error) {
	if configFilePath == "" {
		return nil, nil, errors.New("path to the configuration file is empty")
	}

	var err error
	var nodeRole string
	conf := &Configuration{
		LocalConfig:  &LocalConfig{},
		SharedConfig: &protos.SharedConfig{},
	}

	conf.LocalConfig, nodeRole, err = LoadLocalConfig(configFilePath)
	if err != nil {
		return nil, nil, err
	}

	if conf.LocalConfig.NodeLocalConfig.FileStore == nil || conf.LocalConfig.NodeLocalConfig.FileStore.Path == "" {
		return nil, nil, errors.New("path to the FileStore is missing in local config")
	}

	var lastConfigBlock *common.Block
	var configStore *configstore.Store

	switch conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method {
	case "yaml":
		if conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File != "" {
			conf.SharedConfig, _, err = LoadSharedConfig(conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to read the shared configuration from: %s", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			}
		} else {
			return nil, nil, errors.Wrapf(err, "failed to read shared config, path is empty")
		}
	case "block":
		logger.Infof("reading shared config from block for %s node", nodeRole)
		// If node is router or batcher, check if config store has blocks, and if yes bootstrap from the last block
		if nodeRole == RouterStr || nodeRole == BatcherStr {
			configStore, err = configstore.NewStore(conf.LocalConfig.NodeLocalConfig.FileStore.Path)
			if err != nil {
				return nil, nil, fmt.Errorf("failed creating %s config store: %s", nodeRole, err)
			}
			listBlockNumbers, err := configStore.ListBlockNumbers()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to list blocks from %s config store: %s", nodeRole, err)
			}

			if len(listBlockNumbers) > 0 {
				lastBlockNumberIdx := uint64(len(listBlockNumbers) - 1)
				lastBlockNumber := listBlockNumbers[lastBlockNumberIdx]
				lastConfigBlock, err = configStore.GetByNumber(lastBlockNumber)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to retrieve the last block from %s config store: %s", nodeRole, err)
				}
				logger.Infof("last block number %d was retrieved from %s config store", lastBlockNumber, nodeRole)
			}
		}

		// If node is assembler, check if its ledger has blocks, and if yes bootstrap from the last block
		if nodeRole == AssemblerStr {
			assemblerLedgerFactory := &node_ledger.DefaultAssemblerLedgerFactory{}
			assemblerLedger, err := assemblerLedgerFactory.Create(logger, conf.LocalConfig.NodeLocalConfig.FileStore.Path)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create assembler ledger instance: %s", err)
			}
			defer assemblerLedger.Close()
			if assemblerLedger.LedgerReader().Height() > 0 {
				lastConfigBlock, err = node_ledger.GetLastConfigBlockFromAssemblerLedger(assemblerLedger)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to retrieve last config block from assembler ledger: %s", err)
				}
			}
		}

		// If node is consensus, get the last decision from the ledger, extract the decision number of the last config block and get the last available block from it.
		if nodeRole == ConsensusStr {
			consensusLedger, err := node_ledger.NewConsensusLedger(conf.LocalConfig.NodeLocalConfig.FileStore.Path)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create consensus ledger instance: %s", err)
			}
			defer consensusLedger.Close()

			lastConfigBlock, err = GetLastConfigBlockFromConsensusLedger(consensusLedger, logger)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get the last config block from consensus ledger instance: %s", err)
			}
		}

		if lastConfigBlock == nil {
			lastConfigBlock, err = readGenesisBlockFromBootstrapPath(conf.LocalConfig)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read genesis block from bootstrap file, err: %v", err)
			}

			// this is relevant only for batcher and router nodes
			if configStore != nil {
				err = configStore.Add(lastConfigBlock)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to add the genesis block to the config store: %s", err)
				}
				logger.Infof("Append genesis block to the %s config store", nodeRole)
			}
		}

		conf.SharedConfig, err = sharedConfigFromBlock(lastConfigBlock)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read the shared configuration from block, err: %v", err)
		}

	default:
		return nil, nil, errors.Errorf("bootstrap method %s is invalid", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method)
	}

	return conf, lastConfigBlock, nil
}

func readGenesisBlockFromBootstrapPath(conf *LocalConfig) (*common.Block, error) {
	if conf.NodeLocalConfig.GeneralConfig.Bootstrap.File == "" {
		return nil, errors.Errorf("failed to read a config block, path is empty")
	}

	blockPath := conf.NodeLocalConfig.GeneralConfig.Bootstrap.File
	data, err := os.ReadFile(blockPath)
	if err != nil {
		return nil, fmt.Errorf("could not read block %s", blockPath)
	}
	genesisBlock, err := protoutil.UnmarshalBlock(data)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling to block: %s", err)
	}
	return genesisBlock, nil
}

func sharedConfigFromBlock(block *common.Block) (*protos.SharedConfig, error) {
	consensusMetaData, err := ReadSharedConfigFromBootstrapConfigBlock(block)
	if err != nil {
		return nil, err
	}

	sharedConfig := &protos.SharedConfig{}
	err = proto.Unmarshal(consensusMetaData, sharedConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
	}

	return sharedConfig, nil
}

func (config *Configuration) GetBFTConfig(partyID types.PartyID) (smartbft_types.Configuration, error) {
	smartBFTConfigFetchedFromSharedConfig := config.SharedConfig.ConsensusConfig.SmartBFTConfig

	var err error
	var BFTConfig smartbft_types.Configuration

	if BFTConfig.RequestBatchMaxInterval, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestBatchMaxInterval); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestBatchMaxInterval")
	}
	if BFTConfig.RequestForwardTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestForwardTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestForwardTimeout")
	}
	if BFTConfig.RequestComplainTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestComplainTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestComplainTimeout")
	}
	if BFTConfig.RequestAutoRemoveTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestAutoRemoveTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestAutoRemoveTimeout")
	}
	if BFTConfig.ViewChangeResendInterval, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.ViewChangeResendInterval); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option ViewChangeResendInterval")
	}
	if BFTConfig.ViewChangeTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.ViewChangeTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option ViewChangeTimeout")
	}
	if BFTConfig.LeaderHeartbeatTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.LeaderHeartbeatTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option LeaderHeartbeatTimeout")
	}
	if BFTConfig.CollectTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.CollectTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option CollectTimeout")
	}
	if BFTConfig.RequestPoolSubmitTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestPoolSubmitTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestPoolSubmitTimeout")
	}

	BFTConfig.SelfID = uint64(partyID)
	BFTConfig.RequestBatchMaxCount = smartBFTConfigFetchedFromSharedConfig.RequestBatchMaxCount
	BFTConfig.RequestBatchMaxBytes = smartBFTConfigFetchedFromSharedConfig.RequestBatchMaxBytes
	BFTConfig.IncomingMessageBufferSize = smartBFTConfigFetchedFromSharedConfig.IncomingMessageBufferSize
	BFTConfig.RequestPoolSize = smartBFTConfigFetchedFromSharedConfig.RequestPoolSize
	BFTConfig.LeaderHeartbeatCount = smartBFTConfigFetchedFromSharedConfig.LeaderHeartbeatCount
	BFTConfig.NumOfTicksBehindBeforeSyncing = smartBFTConfigFetchedFromSharedConfig.NumOfTicksBehindBeforeSyncing
	BFTConfig.SyncOnStart = smartBFTConfigFetchedFromSharedConfig.SyncOnStart
	BFTConfig.SpeedUpViewChange = smartBFTConfigFetchedFromSharedConfig.SpeedUpViewChange
	BFTConfig.LeaderRotation = smartBFTConfigFetchedFromSharedConfig.LeaderRotation
	BFTConfig.DecisionsPerLeader = smartBFTConfigFetchedFromSharedConfig.DecisionsPerLeader
	BFTConfig.RequestMaxBytes = smartBFTConfigFetchedFromSharedConfig.RequestMaxBytes

	return BFTConfig, nil
}

func (config *Configuration) ExtractRouterConfig(configBlock *common.Block) *nodeconfig.RouterNodeConfig {
	if config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress == "" {
		config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress = config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress
	}

	// use shards to get every party's RootCAs
	shards := config.ExtractShards()
	orderingServiceTrustedRootCAs := node.TLSCAcertsFromShards(shards)
	bundle := config.extractBundleFromConfigBlock(configBlock)
	appTrustedRoots := ExtractAppTrustedRootsFromConfigBlock(bundle)
	localConfigClientsTrustedRoots := config.LocalConfig.TLSConfig.ClientRootCAs
	trustedRoots := make([][]byte, 0, len(orderingServiceTrustedRootCAs)+len(appTrustedRoots)+len(localConfigClientsTrustedRoots))
	trustedRoots = append(trustedRoots, orderingServiceTrustedRootCAs...)
	trustedRoots = append(trustedRoots, appTrustedRoots...)
	trustedRoots = append(trustedRoots, localConfigClientsTrustedRoots...)

	routerConfig := &nodeconfig.RouterNodeConfig{
		PartyID:                             config.LocalConfig.NodeLocalConfig.PartyID,
		TLSCertificateFile:                  config.LocalConfig.TLSConfig.Certificate,
		TLSPrivateKeyFile:                   config.LocalConfig.TLSConfig.PrivateKey,
		ListenAddress:                       net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort))),
		ConfigStorePath:                     config.LocalConfig.NodeLocalConfig.FileStore.Path,
		Shards:                              shards,
		Consenter:                           config.ExtractConsenterInParty(),
		NumOfConnectionsForBatcher:          config.LocalConfig.NodeLocalConfig.RouterParams.NumberOfConnectionsPerBatcher,
		NumOfgRPCStreamsPerConnection:       config.LocalConfig.NodeLocalConfig.RouterParams.NumberOfStreamsPerConnection,
		UseTLS:                              config.LocalConfig.TLSConfig.Enabled,
		ClientAuthRequired:                  config.LocalConfig.TLSConfig.ClientAuthRequired,
		ClientRootCAs:                       trustedRoots,
		RequestMaxBytes:                     config.SharedConfig.BatchingConfig.RequestMaxBytes,
		ClientSignatureVerificationRequired: config.LocalConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired,
		Bundle:                              bundle,
		MonitoringListenAddress:             net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenPort))),
		MetricsLogInterval:                  config.LocalConfig.NodeLocalConfig.GeneralConfig.MetricsLogInterval,
	}
	return routerConfig
}

func (config *Configuration) ExtractBatcherConfig(configBlock *common.Block) *nodeconfig.BatcherNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "keystore", "priv_sk"))
	if err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config: %s", err))
	}
	if config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress == "" {
		config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress = config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress
	}

	batcherConfig := &nodeconfig.BatcherNodeConfig{
		Shards:                              config.ExtractShards(),
		Consenters:                          config.ExtractConsenters(),
		Directory:                           config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:                       net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort))),
		ConfigStorePath:                     config.LocalConfig.NodeLocalConfig.FileStore.Path,
		PartyId:                             config.LocalConfig.NodeLocalConfig.PartyID,
		ShardId:                             config.LocalConfig.NodeLocalConfig.BatcherParams.ShardID,
		TLSPrivateKeyFile:                   config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile:                  config.LocalConfig.TLSConfig.Certificate,
		ClientRootCAs:                       config.LocalConfig.TLSConfig.ClientRootCAs,
		SigningPrivateKey:                   signingPrivateKey,
		MemPoolMaxSize:                      config.LocalConfig.NodeLocalConfig.BatcherParams.MemPoolMaxSize,
		BatchMaxSize:                        config.SharedConfig.BatchingConfig.BatchSize.MaxMessageCount,
		BatchMaxBytes:                       config.SharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes,
		RequestMaxBytes:                     config.SharedConfig.BatchingConfig.RequestMaxBytes,
		SubmitTimeout:                       config.LocalConfig.NodeLocalConfig.BatcherParams.SubmitTimeout,
		BatchSequenceGap:                    types.BatchSequence(config.LocalConfig.NodeLocalConfig.BatcherParams.BatchSequenceGap),
		MonitoringListenAddress:             net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenPort))),
		MetricsLogInterval:                  config.LocalConfig.NodeLocalConfig.GeneralConfig.MetricsLogInterval,
		ClientSignatureVerificationRequired: config.LocalConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired,
		Bundle:                              config.extractBundleFromConfigBlock(configBlock),
	}

	if batcherConfig.FirstStrikeThreshold, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.FirstStrikeThreshold); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option FirstStrikeThreshold, err: %s", err))
	}

	if batcherConfig.SecondStrikeThreshold, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.SecondStrikeThreshold); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option SecondStrikeThreshold, err: %s", err))
	}

	if batcherConfig.AutoRemoveTimeout, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.AutoRemoveTimeout); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option AutoRemoveTimeout, err: %s", err))
	}

	if batcherConfig.BatchCreationTimeout, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.BatchCreationTimeout); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option BatchCreationTimeout, err: %s", err))
	}

	return batcherConfig
}

func (config *Configuration) ExtractConsenterConfig(configBlock *common.Block) *nodeconfig.ConsenterNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "keystore", "priv_sk"))
	if err != nil {
		panic(fmt.Sprintf("error launching consenter, failed extracting consenter config: %s", err))
	}
	BFTConfig, err := config.GetBFTConfig(config.LocalConfig.NodeLocalConfig.PartyID)
	if err != nil {
		panic(fmt.Sprintf("error launching consenter, failed extracting consenter config: %s", err))
	}
	if config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress == "" {
		config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress = config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress
	}
	consenterConfig := &nodeconfig.ConsenterNodeConfig{
		Shards:                              config.ExtractShards(),
		Consenters:                          config.ExtractConsenters(),
		Router:                              config.ExtractRouterInParty(),
		Directory:                           config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:                       net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort))),
		PartyId:                             config.LocalConfig.NodeLocalConfig.PartyID,
		TLSPrivateKeyFile:                   config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile:                  config.LocalConfig.TLSConfig.Certificate,
		ClientRootCAs:                       config.LocalConfig.TLSConfig.ClientRootCAs,
		SigningPrivateKey:                   signingPrivateKey,
		WALDir:                              DefaultConsenterNodeConfigParams(config.LocalConfig.NodeLocalConfig.FileStore.Path).WALDir,
		BFTConfig:                           BFTConfig,
		MonitoringListenAddress:             net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenPort))),
		MetricsLogInterval:                  config.LocalConfig.NodeLocalConfig.GeneralConfig.MetricsLogInterval,
		ClientSignatureVerificationRequired: config.LocalConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired,
		Bundle:                              config.extractBundleFromConfigBlock(configBlock),
		RequestMaxBytes:                     config.SharedConfig.BatchingConfig.RequestMaxBytes,
	}
	return consenterConfig
}

func (config *Configuration) ExtractAssemblerConfig(configBlock *common.Block) *nodeconfig.AssemblerNodeConfig {
	consenters := config.ExtractConsenters()
	var consenterFromMyParty nodeconfig.ConsenterInfo
	for _, consenter := range consenters {
		if consenter.PartyID == config.LocalConfig.NodeLocalConfig.PartyID {
			consenterFromMyParty = consenter
			break
		}
	}
	if config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress == "" {
		config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress = config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress
	}

	// use shards to get every party's RootCAs
	shards := config.ExtractShards()
	orderingServiceTrustedRootCAs := node.TLSCAcertsFromShards(shards)
	bundle := config.extractBundleFromConfigBlock(configBlock)
	appTrustedRoots := ExtractAppTrustedRootsFromConfigBlock(bundle)
	localConfigClientsTrustedRoots := config.LocalConfig.TLSConfig.ClientRootCAs
	trustedRoots := make([][]byte, 0, len(orderingServiceTrustedRootCAs)+len(appTrustedRoots)+len(localConfigClientsTrustedRoots))
	trustedRoots = append(trustedRoots, orderingServiceTrustedRootCAs...)
	trustedRoots = append(trustedRoots, appTrustedRoots...)
	trustedRoots = append(trustedRoots, localConfigClientsTrustedRoots...)

	assemblerConfig := &nodeconfig.AssemblerNodeConfig{
		TLSPrivateKeyFile:         config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile:        config.LocalConfig.TLSConfig.Certificate,
		PartyId:                   config.LocalConfig.NodeLocalConfig.PartyID,
		Directory:                 config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:             net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort))),
		PrefetchBufferMemoryBytes: config.LocalConfig.NodeLocalConfig.AssemblerParams.PrefetchBufferMemoryBytes,
		RestartLedgerScanTimeout:  config.LocalConfig.NodeLocalConfig.AssemblerParams.RestartLedgerScanTimeout,
		PrefetchEvictionTtl:       config.LocalConfig.NodeLocalConfig.AssemblerParams.PrefetchEvictionTtl,
		PopWaitMonitorTimeout:     config.LocalConfig.NodeLocalConfig.AssemblerParams.PopWaitMonitorTimeout,
		ReplicationChannelSize:    config.LocalConfig.NodeLocalConfig.AssemblerParams.ReplicationChannelSize,
		BatchRequestsChannelSize:  config.LocalConfig.NodeLocalConfig.AssemblerParams.BatchRequestsChannelSize,
		Shards:                    shards,
		Consenter:                 consenterFromMyParty,
		UseTLS:                    config.LocalConfig.TLSConfig.Enabled,
		ClientAuthRequired:        config.LocalConfig.TLSConfig.ClientAuthRequired,
		ClientRootCAs:             trustedRoots,
		MonitoringListenAddress:   net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.MonitoringListenPort))),
		MetricsLogInterval:        config.LocalConfig.NodeLocalConfig.GeneralConfig.MetricsLogInterval,
		Bundle:                    bundle,
	}
	return assemblerConfig
}

func ExtractAppTrustedRootsFromConfigBlock(bundle channelconfig.Resources) [][]byte {
	appRootCAs := [][]byte{}
	appOrgMSPs := make(map[string]struct{})

	if ac, ok := bundle.ApplicationConfig(); ok {
		// loop through app orgs and build map of MSPIDs
		for _, appOrg := range ac.Organizations() {
			appOrgMSPs[appOrg.MSPID()] = struct{}{}
		}
	}

	msps, err := bundle.MSPManager().GetMSPs()
	if err != nil {
		panic(fmt.Sprintf("Error getting root CAs from bundle msp manager, err: %s", err))
	}

	for k, v := range msps {
		// check to see if this is a FABRIC MSP
		if v.GetType() == msp.FABRIC {
			for _, root := range v.GetTLSRootCerts() {
				// check to see of this is an app org MSP
				if _, ok := appOrgMSPs[k]; ok {
					appRootCAs = append(appRootCAs, root)
				}
			}
			for _, intermediate := range v.GetTLSIntermediateCerts() {
				// check to see of this is an app org MSP
				if _, ok := appOrgMSPs[k]; ok {
					appRootCAs = append(appRootCAs, intermediate)
				}
			}
		}
	}
	return appRootCAs
}

func (config *Configuration) ExtractShards() []nodeconfig.ShardInfo {
	shardToBatchers := make(map[types.ShardID][]nodeconfig.BatcherInfo)
	for _, party := range config.SharedConfig.PartiesConfig {

		var tlsCACertsCollection []nodeconfig.RawBytes
		for _, ca := range party.TLSCACerts {
			tlsCACertsCollection = append(tlsCACertsCollection, ca)
		}

		for _, batcher := range party.BatchersConfig {
			shardId := types.ShardID(batcher.ShardID)

			pemPublicKey := utils.GetPublicKeyFromCertificate(batcher.SignCert)

			batcher := nodeconfig.BatcherInfo{
				PartyID:    types.PartyID(party.PartyID),
				Endpoint:   net.JoinHostPort(batcher.Host, strconv.Itoa(int(batcher.Port))),
				TLSCACerts: tlsCACertsCollection,
				PublicKey:  pemPublicKey,
				TLSCert:    batcher.TlsCert,
			}
			shardToBatchers[shardId] = append(shardToBatchers[shardId], batcher)
		}
	}

	// build Shards from the map
	var shards []nodeconfig.ShardInfo
	for shardId, batchers := range shardToBatchers {
		shardInfo := nodeconfig.ShardInfo{
			ShardId:  shardId,
			Batchers: batchers,
		}
		shards = append(shards, shardInfo)
	}

	sort.Slice(shards, func(i, j int) bool {
		return int(shards[i].ShardId) < int(shards[j].ShardId)
	})

	return shards
}

func (config *Configuration) ExtractConsenters() []nodeconfig.ConsenterInfo {
	var consenters []nodeconfig.ConsenterInfo
	for _, party := range config.SharedConfig.PartiesConfig {
		var tlsCACertsCollection []nodeconfig.RawBytes
		for _, ca := range party.TLSCACerts {
			tlsCACertsCollection = append(tlsCACertsCollection, ca)
		}

		pemPublicKey := utils.GetPublicKeyFromCertificate(party.ConsenterConfig.SignCert)

		consenterInfo := nodeconfig.ConsenterInfo{
			PartyID:    types.PartyID(party.PartyID),
			Endpoint:   net.JoinHostPort(party.ConsenterConfig.Host, strconv.Itoa(int(party.ConsenterConfig.Port))),
			PublicKey:  pemPublicKey,
			TLSCACerts: tlsCACertsCollection,
		}
		consenters = append(consenters, consenterInfo)
	}

	return consenters
}

func (config *Configuration) ExtractRouterInParty() nodeconfig.RouterInfo {
	partyID := config.LocalConfig.NodeLocalConfig.PartyID
	var party *protos.PartyConfig
	for _, p := range config.SharedConfig.PartiesConfig {
		if types.PartyID(p.PartyID) == partyID {
			party = p
		}
	}
	if party == nil {
		panic("failed to extract router from config")
	}

	routerConfig := party.RouterConfig

	var tlsCACertsCollection []nodeconfig.RawBytes
	for _, ca := range party.TLSCACerts {
		tlsCACertsCollection = append(tlsCACertsCollection, ca)
	}

	routerInfo := nodeconfig.RouterInfo{
		PartyID:    partyID,
		Endpoint:   net.JoinHostPort(routerConfig.Host, strconv.Itoa(int(routerConfig.Port))),
		TLSCACerts: tlsCACertsCollection,
		TLSCert:    routerConfig.TlsCert,
	}

	return routerInfo
}

func (config *Configuration) ExtractConsenterInParty() nodeconfig.ConsenterInfo {
	consenterInfos := config.ExtractConsenters()
	for _, consenter := range consenterInfos {
		if consenter.PartyID == config.LocalConfig.NodeLocalConfig.PartyID {
			return consenter
		}
	}
	panic("failed to extract consenter from config")
}

func (config *Configuration) extractBundleFromConfigBlock(configBlock *common.Block) channelconfig.Resources {
	bccsp, err := (&factory.SWFactory{}).Get(config.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP)
	if err != nil {
		bccsp = factory.GetDefault()
	}
	env, err := protoutil.GetEnvelopeFromBlock(configBlock.Data.Data[0])
	if err != nil {
		panic(fmt.Sprintf("failed to get envelope from config block: %s", err))
	}
	bundle, err := policy.BuildBundleFromBlock(env, bccsp)
	if err != nil {
		panic(fmt.Sprintf("failed to build bundle from config block: %s", err))
	}
	return bundle
}

func GetLastConfigBlockFromConsensusLedger(consensusLedger *node_ledger.ConsensusLedger, logger types.Logger) (*common.Block, error) {
	h := consensusLedger.Height()
	if h == 0 {
		logger.Infof("Consensus ledger height is 0")
		return nil, nil
	}
	logger.Infof("Consensus ledger height is: %d", consensusLedger.Height())
	lastBlockIdx := consensusLedger.Height() - 1
	lastBlock, err := consensusLedger.RetrieveBlockByNumber(lastBlockIdx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve last block %d from consensus ledger: %s", lastBlockIdx, err)
	}
	proposal, _, err := state.BytesToDecision(lastBlock.Data.Data[0])
	if err != nil {
		return nil, fmt.Errorf("failed to read decision from last block in consensus ledger: %s", err)
	}

	header := &state.Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		return nil, fmt.Errorf("failed to deserialize decision header from last block: %s", err)
	}

	decisionNumOfLastConfigBlock := header.DecisionNumOfLastConfigBlock
	logger.Infof("Decision number of last config block: %d", decisionNumOfLastConfigBlock)
	decisionOfLastConfigBlock, err := consensusLedger.RetrieveBlockByNumber(uint64(decisionNumOfLastConfigBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve decision of the last config block from consensus ledger: %s", err)
	}
	proposal, _, err = state.BytesToDecision(decisionOfLastConfigBlock.Data.Data[0])
	if err != nil {
		return nil, fmt.Errorf("failed to read decision from last config block in consensus ledger: %s", err)
	}
	header = &state.Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		return nil, fmt.Errorf("failed to deserialize decision header from last block: %s", err)
	}
	lastConfigBlock := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
	return lastConfigBlock, nil
}
