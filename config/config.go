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
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Configuration holds the complete configuration of a database node.
type Configuration struct {
	LocalConfig  *LocalConfig
	SharedConfig *ordererpb.SharedConfig
}

// ReadConfig reads the configurations from the config file and returns it. The configuration includes both local and shared.
func ReadConfig(configFilePath string, logger *flogging.FabricLogger) (*Configuration, *common.Block, error) {
	var err error
	var nodeRole string
	conf := &Configuration{
		LocalConfig:  &LocalConfig{},
		SharedConfig: &ordererpb.SharedConfig{},
	}

	conf.LocalConfig, nodeRole, err = LoadLocalConfig(configFilePath, logger)
	if err != nil {
		return nil, nil, err
	}

	// lastStoredConfigBlock is the last config block found in the node's local storage.
	// It stays nil for a node with empty local storage.
	var lastStoredConfigBlock *common.Block
	// configStore is non-nil only for router/batcher nodes, which persist config blocks in a config store.
	var configStore *configstore.Store

	// If node is router or batcher, check if config store has blocks, and if yes take the last stored block.
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
			lastStoredConfigBlock, err = configStore.GetByNumber(lastBlockNumber)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to retrieve the last block from %s config store: %s", nodeRole, err)
			}
			logger.Infof("last block number %d was retrieved from %s config store", lastBlockNumber, nodeRole)
		}
	}

	// If node is assembler, check if its ledger has blocks, and if yes take the last config block.
	if nodeRole == AssemblerStr {
		assemblerLedgerFactory := &node_ledger.DefaultAssemblerLedgerFactory{}
		assemblerLedger, err := assemblerLedgerFactory.Create(logger, conf.LocalConfig.NodeLocalConfig.FileStore.Path)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create assembler ledger instance: %s", err)
		}
		defer assemblerLedger.Close()
		if assemblerLedger.LedgerReader().Height() > 0 {
			lastStoredConfigBlock, err = node_ledger.GetLastConfigBlockFromAssemblerLedger(assemblerLedger)
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

		lastStoredConfigBlock, err = GetLastConfigBlockFromConsensusLedger(consensusLedger, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get the last config block from consensus ledger instance: %s", err)
		}
	}

	// Read the bootstrap block from the configured bootstrap file. It may be a genesis
	// block (fresh start), or an advanced config block for late join or rejoin.
	bootstrapBlock, err := readBootstrapConfigBlock(conf.LocalConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read bootstrap block from bootstrap file, err: %v", err)
	}

	// Select the more advanced block (by sequence number) between the last block in local storage and the bootstrap block.
	var lastConfigBlock *common.Block
	if lastStoredConfigBlock != nil && lastStoredConfigBlock.Header.Number >= bootstrapBlock.Header.Number {
		lastConfigBlock = lastStoredConfigBlock
	} else {
		lastConfigBlock = bootstrapBlock
		if lastStoredConfigBlock != nil {
			logger.Infof("rejoin-block detected: bootstrap block %d is ahead of last stored config block %d, recovering from the bootstrap block",
				bootstrapBlock.Header.Number, lastStoredConfigBlock.Header.Number)
		}

		// Persist the bootstrap block. This is relevant only for batcher and router nodes.
		if configStore != nil {
			err = configStore.Add(bootstrapBlock)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to add the bootstrap block to the config store: %s", err)
			}
			logger.Infof("Append bootstrap block %d to the %s config store", bootstrapBlock.Header.Number, nodeRole)
		}
	}

	bccsp, err := conf.GetBCCSP()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize BCCSP from config")
	}

	conf.SharedConfig, err = sharedConfigFromBlock(lastConfigBlock, bccsp)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the shared configuration from block, err: %v", err)
	}

	return conf, lastConfigBlock, nil
}

// readBootstrapConfigBlock reads the block pointed to by the bootstrap file in the local config.
// The bootstrap block is not necessarily the genesis block: it can also be a config block for late
// bootstrap, or a config block used to rejoin.
func readBootstrapConfigBlock(conf *LocalConfig) (*common.Block, error) {
	blockPath := conf.NodeLocalConfig.GeneralConfig.Bootstrap.File
	data, err := os.ReadFile(blockPath)
	if err != nil {
		return nil, fmt.Errorf("could not read block %s", blockPath)
	}
	bootstrapBlock, err := protoutil.UnmarshalBlock(data)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling to block: %s", err)
	}
	return bootstrapBlock, nil
}

func sharedConfigFromBlock(block *common.Block, bccsp bccsp.BCCSP) (*ordererpb.SharedConfig, error) {
	if block == nil {
		return nil, errors.New("block is nil")
	}
	consensusMetaData, err := ReadSharedConfigFromBootstrapConfigBlock(block, bccsp)
	if err != nil {
		return nil, err
	}

	sharedConfig := &ordererpb.SharedConfig{}
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
	bccsp, err := config.GetBCCSP()
	if err != nil {
		panic(fmt.Sprintf("error launching router, failed extracting router config: %s", err))
	}

	// use shards to get every party's RootCAs
	shards := config.ExtractShards()
	orderingServiceTrustedRootCAs := node_utils.TLSCAcertsFromShards(shards)
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
		FileStorePath:                       config.LocalConfig.NodeLocalConfig.FileStore.Path,
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
		BCCSP:                               bccsp,
		Operations: &operations.Operations{
			ListenAddress: net.JoinHostPort(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenPort))),
			TLS: operations.TLS{
				Enabled:            config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Enabled,
				Certificate:        config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Certificate,
				PrivateKey:         config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.PrivateKey,
				ClientAuthRequired: config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired,
				ClientRootCAs:      config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs,
			},
		},
		Metrics: &operations.Metrics{
			Provider:           config.LocalConfig.NodeLocalConfig.MetricsConfig.Provider,
			MetricsLogInterval: config.LocalConfig.NodeLocalConfig.MetricsConfig.MetricsLogInterval,
		},
	}
	return routerConfig
}

func (config *Configuration) ExtractBatcherConfig(configBlock *common.Block) *nodeconfig.BatcherNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "keystore", "priv_sk"))
	if err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config: %s", err))
	}

	// use shards to get every party's RootCAs
	shards := config.ExtractShards()
	shardId := config.LocalConfig.NodeLocalConfig.BatcherParams.ShardID
	var orderingServiceTrustedRootCAs [][]byte
	for _, shard := range shards {
		if shard.ShardId != shardId {
			continue
		}
		for _, batchers := range shard.Batchers {
			for _, tlsCA := range batchers.TLSCACerts {
				orderingServiceTrustedRootCAs = append(orderingServiceTrustedRootCAs, tlsCA)
			}
		}
	}

	bundle := config.extractBundleFromConfigBlock(configBlock)
	localConfigClientsTrustedRoots := config.LocalConfig.TLSConfig.ClientRootCAs
	trustedRoots := make([][]byte, 0, len(orderingServiceTrustedRootCAs)+len(localConfigClientsTrustedRoots))
	trustedRoots = append(trustedRoots, orderingServiceTrustedRootCAs...)
	trustedRoots = append(trustedRoots, localConfigClientsTrustedRoots...)

	batcherConfig := &nodeconfig.BatcherNodeConfig{
		Shards:             shards,
		Consenters:         config.ExtractConsenters(),
		Directory:          config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:      net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort))),
		ConfigStorePath:    config.LocalConfig.NodeLocalConfig.FileStore.Path,
		PartyId:            config.LocalConfig.NodeLocalConfig.PartyID,
		ShardId:            shardId,
		TLSPrivateKeyFile:  config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile: config.LocalConfig.TLSConfig.Certificate,
		ClientRootCAs:      trustedRoots,
		SigningPrivateKey:  signingPrivateKey,
		MemPoolMaxSize:     config.LocalConfig.NodeLocalConfig.BatcherParams.MemPoolMaxSize,
		BatchMaxSize:       config.SharedConfig.BatchingConfig.BatchSize.MaxMessageCount,
		BatchMaxBytes:      config.SharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes,
		RequestMaxBytes:    config.SharedConfig.BatchingConfig.RequestMaxBytes,
		SubmitTimeout:      config.LocalConfig.NodeLocalConfig.BatcherParams.SubmitTimeout,
		BatchSequenceGap:   types.BatchSequence(config.LocalConfig.NodeLocalConfig.BatcherParams.BatchSequenceGap),
		Operations: &operations.Operations{
			ListenAddress: net.JoinHostPort(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenPort))),
			TLS: operations.TLS{
				Enabled:            config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Enabled,
				Certificate:        config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Certificate,
				PrivateKey:         config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.PrivateKey,
				ClientAuthRequired: config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired,
				ClientRootCAs:      config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs,
			},
		},
		Metrics: &operations.Metrics{
			Provider:           config.LocalConfig.NodeLocalConfig.MetricsConfig.Provider,
			MetricsLogInterval: config.LocalConfig.NodeLocalConfig.MetricsConfig.MetricsLogInterval,
		},
		ClientSignatureVerificationRequired: config.LocalConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired,
		Bundle:                              bundle,
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

	bccsp, err := config.GetBCCSP()
	if err != nil {
		panic(fmt.Sprintf("error launching consenter, failed extracting consenter config: %s", err))
	}

	// TODO: avoid duplications in clientRootCAs
	shards := config.ExtractShards()
	consenters := config.ExtractConsenters()

	orderingServiceTrustedRootCAs := node_utils.TLSCAcertsFromShards(shards)
	orderingServiceTrustedRootCAs = append(orderingServiceTrustedRootCAs, node_utils.TLSCAcertsFromConsenters(consenters)...)

	bundle := config.extractBundleFromConfigBlock(configBlock)
	localConfigClientsTrustedRoots := config.LocalConfig.TLSConfig.ClientRootCAs
	trustedRoots := make([][]byte, 0, len(orderingServiceTrustedRootCAs)+len(localConfigClientsTrustedRoots))
	trustedRoots = append(trustedRoots, orderingServiceTrustedRootCAs...)
	trustedRoots = append(trustedRoots, localConfigClientsTrustedRoots...)

	consenterConfig := &nodeconfig.ConsenterNodeConfig{
		Shards:             shards,
		Consenters:         consenters,
		Router:             config.ExtractRouterInParty(),
		Directory:          config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:      net.JoinHostPort(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort))),
		PartyId:            config.LocalConfig.NodeLocalConfig.PartyID,
		TLSPrivateKeyFile:  config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile: config.LocalConfig.TLSConfig.Certificate,
		ClientRootCAs:      trustedRoots,
		SigningPrivateKey:  signingPrivateKey,
		WALDir:             config.LocalConfig.NodeLocalConfig.ConsensusParams.WALDir,
		BFTConfig:          BFTConfig,
		Operations: &operations.Operations{
			ListenAddress: net.JoinHostPort(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenPort))),
			TLS: operations.TLS{
				Enabled:            config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Enabled,
				Certificate:        config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Certificate,
				PrivateKey:         config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.PrivateKey,
				ClientAuthRequired: config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired,
				ClientRootCAs:      config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs,
			},
		},
		Metrics: &operations.Metrics{
			Provider:           config.LocalConfig.NodeLocalConfig.MetricsConfig.Provider,
			MetricsLogInterval: config.LocalConfig.NodeLocalConfig.MetricsConfig.MetricsLogInterval,
		},
		ClientSignatureVerificationRequired: config.LocalConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired,
		Bundle:                              bundle,
		BCCSP:                               bccsp,
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

	// use shards to get every party's RootCAs
	shards := config.ExtractShards()
	orderingServiceTrustedRootCAs := node_utils.TLSCAcertsFromShards(shards)
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
		Operations: &operations.Operations{
			ListenAddress: net.JoinHostPort(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenAddress, strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.OperationsConfig.ListenPort))),
			TLS: operations.TLS{
				Enabled:            config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Enabled,
				Certificate:        config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.Certificate,
				PrivateKey:         config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.PrivateKey,
				ClientAuthRequired: config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired,
				ClientRootCAs:      config.LocalConfig.NodeLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs,
			},
		},
		Metrics: &operations.Metrics{
			Provider:           config.LocalConfig.NodeLocalConfig.MetricsConfig.Provider,
			MetricsLogInterval: config.LocalConfig.NodeLocalConfig.MetricsConfig.MetricsLogInterval,
		},
		Bundle: bundle,
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

// ExtractAssemblers extracts the assemblers from the configuration and returns a slice of AssemblerInfo.
func (config *Configuration) ExtractAssemblers() []nodeconfig.AssemblerInfo {
	var assemblers []nodeconfig.AssemblerInfo
	for _, party := range config.SharedConfig.PartiesConfig {
		if party.AssemblerConfig == nil {
			continue
		}
		var tlsCACertsCollection []nodeconfig.RawBytes
		for _, ca := range party.TLSCACerts {
			tlsCACertsCollection = append(tlsCACertsCollection, ca)
		}

		assemblerInfo := nodeconfig.AssemblerInfo{
			PartyID:    types.PartyID(party.PartyID),
			Endpoint:   net.JoinHostPort(party.AssemblerConfig.Host, strconv.Itoa(int(party.AssemblerConfig.Port))),
			TLSCACerts: tlsCACertsCollection,
			TLSCert:    party.AssemblerConfig.TlsCert,
		}
		assemblers = append(assemblers, assemblerInfo)
	}

	return assemblers
}

func (config *Configuration) ExtractRouterInParty() nodeconfig.RouterInfo {
	partyID := config.LocalConfig.NodeLocalConfig.PartyID
	var party *ordererpb.PartyConfig
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
	bccsp, err := config.GetBCCSP()
	if err != nil {
		panic(fmt.Sprintf("failed to extract bundle from config block: %s", err))
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

type ConsensusLedgerReader interface {
	RetrieveBlockByNumber(blockNum uint64) (*common.Block, error)
	Height() uint64
}

// GetLastConfigBlockFromConsensusLedger retrieves the last (fabric) config block from the consensus ledger.
// It returns the last config block by first finding the decision number of the last (decision) config block, retrieves that decision block, and then extracts the fabric config block from it.
func GetLastConfigBlockFromConsensusLedger(consensusLedger ConsensusLedgerReader, logger *flogging.FabricLogger) (*common.Block, error) {
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

	return GetLastConfigBlockUsingBlockFromConsensusLedger(lastBlock, consensusLedger, logger)
}

// GetLastConfigBlockUsingBlockFromConsensusLedger retrieves the last (fabric) config block from the consensus ledger, at or before the given (decision) block.
// It returns the last config block by first finding the decision number of the last (decision) config block, retrieves that decision block, and then extracts the fabric config block from it.
func GetLastConfigBlockUsingBlockFromConsensusLedger(block *common.Block, consensusLedger ConsensusLedgerReader, logger *flogging.FabricLogger) (*common.Block, error) {
	if block == nil {
		return nil, errors.New("block is nil")
	}
	if block.Header == nil {
		return nil, errors.New("block header is missing")
	}
	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("block data is missing")
	}

	proposal, err := state.BytesToProposal(block.Data.Data[0])
	if err != nil {
		return nil, fmt.Errorf("failed to read decision from block in consensus ledger: %s", err)
	}

	header := &state.Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		return nil, fmt.Errorf("failed to deserialize decision header from block: %s", err)
	}
	// TODO get decisionNumOfLastConfigBlock from the metadata of the block instead of the header of the decision
	decisionNumOfLastConfigBlock := header.DecisionNumOfLastConfigBlock
	logger.Infof("Decision number of last config block: %d, in block: %d", decisionNumOfLastConfigBlock, block.Header.Number)
	decisionOfLastConfigBlock, err := consensusLedger.RetrieveBlockByNumber(uint64(decisionNumOfLastConfigBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve decision of the last config block from consensus ledger: %s", err)
	}
	proposal, err = state.BytesToProposal(decisionOfLastConfigBlock.Data.Data[0])
	if err != nil {
		return nil, fmt.Errorf("failed to read proposal from last config block in consensus ledger: %s", err)
	}
	header = &state.Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		return nil, fmt.Errorf("failed to deserialize decision header from last block: %s", err)
	}
	lastConfigBlock := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
	return lastConfigBlock, nil
}

func (config *Configuration) CheckIfRouterNodeExistsInSharedConfig() error {
	localPartyID := uint32(config.LocalConfig.NodeLocalConfig.PartyID)
	localTLSCert := config.LocalConfig.TLSConfig.Certificate

	for _, sharedPartyConfig := range config.SharedConfig.PartiesConfig {
		if localPartyID == sharedPartyConfig.PartyID {
			if sharedPartyConfig.RouterConfig == nil {
				return fmt.Errorf("router configuration of partyID %d is missing from the shared configuration: %+v", localPartyID, sharedPartyConfig)
			}
			equal, err := utils.AreCertificatesEqual(localTLSCert, sharedPartyConfig.RouterConfig.TlsCert)
			if err != nil {
				return fmt.Errorf("error comparing TLS cert of router of party%d to the shared configuration: %s", localPartyID, err)
			}
			if !equal {
				localTLSCertString, err := utils.CertificateBytesToString(localTLSCert)
				if err != nil {
					return fmt.Errorf("error converting local TLS cert of router of party%d to string: %s", localPartyID, err)
				}
				sharedTLSCertString, err := utils.CertificateBytesToString(sharedPartyConfig.RouterConfig.TlsCert)
				if err != nil {
					return fmt.Errorf("error converting shared TLS cert of router of party%d to string: %s", localPartyID, err)
				}
				return fmt.Errorf("certificate mismatch: the router of party %d is attempting to load with TLS certificate: %v that differs from the shared configuration TLS certificate: %v", localPartyID, localTLSCertString, sharedTLSCertString)
			}
			return nil
		}
	}

	return fmt.Errorf("partyID %d is not present in the shared configuration's party list", localPartyID)
}

func (config *Configuration) CheckIfBatcherNodeExistsInSharedConfig(localSignCert []byte) error {
	localConfig := config.LocalConfig.NodeLocalConfig
	localPartyID := uint32(config.LocalConfig.NodeLocalConfig.PartyID)
	localShardID := uint32(localConfig.BatcherParams.ShardID)
	localTLSCert := config.LocalConfig.TLSConfig.Certificate

	var sharedPartyConfig *ordererpb.PartyConfig
	for _, party := range config.SharedConfig.PartiesConfig {
		if localPartyID == party.PartyID {
			sharedPartyConfig = party
			break
		}
	}
	if sharedPartyConfig == nil {
		return fmt.Errorf("partyID %d is not present in the shared configuration's party list", localPartyID)
	}

	var sharedBatcherConfig *ordererpb.BatcherNodeConfig
	for _, batcher := range sharedPartyConfig.BatchersConfig {
		if localShardID == batcher.ShardID {
			sharedBatcherConfig = batcher
			break
		}
	}
	if sharedBatcherConfig == nil {
		return fmt.Errorf("batcher in shard%d does not exist for party%d in the shared config", localShardID, localPartyID)
	}

	equal, err := utils.AreCertificatesEqual(localTLSCert, sharedBatcherConfig.TlsCert)
	if err != nil {
		return fmt.Errorf("error comparing TLS cert of batcher in shard%d of party%d to the shared configuration: %s", localShardID, localPartyID, err)
	}
	if !equal {
		localTLSCertString, err := utils.CertificateBytesToString(localTLSCert)
		if err != nil {
			return fmt.Errorf("error converting local TLS cert of batcher in shard%d of party%d to string: %s", localShardID, localPartyID, err)
		}
		sharedTLSCertString, err := utils.CertificateBytesToString(sharedBatcherConfig.TlsCert)
		if err != nil {
			return fmt.Errorf("error converting shared TLS cert of batcher in shard%d of party%d to string: %s", localShardID, localPartyID, err)
		}
		return fmt.Errorf("certificate mismatch: the batcher of party %d shard %d is attempting to load with TLS certificate: %v that differs from the shared configuration TLS certificate: %v", localPartyID, localShardID, localTLSCertString, sharedTLSCertString)
	}

	equal, err = utils.AreCertificatesEqual(localSignCert, sharedBatcherConfig.SignCert)
	if err != nil {
		return fmt.Errorf("error comparing sign cert of batcher in shard%d of party%d to the shared configuration: %s", localShardID, localPartyID, err)
	}
	if !equal {
		localSignCertString, err := utils.CertificateBytesToString(localSignCert)
		if err != nil {
			return fmt.Errorf("error converting local sign cert of batcher in shard%d of party%d to string: %s", localShardID, localPartyID, err)
		}
		sharedSignCertString, err := utils.CertificateBytesToString(sharedBatcherConfig.SignCert)
		if err != nil {
			return err
		}
		return fmt.Errorf("sign certificate mismatch: Batcher%d shard%d is attempting to load with sign certificate: %v that differs from the shared configuration sign certificate: %v", localPartyID, localShardID, localSignCertString, sharedSignCertString)
	}

	return nil
}

func (config *Configuration) CheckIfConsenterNodeExistsInSharedConfig(localSignCert []byte) error {
	localPartyID := uint32(config.LocalConfig.NodeLocalConfig.PartyID)
	localTLSCert := config.LocalConfig.TLSConfig.Certificate

	for _, sharedPartyConfig := range config.SharedConfig.PartiesConfig {
		if localPartyID != sharedPartyConfig.PartyID {
			continue
		}

		if sharedPartyConfig.ConsenterConfig == nil {
			return fmt.Errorf("consenter configuration of partyID %d is missing from the shared configuration: %+v", localPartyID, sharedPartyConfig)
		}

		equal, err := utils.AreCertificatesEqual(localSignCert, sharedPartyConfig.ConsenterConfig.SignCert)
		if err != nil {
			return fmt.Errorf("error comparing sign cert of consenter of party%d to the shared configuration: %s", localPartyID, err)
		}
		if !equal {
			localSignCertString, err := utils.CertificateBytesToString(localSignCert)
			if err != nil {
				return fmt.Errorf("error converting local sign cert of consenter of party%d to string: %s", localPartyID, err)
			}
			sharedSignCertString, err := utils.CertificateBytesToString(sharedPartyConfig.ConsenterConfig.SignCert)
			if err != nil {
				return fmt.Errorf("error converting shared sign cert of consenter of party%d to string: %s", localPartyID, err)
			}
			return fmt.Errorf("sign certificate mismatch: Consenter%d is attempting to load with sign certificate: %v that differs from the shared configuration sign certificate: %v", localPartyID, localSignCertString, sharedSignCertString)
		}

		equal, err = utils.AreCertificatesEqual(localTLSCert, sharedPartyConfig.ConsenterConfig.TlsCert)
		if err != nil {
			return fmt.Errorf("error comparing TLS cert of consenter of party%d to the shared configuration: %s", localPartyID, err)
		}
		if !equal {
			localTLSCertString, err := utils.CertificateBytesToString(localTLSCert)
			if err != nil {
				return fmt.Errorf("error converting local TLS cert of consenter of party%d to string: %s", localPartyID, err)
			}
			sharedTLSCertString, err := utils.CertificateBytesToString(sharedPartyConfig.ConsenterConfig.TlsCert)
			if err != nil {
				return fmt.Errorf("error converting shared TLS cert of consenter of party%d to string: %s", localPartyID, err)
			}
			return fmt.Errorf("certificate mismatch: the consenter of party %d is attempting to load with TLS certificate: %v that differs from the shared configuration TLS certificate: %v", localPartyID, localTLSCertString, sharedTLSCertString)
		}

		return nil
	}
	return fmt.Errorf("partyID %d is not present in the shared configuration's party list", localPartyID)
}

func (config *Configuration) CheckIfAssemblerNodeExistsInSharedConfig() error {
	localPartyID := uint32(config.LocalConfig.NodeLocalConfig.PartyID)
	localTLSCert := config.LocalConfig.TLSConfig.Certificate

	for _, sharedPartyConfig := range config.SharedConfig.PartiesConfig {
		if localPartyID == sharedPartyConfig.PartyID {
			if sharedPartyConfig.AssemblerConfig == nil {
				return fmt.Errorf("assembler configuration of partyID %d is missing from the shared configuration: %+v", localPartyID, sharedPartyConfig)
			}
			equal, err := utils.AreCertificatesEqual(localTLSCert, sharedPartyConfig.AssemblerConfig.TlsCert)
			if err != nil {
				return fmt.Errorf("error comparing TLS cert of assembler of party%d to the shared configuration: %s", localPartyID, err)
			}
			if !equal {
				localTLSCertString, err := utils.CertificateBytesToString(localTLSCert)
				if err != nil {
					return fmt.Errorf("error converting local TLS cert of assembler of party%d to string: %s", localPartyID, err)
				}
				sharedTLSCertString, err := utils.CertificateBytesToString(sharedPartyConfig.AssemblerConfig.TlsCert)
				if err != nil {
					return fmt.Errorf("error converting shared TLS cert of assembler of party%d to string: %s", localPartyID, err)
				}
				return fmt.Errorf("certificate mismatch: the assembler of party %d is attempting to load with TLS certificate: %v that differs from the shared configuration TLS certificate: %v", localPartyID, localTLSCertString, sharedTLSCertString)
			}
			return nil
		}
	}
	return fmt.Errorf("partyID %d is not present in the shared configuration's party list", localPartyID)
}

// NewUpdatedConfigurationFromBlock builds a new configuration based on current configuration and block
func (config *Configuration) NewUpdatedConfigurationFromBlock(block *common.Block) (*Configuration, error) {
	if config == nil {
		return nil, errors.New("failed applying new config, current configuration is nil")
	}

	bccsp, err := config.GetBCCSP()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize BCCSP from config")
	}

	sharedConfig, err := sharedConfigFromBlock(block, bccsp)
	if err != nil {
		return nil, errors.Wrapf(err, "failed applying new config, failed to read shared configuration from block number %d", block.GetHeader().GetNumber())
	}
	newConfig := &Configuration{
		LocalConfig:  config.LocalConfig,
		SharedConfig: sharedConfig,
	}

	return newConfig, nil
}

func (config *Configuration) GetBCCSP() (bccsp.BCCSP, error) {
	opts := config.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP
	cryptoProvider, err := factory.GetBCCSPFromOpts(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize BCCSP from config")
	}
	return cryptoProvider, nil
}

func extractConfigFromChannelConfig(ordererConfig channelconfig.Orderer) (*Configuration, error) {
	consensusMeta := ordererConfig.ConsensusMetadata()
	sharedConfig := &ordererpb.SharedConfig{}
	if err := proto.Unmarshal(consensusMeta, sharedConfig); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}
	return &Configuration{SharedConfig: sharedConfig}, nil
}

func ExtractConsenterAddresses(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error) {
	conf, err := extractConfigFromChannelConfig(ordererConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract config from channel config")
	}

	cInfo := conf.ExtractConsenters()

	party2Endpoint := make(orderers.Party2Endpoint)

	for _, consenter := range cInfo {
		party2Endpoint[consenter.PartyID] = &orderers.Endpoint{
			Address: consenter.Endpoint,
		}
		for _, cert := range consenter.TLSCACerts {
			party2Endpoint[consenter.PartyID].RootCerts = append(party2Endpoint[consenter.PartyID].RootCerts, cert)
		}
	}

	return party2Endpoint, nil
}

// ExtractAssemblerAddresses extracts the assembler addresses from the orderer configuration and returns a mapping of party IDs to their corresponding endpoints.
func ExtractAssemblerAddresses(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error) {
	conf, err := extractConfigFromChannelConfig(ordererConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract config from channel config")
	}

	aInfo := conf.ExtractAssemblers()

	party2Endpoint := make(orderers.Party2Endpoint)

	for _, assembler := range aInfo {
		party2Endpoint[assembler.PartyID] = &orderers.Endpoint{
			Address: assembler.Endpoint,
		}
		for _, cert := range assembler.TLSCACerts {
			party2Endpoint[assembler.PartyID].RootCerts = append(party2Endpoint[assembler.PartyID].RootCerts, cert)
		}
	}

	return party2Endpoint, nil
}
