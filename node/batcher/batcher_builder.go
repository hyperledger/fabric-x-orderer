/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"path/filepath"
	"sort"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/request"
)

func CreateBatcher(nodeConfig *node_config.BatcherNodeConfig, fullConfig *config.Configuration, logger *flogging.FabricLogger, mainExitChan chan struct{}, cdrc ConsensusDecisionReplicatorCreator, senderCreator ConsenterControlEventSenderCreator, signer Signer) *Batcher {
	configStore, err := configstore.NewStore(nodeConfig.ConfigStorePath)
	if err != nil {
		logger.Panicf("Failed creating batcher config store: %s", err.Error())
	}

	walDir := filepath.Join(nodeConfig.Directory, "wal")
	batcherWAL, walInitState, err := wal.InitializeAndReadAll(logger, walDir, wal.DefaultOptions())
	if err != nil {
		logger.Panicf("Failed creating WAL: %s", err.Error())
	}
	lastKnownDecisionNum := getLastKnownDecisionNum(walInitState, configStore, logger)

	b := &Batcher{
		config:                             nodeConfig,
		fullConfig:                         fullConfig,
		consensusDecisionReplicatorCreator: cdrc,
		logger:                             logger,
		signer:                             signer,
		ConfigStore:                        configStore,
		wal:                                batcherWAL,
		mainExitChan:                       mainExitChan,
	}

	b.configureBatcher(senderCreator, nil, lastKnownDecisionNum)
	return b
}

func (b *Batcher) configureBatcher(senderCreator ConsenterControlEventSenderCreator, memPool MemPool, lastKnownDecisionNum types.DecisionNum) {
	var parties []types.PartyID
	for shIdx, sh := range b.config.Shards {
		if sh.ShardId != b.config.ShardId {
			continue
		}

		for _, b := range b.config.Shards[shIdx].Batchers {
			parties = append(parties, b.PartyID)
		}
		break
	}

	ledgerArray, err := node_ledger.NewBatchLedgerArray(b.config.ShardId, b.config.PartyId, parties, b.config.Directory, b.logger)
	if err != nil {
		b.logger.Panicf("Failed creating BatchLedgerArray: %s", err.Error())
	}

	deliverService := &BatcherDeliverService{
		LedgerArray: ledgerArray,
		Logger:      b.logger,
	}

	batchPuller := NewBatchPuller(b.config, ledgerArray, b.logger)

	dr := b.consensusDecisionReplicatorCreator.CreateDecisionConsensusReplicator(b.config, b.logger, lastKnownDecisionNum)

	requestsIDAndVerifier := NewRequestsInspectorVerifier(b.logger, b.config, nil, DefaultRequestID)

	batchers := batchersFromConfig(b.config)
	if len(batchers) == 0 {
		b.logger.Panicf("Failed locating the configuration of our shard (%d) among %v", b.config.ShardId, b.config.Shards)
	}

	b.requestsInspectorVerifier = requestsIDAndVerifier
	b.batcherDeliverService = deliverService
	b.decisionReplicator = dr
	b.batchers = batchers
	b.Ledger = ledgerArray
	b.batcherCerts2IDs = make(map[string]types.PartyID)
	b.metrics = NewBatcherMetrics(b.config, batchers, ledgerArray, b.logger)

	b.controlEventSenders = make([]ConsenterControlEventSender, len(b.config.Consenters))
	for i, consenterInfo := range b.config.Consenters {
		b.controlEventSenders[i] = senderCreator.CreateConsenterControlEventSender(b.config.TLSPrivateKeyFile, b.config.TLSCertificateFile, consenterInfo)
	}

	initState := computeZeroState(b.config)

	b.primaryID, b.term = b.getPrimaryIDAndTerm(&initState)

	b.batcherCerts2IDs = indexTLSCerts(b.batchers, b.logger)

	f := (initState.N - 1) / 3

	ctxBroadcast, cancelBroadcast := context.WithCancel(context.Background())
	b.controlEventBroadcaster = NewControlEventBroadcaster(b.controlEventSenders, int(initState.N), int(f), 100*time.Millisecond, 10*time.Second, b.logger, ctxBroadcast, cancelBroadcast)

	b.primaryAckConnector = CreatePrimaryAckConnector(b.primaryID, b.config.ShardId, b.logger, b.config, GetBatchersEndpointsAndCerts(b.batchers), context.Background(), 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)
	b.primaryReqConnector = CreatePrimaryReqConnector(b.primaryID, b.logger, b.config, GetBatchersEndpointsAndCerts(b.batchers), context.Background(), 10*time.Second, 100*time.Millisecond, 1*time.Second)

	b.batcher = &BatcherRole{
		Batchers:                GetBatchersIDs(b.batchers),
		BatchPuller:             batchPuller,
		Threshold:               int(f + 1),
		N:                       initState.N,
		BatchTimeout:            b.config.BatchCreationTimeout,
		Ledger:                  ledgerArray,
		ID:                      b.config.PartyId,
		Shard:                   b.config.ShardId,
		Logger:                  b.logger,
		StateProvider:           b,
		ConfigSequenceGetter:    b,
		RequestInspector:        b.requestsInspectorVerifier,
		BAFCreator:              b,
		BAFSender:               b,
		BatchAcker:              b,
		Complainer:              b,
		BatchedRequestsVerifier: b.requestsInspectorVerifier,
		BatchSequenceGap:        b.config.BatchSequenceGap,
		Metrics:                 b.metrics,
	}

	if memPool == nil {
		b.batcher.MemPool = createMemPool(b, b.config, DefaultRequestID)
	} else {
		b.batcher.MemPool = memPool
	}
}

func createMemPool(b *Batcher, config *node_config.BatcherNodeConfig, requestIDFunc func(req []byte) string) MemPool {
	opts := request.PoolOptions{
		MaxSize:               config.MemPoolMaxSize,
		BatchMaxSize:          config.BatchMaxSize,
		BatchMaxSizeBytes:     config.BatchMaxBytes,
		RequestMaxBytes:       config.RequestMaxBytes,
		SubmitTimeout:         config.SubmitTimeout,
		FirstStrikeThreshold:  config.FirstStrikeThreshold,
		SecondStrikeThreshold: config.SecondStrikeThreshold,
		AutoRemoveTimeout:     config.AutoRemoveTimeout,
	}

	return request.NewPool(b.logger, requestIDFunc, opts, b)
}

func batchersFromConfig(config *node_config.BatcherNodeConfig) []node_config.BatcherInfo {
	var batchers []node_config.BatcherInfo
	for _, shard := range config.Shards {
		if shard.ShardId == config.ShardId {
			batchers = shard.Batchers
		}
	}

	sort.Slice(batchers, func(i, j int) bool {
		return int(batchers[i].PartyID) < int(batchers[j].PartyID)
	})

	return batchers
}

func computeZeroState(config *node_config.BatcherNodeConfig) state.State {
	var s state.State
	for _, shard := range config.Shards {
		s.Shards = append(s.Shards, state.ShardTerm{
			Shard: shard.ShardId,
		})
	}

	s.N = uint16(len(config.Consenters))

	return s
}

func indexTLSCerts(batchers []node_config.BatcherInfo, logger *flogging.FabricLogger) map[string]types.PartyID {
	batcherCertToID := make(map[string]types.PartyID)
	for _, batcher := range batchers {
		rawTLSCert := batcher.TLSCert
		bl, _ := pem.Decode(rawTLSCert)
		if bl == nil || bl.Bytes == nil {
			logger.Panicf("Failed decoding TLS certificate of batcher %d from PEM", batcher.PartyID)
		}

		batcherCertToID[string(bl.Bytes)] = batcher.PartyID
	}

	return batcherCertToID
}

func getLastKnownDecisionNum(walInitState [][]byte, configStore *configstore.Store, logger *flogging.FabricLogger) types.DecisionNum {
	if len(walInitState) > 0 {
		header := &state.Header{}
		if err := header.Deserialize(walInitState[len(walInitState)-1]); err != nil {
			logger.Panicf("Could not read header from WAL: %s", err.Error())
		}
		logger.Infof("Last known decision number in wal is %d", header.Num)
		if header.Num > 0 {
			return header.Num
		}
	}

	return getLastKnownDecisionNumFromConfigStore(configStore, logger)
}

func getLastKnownDecisionNumFromConfigStore(configStore *configstore.Store, logger *flogging.FabricLogger) types.DecisionNum {
	logger.Infof("Checking config store for last known decision number")
	lastConfigBlock, err := configStore.Last()
	if err != nil {
		logger.Panicf("Failed getting last config block from config store: %s", err.Error())
	}
	if lastConfigBlock.GetHeader().GetNumber() == 0 {
		logger.Infof("Returning 0 as last known decision number from config store")
		return 0
	}

	return getLastKnownDecisionNumFromConfigBlock(lastConfigBlock, logger)
}

func getLastKnownDecisionNumFromConfigBlock(configBlock *common.Block, logger *flogging.FabricLogger) types.DecisionNum {
	ordererBlockMetadata := configBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	_, _, _, lastDecisionNumber, _, _, _, err := node_ledger.AssemblerBlockMetadataFromBytes(ordererBlockMetadata)
	if err != nil {
		logger.Panicf("Failed extracting decision number from last config block: %s", err)
	}
	logger.Infof("Returning %d as last known decision number from config store", lastDecisionNumber)
	return lastDecisionNumber
}

func DefaultRequestID(req []byte) string {
	if len(req) == 0 {
		return ""
	}
	// TODO maybe calculate the request ID differently
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}
