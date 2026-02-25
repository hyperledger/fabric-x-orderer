/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"encoding/pem"
	"path/filepath"
	"sort"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/request"
)

func CreateBatcher(config *node_config.BatcherNodeConfig, logger *flogging.FabricLogger, cdrc ConsensusDecisionReplicatorCreator, senderCreator ConsenterControlEventSenderCreator, signer Signer) *Batcher {
	var parties []types.PartyID
	for shIdx, sh := range config.Shards {
		if sh.ShardId != config.ShardId {
			continue
		}

		for _, b := range config.Shards[shIdx].Batchers {
			parties = append(parties, b.PartyID)
		}
		break
	}

	ledgerArray, err := node_ledger.NewBatchLedgerArray(config.ShardId, config.PartyId, parties, config.Directory, logger)
	if err != nil {
		logger.Panicf("Failed creating BatchLedgerArray: %s", err.Error())
	}

	deliverService := &BatcherDeliverService{
		LedgerArray: ledgerArray,
		Logger:      logger,
	}

	batchPuller := NewBatchPuller(config, ledgerArray, logger)

	walDir := filepath.Join(config.Directory, "wal")
	batcherWAL, walInitState, err := wal.InitializeAndReadAll(logger, walDir, wal.DefaultOptions())
	if err != nil {
		logger.Panicf("Failed creating WAL: %s", err.Error())
	}

	configStore, err := configstore.NewStore(config.ConfigStorePath)
	if err != nil {
		logger.Panicf("Failed creating batcher config store: %s", err.Error())
	}

	lastKnownDecisionNum := getLastKnownDecisionNum(walInitState, configStore, logger)

	dr := cdrc.CreateDecisionConsensusReplicator(config, logger, lastKnownDecisionNum)

	requestsIDAndVerifier := NewRequestsInspectorVerifier(logger, config, nil)

	batchers := batchersFromConfig(config)
	if len(batchers) == 0 {
		logger.Panicf("Failed locating the configuration of our shard (%d) among %v", config.ShardId, config.Shards)
	}

	b := &Batcher{
		requestsInspectorVerifier: requestsIDAndVerifier,
		batcherDeliverService:     deliverService,
		decisionReplicator:        dr,
		signer:                    signer,
		logger:                    logger,
		batchers:                  batchers,
		Ledger:                    ledgerArray,
		ConfigStore:               configStore,
		batcherCerts2IDs:          make(map[string]types.PartyID),
		config:                    config,
		metrics:                   NewBatcherMetrics(config, batchers, ledgerArray, logger),
		wal:                       batcherWAL,
	}

	b.controlEventSenders = make([]ConsenterControlEventSender, len(config.Consenters))
	for i, consenterInfo := range config.Consenters {
		b.controlEventSenders[i] = senderCreator.CreateConsenterControlEventSender(config.TLSPrivateKeyFile, config.TLSCertificateFile, consenterInfo)
	}

	initState := computeZeroState(config)

	b.primaryID, b.term = b.getPrimaryIDAndTerm(&initState)

	b.batcherCerts2IDs = indexTLSCerts(b.batchers, b.logger)

	f := (initState.N - 1) / 3

	ctxBroadcast, cancelBroadcast := context.WithCancel(context.Background())
	b.controlEventBroadcaster = NewControlEventBroadcaster(b.controlEventSenders, int(initState.N), int(f), 100*time.Millisecond, 10*time.Second, b.logger, ctxBroadcast, cancelBroadcast)

	b.primaryAckConnector = CreatePrimaryAckConnector(b.primaryID, config.ShardId, logger, config, GetBatchersEndpointsAndCerts(b.batchers), context.Background(), 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)
	b.primaryReqConnector = CreatePrimaryReqConnector(b.primaryID, logger, config, GetBatchersEndpointsAndCerts(b.batchers), context.Background(), 10*time.Second, 100*time.Millisecond, 1*time.Second)

	b.batcher = &BatcherRole{
		Batchers:                GetBatchersIDs(b.batchers),
		BatchPuller:             batchPuller,
		Threshold:               int(f + 1),
		N:                       initState.N,
		BatchTimeout:            config.BatchCreationTimeout,
		Ledger:                  ledgerArray,
		MemPool:                 createMemPool(b, config),
		ID:                      config.PartyId,
		Shard:                   config.ShardId,
		Logger:                  logger,
		StateProvider:           b,
		ConfigSequenceGetter:    b,
		RequestInspector:        b.requestsInspectorVerifier,
		BAFCreator:              b,
		BAFSender:               b,
		BatchAcker:              b,
		Complainer:              b,
		BatchedRequestsVerifier: b.requestsInspectorVerifier,
		BatchSequenceGap:        config.BatchSequenceGap,
		Metrics:                 b.metrics,
	}

	return b
}

func createMemPool(b *Batcher, config *node_config.BatcherNodeConfig) MemPool {
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

	return request.NewPool(b.logger, b.requestsInspectorVerifier, opts, b)
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

	logger.Infof("Checking config store for last known decision number")
	lastConfigBlock, err := configStore.Last()
	if err != nil {
		logger.Panicf("Failed getting last config block from config store: %s", err.Error())
	}
	if lastConfigBlock.GetHeader().GetNumber() == 0 {
		logger.Infof("Returning 0 as last known decision number from config store")
		return 0
	}
	ordererBlockMetadata := lastConfigBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	_, _, _, lastDecisionNumber, _, _, _, err := node_ledger.AssemblerBlockMetadataFromBytes(ordererBlockMetadata)
	if err != nil {
		logger.Panicf("Failed extracting decision number from last config block: %s", err)
	}
	logger.Infof("Returning %d as last known decision number from config store", lastDecisionNumber)
	return lastDecisionNumber
}
