/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"encoding/pem"
	"sort"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/request"
)

func CreateBatcher(config *node_config.BatcherNodeConfig, logger types.Logger, net Net, csrc ConsensusStateReplicatorCreator, senderCreator ConsenterControlEventSenderCreator, signer Signer) *Batcher {
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

	ledger, err := node_ledger.NewBatchLedgerArray(config.ShardId, config.PartyId, parties, config.Directory, logger)
	if err != nil {
		logger.Panicf("Failed creating BatchLedgerArray: %s", err)
	}

	ds := &BatcherDeliverService{
		LedgerArray: ledger,
		Logger:      logger,
	}

	bp := NewBatchPuller(config, ledger, logger)

	sr := csrc.CreateStateConsensusReplicator(config, logger)

	requestsIDAndVerifier := NewRequestsInspectorVerifier(logger, config, nil)

	configStore, err := configstore.NewStore(config.ConfigStorePath)
	if err != nil {
		logger.Panicf("Failed creating batcher config store: %s", err)
	}

	batchers := batchersFromConfig(config)
	if len(batchers) == 0 {
		logger.Panicf("Failed locating the configuration of our shard (%d) among %v", config.ShardId, config.Shards)
	}

	b := &Batcher{
		requestsInspectorVerifier: requestsIDAndVerifier,
		batcherDeliverService:     ds,
		stateReplicator:           sr,
		signer:                    signer,
		logger:                    logger,
		Net:                       net,
		batchers:                  batchers,
		Ledger:                    ledger,
		ConfigStore:               configStore,
		batcherCerts2IDs:          make(map[string]types.PartyID),
		config:                    config,
		Metrics:                   NewBatcherMetrics(config, batchers, ledger, logger),
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
		BatchPuller:             bp,
		Threshold:               int(f + 1),
		N:                       initState.N,
		BatchTimeout:            config.BatchCreationTimeout,
		Ledger:                  ledger,
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
		Metrics:                 b.Metrics,
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

func indexTLSCerts(batchers []node_config.BatcherInfo, logger types.Logger) map[string]types.PartyID {
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
