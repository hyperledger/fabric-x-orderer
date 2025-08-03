/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package prefetch_benchmark_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type benchTestParams struct {
	testDuration               time.Duration
	shards                     []types.ShardID
	parties                    []types.PartyID
	batchesPerSecond           int
	maxBatchesPerDecision      int
	decisionsPerSecond         float64
	txInBatch                  int
	txSizeBytes                int
	primaryChangeProb          float64
	sameShardPartySeqProb      float64
	indexDefaultTtl            time.Duration
	statsInterval              time.Duration
	indexMaxPartitionSizeBytes int
	requestChannelSize         int
}

type prefetcherBenchmarkSetup struct {
	wg sync.WaitGroup
	// prob of primary change
	primaryChangeProb float64
	// prob of generation of 2 batches with same SPS but with 2 different digests
	sameShardPartySeqProb  float64
	batchesPerSecond       int
	partitionToLock        map[assembler.ShardPrimary]*sync.Mutex
	shardToBatcherStub     map[types.ShardID]*batcherStub
	batcherToConsensusChan chan types.BatchID
	decisionsChan          <-chan []types.BatchID
	decisionsPerSecond     float64
	logger                 types.Logger
	logLevel               zapcore.Level
	indexDefaultTtl        time.Duration
	maxBatchesPerDecision  int
	statsMonitor           *statsMonitor
	cancellationContext    context.Context
	cancelContextFunc      context.CancelFunc
	prefetchIndex          *assembler.PrefetchIndex
	prefetcher             *assembler.Prefetcher
	batchFetcherMock       *mocks.FakeBatchBringer
}

func createPrefetcherBenchmarkSetup(
	t *testing.T,
	logLevel zapcore.Level,
	params *benchTestParams,
) *prefetcherBenchmarkSetup {
	logger := testutil.CreateLoggerForModule(t, "Assembler", logLevel)
	ctx, cancel := context.WithCancel(context.Background())
	batcherToConsensusChan := make(chan types.BatchID, PREFETCH_STRESS_TEST_MEMORY_FACTOR*params.batchesPerSecond)
	test := &prefetcherBenchmarkSetup{
		primaryChangeProb:      params.primaryChangeProb,
		sameShardPartySeqProb:  params.sameShardPartySeqProb,
		batchesPerSecond:       params.batchesPerSecond,
		shardToBatcherStub:     make(map[types.ShardID]*batcherStub),
		batcherToConsensusChan: batcherToConsensusChan,
		decisionsPerSecond:     params.decisionsPerSecond,
		logger:                 logger,
		logLevel:               logLevel,
		indexDefaultTtl:        params.indexDefaultTtl,
		maxBatchesPerDecision:  params.maxBatchesPerDecision,
		statsMonitor:           newStatsMonitor(testutil.CreateLoggerForModule(t, "Stats Monitor", logLevel), PREFETCH_STRESS_TEST_MEMORY_FACTOR*(4*params.batchesPerSecond+int(2*params.decisionsPerSecond)), params.statsInterval),
		cancellationContext:    ctx,
		cancelContextFunc:      cancel,
		partitionToLock:        make(map[assembler.ShardPrimary]*sync.Mutex),
		batchFetcherMock:       &mocks.FakeBatchBringer{},
	}
	for _, shardId := range params.shards {
		test.shardToBatcherStub[shardId] = newBatcherStub(
			t,
			logLevel,
			shardId,
			params.parties,
			batcherToConsensusChan,
			params.primaryChangeProb,
			params.sameShardPartySeqProb,
			params.batchesPerSecond,
			params.txSizeBytes,
			params.txInBatch,
			test.statsMonitor,
		)
	}

	test.batchFetcherMock.ReplicateCalls(func(shardId types.ShardID) <-chan core.Batch {
		return test.shardToBatcherStub[shardId].DeliveryChan()
	})

	test.batchFetcherMock.GetBatchCalls(func(batchId types.BatchID) (core.Batch, error) {
		test.statsMonitor.EventsChan <- &PrefetchBenchEvent{Event: EventPrefetchIndexMissed, Data: batchId}
		return test.shardToBatcherStub[batchId.Shard()].GetBatch(batchId)
	})

	timerFactoryMock := &mocks.FakeTimerFactory{}
	timerFactoryMock.CreateCalls(func(duration time.Duration, action func()) assembler.StoppableTimer {
		return time.AfterFunc(duration, action)
	})

	prefetchIndex := assembler.NewPrefetchIndex(
		params.shards,
		params.parties,
		testutil.CreateLoggerForModule(t, "Assembler Prefetch Index", logLevel),
		params.indexDefaultTtl,
		params.indexMaxPartitionSizeBytes,
		params.requestChannelSize,
		timerFactoryMock,
		&assembler.DefaultBatchCacheFactory{},
		&assembler.DefaultPartitionPrefetchIndexerFactory{},
	)
	test.prefetchIndex = prefetchIndex
	test.prefetcher = assembler.NewPrefetcher(
		params.shards,
		params.parties,
		prefetchIndex,
		test.batchFetcherMock,
		testutil.CreateLoggerForModule(t, "Assembler Prefetcher", logLevel),
	)
	return test
}

func (pst *prefetcherBenchmarkSetup) Start(t *testing.T) {
	pst.statsMonitor.Start()
	for _, stub := range pst.shardToBatcherStub {
		stub.Start()
	}
	pst.prefetcher.Start()
	pst.decisionsChan = generateDecisions(testutil.CreateLoggerForModule(t, "Desicions Gen", pst.logLevel), pst.batcherToConsensusChan, pst.decisionsPerSecond, pst.maxBatchesPerDecision, pst.statsMonitor)
	pst.wg.Add(1)
	go func() {
		pst.logger.Debugf("Starting goroutine of benchmark test")
		defer func() {
			pst.logger.Debugf("Exiting goroutine of benchmark test")
			pst.wg.Done()
		}()
		for decision := range pst.decisionsChan {
			pst.logger.Debugf("Digested a decision of with %d batches", len(decision))
			pst.statsMonitor.EventsChan <- &PrefetchBenchEvent{Event: EventDecisionDigested, Data: decision}
			for _, batchId := range decision {
				_, err := pst.prefetchIndex.PopOrWait(batchId)
				require.NoError(t, err)
				pst.logger.Debugf("Digested %s", assembler.BatchToString(batchId))
				pst.statsMonitor.EventsChan <- &PrefetchBenchEvent{Event: EventBatchDigested, Data: batchId}
			}
		}
	}()
}

func (pst *prefetcherBenchmarkSetup) StopBatchers() {
	for _, stub := range pst.shardToBatcherStub {
		stub.Stop()
	}
	close(pst.batcherToConsensusChan)
}

func (pst *prefetcherBenchmarkSetup) Finish() {
	pst.cancelContextFunc()
	pst.wg.Wait()
	pst.prefetcher.Stop()
	pst.prefetchIndex.Stop()
	pst.statsMonitor.Stop()
}

func runPrefetchMechanismBenchmark(t *testing.T, params *benchTestParams) {
	// Arrange
	logLevel := zap.InfoLevel
	logger := testutil.CreateLoggerForModule(t, "Main Test", logLevel)
	test := createPrefetcherBenchmarkSetup(
		t,
		logLevel,
		params,
	)

	// Act
	test.Start(t)
	<-time.After(params.testDuration)
	test.StopBatchers()

	// Assert
	require.Eventually(t, func() bool {
		generatedBatches := test.statsMonitor.GetAccumulatedEventCount(EventBatchGenerated)
		digestedBatches := test.statsMonitor.GetAccumulatedEventCount(EventBatchDigested)
		generatedDecisions := test.statsMonitor.GetAccumulatedEventCount(EventDecisionDigested)
		digestedDecisions := test.statsMonitor.GetAccumulatedEventCount(EventDecisionDigested)
		logger.Debugf("Checking counters: generatedBatches %d digestedBatches %d generatedDecisions %d digestedDecisions %d", generatedBatches, digestedBatches, generatedDecisions, digestedDecisions)
		return generatedBatches == digestedBatches && generatedDecisions == digestedDecisions
	}, 3*params.testDuration, 100*time.Millisecond)
	test.Finish()
	digestedBatches := test.statsMonitor.GetAccumulatedEventCount(EventBatchDigested)
	expectedGeneratedBatches := int(0.8 * float32(len(params.shards)*params.batchesPerSecond))
	if digestedBatches < expectedGeneratedBatches {
		test.logger.Warnf("At least 80%% of the required load should have been tested, tested load %d expected %d", digestedBatches, expectedGeneratedBatches)
	}
}

func TestPrefetchMechanismBenchmark(t *testing.T) {
	t.Run("BatchesGenRateIsGreaterThanDecisionGenRate ", func(t *testing.T) {
		batchesPerSecond := 100
		params := &benchTestParams{
			testDuration:               3 * time.Second,
			shards:                     []types.ShardID{1, 2},
			parties:                    []types.PartyID{1, 2},
			batchesPerSecond:           batchesPerSecond,
			maxBatchesPerDecision:      25,
			decisionsPerSecond:         5.0,
			txInBatch:                  1000,
			txSizeBytes:                256,
			primaryChangeProb:          1.0 / (2 * float64(batchesPerSecond)),
			sameShardPartySeqProb:      1.0 / (5 * float64(batchesPerSecond)),
			indexDefaultTtl:            time.Hour,
			statsInterval:              time.Second,
			indexMaxPartitionSizeBytes: 1 * 1024 * 1024 * 1024, // 1GB
			requestChannelSize:         1000,
		}
		runPrefetchMechanismBenchmark(t, params)
	})

	t.Run("BatchesGenRateIsSameAsDecisionGenRate", func(t *testing.T) {
		batchesPerSecond := 100
		params := &benchTestParams{
			testDuration:               3 * time.Second,
			shards:                     []types.ShardID{1, 2},
			parties:                    []types.PartyID{1, 2},
			batchesPerSecond:           batchesPerSecond,
			maxBatchesPerDecision:      25,
			decisionsPerSecond:         8.0,
			txInBatch:                  1000,
			txSizeBytes:                256,
			primaryChangeProb:          1.0 / (2 * float64(batchesPerSecond)),
			sameShardPartySeqProb:      1.0 / (5 * float64(batchesPerSecond)),
			indexDefaultTtl:            time.Hour,
			statsInterval:              time.Second,
			indexMaxPartitionSizeBytes: 1 * 1024 * 1024 * 1024, // 1GB
			requestChannelSize:         1000,
		}
		runPrefetchMechanismBenchmark(t, params)
	})

	t.Run("BatchesGenRateIsLowerThanDecisionGenRate", func(t *testing.T) {
		batchesPerSecond := 100
		params := &benchTestParams{
			testDuration:               3 * time.Second,
			shards:                     []types.ShardID{1, 2},
			parties:                    []types.PartyID{1, 2},
			batchesPerSecond:           batchesPerSecond,
			maxBatchesPerDecision:      100,
			decisionsPerSecond:         4.0,
			txInBatch:                  1000,
			txSizeBytes:                256,
			primaryChangeProb:          1.0 / (2 * float64(batchesPerSecond)),
			sameShardPartySeqProb:      1.0 / (5 * float64(batchesPerSecond)),
			indexDefaultTtl:            time.Hour,
			statsInterval:              time.Second,
			indexMaxPartitionSizeBytes: 1 * 1024 * 1024 * 1024, // 1GB
			requestChannelSize:         1000,
		}
		runPrefetchMechanismBenchmark(t, params)
	})
}
