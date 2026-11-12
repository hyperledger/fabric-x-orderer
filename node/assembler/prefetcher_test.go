/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	assembler_mocks "github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

type prefetcherTestVars struct {
	shards                 []types.ShardID
	parties                []types.PartyID
	batchRequestChan       chan types.BatchID
	shardToReplicationChan map[types.ShardID]chan types.Batch
	prefetcher             *assembler.Prefetcher
	prefetchIndexMock      *assembler_mocks.FakePrefetchIndexer
	batchFetcherMock       *assembler_mocks.FakeBatchBringer
}

func setupPrefetcherTest(t *testing.T, shards []types.ShardID, parties []types.PartyID) *prefetcherTestVars {
	vars := &prefetcherTestVars{
		shards:                 shards,
		parties:                parties,
		batchRequestChan:       make(chan types.BatchID, 10),
		shardToReplicationChan: make(map[types.ShardID]chan types.Batch, len(shards)),
		prefetchIndexMock:      &assembler_mocks.FakePrefetchIndexer{},
		batchFetcherMock:       &assembler_mocks.FakeBatchBringer{},
	}
	for _, shardId := range shards {
		vars.shardToReplicationChan[shardId] = make(chan types.Batch, 10)
	}
	vars.prefetchIndexMock.RequestsReturns(vars.batchRequestChan)
	vars.batchFetcherMock.ReplicateCalls(func(shard types.ShardID) <-chan types.Batch {
		return vars.shardToReplicationChan[shard]
	})

	cfg := &config.AssemblerNodeConfig{
		PartyId: 1,
		Metrics: &operations.Metrics{
			Provider:           "disabled",
			MetricsLogInterval: time.Second,
		},
	}
	logger := testutil.CreateLogger(t, 1)

	metrics := assembler.NewMetrics(cfg, &node_ledger.AssemblerLedgerMetrics{}, logger)

	vars.prefetcher = assembler.NewPrefetcher(
		vars.shards,
		vars.parties,
		vars.prefetchIndexMock,
		vars.batchFetcherMock,
		metrics,
		logger,
	)
	return vars
}

func (ptv *prefetcherTestVars) finish() {
	ptv.prefetcher.Stop()
}

func TestPrefetcher_BatchesReceivedByReplicationAreBeingIndexed(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupPrefetcherTest(t, shards, parties)
	defer test.finish()
	shardToReplicatedChan := map[types.ShardID]chan types.Batch{
		shards[0]: make(chan types.Batch),
		shards[1]: make(chan types.Batch),
	}
	shardToBatch := map[types.ShardID]types.Batch{
		shards[0]: createTestBatch(shards[0], parties[0], 0, nil),
		shards[1]: createTestBatch(shards[1], parties[2], 0, nil),
	}
	test.batchFetcherMock.ReplicateCalls(func(shard types.ShardID) <-chan types.Batch {
		return shardToReplicatedChan[shard]
	})

	// Act
	test.prefetcher.Start()
	shardToReplicatedChan[shards[0]] <- shardToBatch[shards[0]]
	shardToReplicatedChan[shards[1]] <- shardToBatch[shards[1]]

	// Assert
	// Index Put called 2 times
	require.Eventually(t, func() bool {
		return test.prefetchIndexMock.PutCallCount() == 2
	}, eventuallyTimeout, eventuallyTick)
	for i := 0; i < test.prefetchIndexMock.PutCallCount(); i++ {
		batch := test.prefetchIndexMock.PutArgsForCall(0)
		require.True(t, types.BatchIDEqual(batch, shardToBatch[batch.Shard()]))
	}
}

func TestPrefetcher_RequestedBatchWillBeFetchedByFetcherOnce(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1}
	parties := []types.PartyID{1, 2, 3}
	test := setupPrefetcherTest(t, shards, parties)
	defer test.finish()
	batch := createTestBatch(test.shards[0], test.parties[0], 10, nil)
	test.batchFetcherMock.GetBatchReturns(batch, nil)

	// Act
	test.prefetcher.Start()
	test.batchRequestChan <- batch

	// Assert
	require.Never(t, func() bool {
		return test.batchFetcherMock.GetBatchCallCount() > 1
	}, eventuallyTimeout, eventuallyTick)
}

func TestPrefetcher_RequestedBatchWillBeFetchedByFetcherAndForcePutToIndex(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1}
	parties := []types.PartyID{1, 2, 3}
	test := setupPrefetcherTest(t, shards, parties)
	defer test.finish()
	batch := createTestBatch(test.shards[0], test.parties[0], 10, nil)
	test.batchFetcherMock.GetBatchReturns(batch, nil)

	// Act
	test.prefetcher.Start()
	test.batchRequestChan <- batch

	// Assert
	require.Eventually(t, func() bool {
		return test.batchFetcherMock.GetBatchCallCount() == 1 && test.prefetchIndexMock.PutForceCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)
	requestedBatchId := test.batchFetcherMock.GetBatchArgsForCall(0)
	require.True(t, types.BatchIDEqual(batch, requestedBatchId))
	putBatch := test.prefetchIndexMock.PutForceArgsForCall(0)
	require.True(t, types.BatchIDEqual(batch, putBatch))
}

func TestPrefetcher_StopWillStopBatchFetcher(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1}
	parties := []types.PartyID{1, 2, 3}
	test := setupPrefetcherTest(t, shards, parties)

	// Act
	test.prefetcher.Start()
	test.finish()

	// Assert
	require.Eventually(t, func() bool {
		return test.batchFetcherMock.StopCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)
}

// Scenario:
//  1. Start a prefetcher whose index Put waits until the index is stopped, as Put does on a full partition.
//  2. Stream one batch, so that Put waits.
//  3. Stop the prefetcher.
//  4. Expect Stop to return.
func TestPrefetcher_StopWakesAPutWaitingForRoom(t *testing.T) {
	// Arrange
	test := setupPrefetcherTest(t, []types.ShardID{1}, []types.PartyID{1})
	indexStopped := make(chan struct{})
	var stopIndexOnce sync.Once
	test.prefetchIndexMock.StopCalls(func() { stopIndexOnce.Do(func() { close(indexStopped) }) })
	test.prefetchIndexMock.PutCalls(func(types.Batch) error {
		<-indexStopped
		return utils.ErrOperationCancelled
	})
	test.prefetcher.Start()
	test.shardToReplicationChan[1] <- createTestBatch(1, 1, 0, nil)
	require.Eventually(t, func() bool {
		return test.prefetchIndexMock.PutCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)

	// Act
	stopped := make(chan struct{})
	go func() {
		test.prefetcher.Stop()
		close(stopped)
	}()

	// Assert
	require.Eventually(t, func() bool {
		select {
		case <-stopped:
			return true
		default:
			return false
		}
	}, eventuallyTimeout, eventuallyTick)
}

// Scenario:
//  1. Start a prefetcher whose GetBatch waits until the batch fetcher is stopped, then takes a moment to return.
//  2. Request a batch, so that a GetBatch is in progress.
//  3. Stop the prefetcher.
//  4. Expect that GetBatch has returned by the time Stop returns.
func TestPrefetcher_StopWaitsForTheBatchFetchesInProgress(t *testing.T) {
	// Arrange
	test := setupPrefetcherTest(t, []types.ShardID{1}, []types.PartyID{1})
	fetcherStopped := make(chan struct{})
	var stopFetcherOnce sync.Once
	test.batchFetcherMock.StopCalls(func() { stopFetcherOnce.Do(func() { close(fetcherStopped) }) })
	var fetchReturned atomic.Bool
	test.batchFetcherMock.GetBatchCalls(func(types.BatchID) (types.Batch, error) {
		<-fetcherStopped
		time.Sleep(eventuallyTick)
		fetchReturned.Store(true)
		return nil, context.Canceled
	})
	test.prefetcher.Start()
	test.batchRequestChan <- createTestBatchId(1, 1, 0, nil)
	require.Eventually(t, func() bool {
		return test.batchFetcherMock.GetBatchCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)

	// Act
	test.prefetcher.Stop()

	// Assert
	require.True(t, fetchReturned.Load())
	require.Zero(t, test.prefetchIndexMock.PutForceCallCount())
}
