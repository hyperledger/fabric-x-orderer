/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

type prefetchIndexTestVars struct {
	shards                                  []types.ShardID
	parties                                 []types.PartyID
	partitionToPartitionPrefetchIndexerMock map[assembler.ShardPrimary]*mocks.FakePartitionPrefetchIndexer
	partitionToRequestsChan                 map[assembler.ShardPrimary]chan types.BatchID
	prefetchIndex                           *assembler.PrefetchIndex
}

func setupPrefetchIndexTest(t *testing.T) *prefetchIndexTestVars {
	shards := []types.ShardID{1, 2, 3, 4}
	parties := []types.PartyID{1, 2, 3, 4}
	vars := &prefetchIndexTestVars{
		shards:                                  shards,
		parties:                                 parties,
		partitionToPartitionPrefetchIndexerMock: make(map[assembler.ShardPrimary]*mocks.FakePartitionPrefetchIndexer),
		partitionToRequestsChan:                 make(map[assembler.ShardPrimary]chan types.BatchID),
	}
	partitionPrefetchIndexerFactory := &mocks.FakePartitionPrefetchIndexerFactory{}
	partitionPrefetchIndexerFactory.CreateCalls(func(sp assembler.ShardPrimary, l types.Logger, d time.Duration, i int, tf assembler.TimerFactory, bcf assembler.BatchCacheFactory, requestsChan chan types.BatchID) assembler.PartitionPrefetchIndexer {
		mock := &mocks.FakePartitionPrefetchIndexer{}
		vars.partitionToPartitionPrefetchIndexerMock[sp] = mock
		vars.partitionToRequestsChan[sp] = requestsChan
		return mock
	})

	prefetchIndex := assembler.NewPrefetchIndex(
		shards,
		parties,
		testutil.CreateLogger(t, 1),
		time.Nanosecond,
		-1,
		1000,
		nil,
		nil,
		partitionPrefetchIndexerFactory,
	)
	vars.prefetchIndex = prefetchIndex
	return vars
}

func (pitv *prefetchIndexTestVars) finish() {
	pitv.prefetchIndex.Stop()
}

func TestPrefetchIndex_Ctor(t *testing.T) {
	t.Run("PartitionPrefetchIndexerWillBeCreatedForEachPartitionOnCtor", func(t *testing.T) {
		// Arrange & Act
		test := setupPrefetchIndexTest(t)
		defer test.finish()

		// Assert
		for _, shardId := range test.shards {
			for _, partyId := range test.parties {
				partition := assembler.ShardPrimary{Shard: shardId, Primary: partyId}
				require.Contains(t, test.partitionToPartitionPrefetchIndexerMock, partition)
			}
		}
	})
}

func TestPrefetchIndex_Stop(t *testing.T) {
	t.Run("PartitionPrefetchIndexerStopWillBeCalledForAll", func(t *testing.T) {
		// Arrange
		test := setupPrefetchIndexTest(t)
		defer test.finish()

		// Act
		test.prefetchIndex.Stop()

		// Assert
		for _, partitionPrefetchIndexerMock := range test.partitionToPartitionPrefetchIndexerMock {
			require.Eventually(t, func() bool {
				return partitionPrefetchIndexerMock.StopCallCount() == 1
			}, eventuallyTimeout, eventuallyTick)
		}
	})
}

func TestPrefetchIndex_Requests(t *testing.T) {
	t.Run("PartitionPrefetchIndexerRequestingBatchWillBeRequestedInRequests", func(t *testing.T) {
		// Arrange
		test := setupPrefetchIndexTest(t)
		defer test.finish()
		batch := createTestBatch(test.shards[0], test.parties[0], 0, []int{1})

		// Act
		test.partitionToRequestsChan[assembler.ShardPrimaryFromBatch(batch)] <- batch

		// Assert
		requestedBatch := test.prefetchIndex.Requests()
		assertBatchIdsEquals(t, batch, <-requestedBatch)
	})
}

// checks that the correct partition indexer was called with the correct batch id parameter
// indexerOp - should do the operation on the indexer using the given batch
// callsCount - should give the number of calls on the partition indexer relevant method
// batchPerCall - should give the batch id that is given to the partition indexer method as a parameter
func checkCorrectPartitionIndexerCalledWithCorrectBatchId(
	t *testing.T,
	indexerOp func(assembler.PrefetchIndexer, types.Batch),
	callsCount func(*mocks.FakePartitionPrefetchIndexer) int,
	batchPerCall func(*mocks.FakePartitionPrefetchIndexer, int) types.BatchID,
) {
	t.Run("CorrectPartitionPrefetchIndexerPartitionWillBeCalled", func(t *testing.T) {
		// Arrange
		test := setupPrefetchIndexTest(t)
		defer test.finish()
		batch1 := createTestBatch(test.shards[0], test.parties[0], 0, []int{1})
		batch2 := createTestBatch(test.shards[1], test.parties[1], 0, []int{1})
		batch1PartitionIndexer := test.partitionToPartitionPrefetchIndexerMock[assembler.ShardPrimaryFromBatch(batch1)]
		batch2PartitionIndexer := test.partitionToPartitionPrefetchIndexerMock[assembler.ShardPrimaryFromBatch(batch2)]

		// Act
		indexerOp(test.prefetchIndex, batch1)
		indexerOp(test.prefetchIndex, batch2)

		// Assert
		require.Equal(t, 1, callsCount(batch1PartitionIndexer))
		assertBatchIdsEquals(t, batch1, batchPerCall(batch1PartitionIndexer, 0))
		require.Equal(t, 1, callsCount(batch2PartitionIndexer))
		assertBatchIdsEquals(t, batch2, batchPerCall(batch2PartitionIndexer, 0))
	})
}

func TestPrefetchIndex_PopOrWait(t *testing.T) {
	checkCorrectPartitionIndexerCalledWithCorrectBatchId(
		t,
		func(pi assembler.PrefetchIndexer, b types.Batch) { pi.PopOrWait(b) },
		func(fppi *mocks.FakePartitionPrefetchIndexer) int {
			return fppi.PopOrWaitCallCount()
		},
		func(fppi *mocks.FakePartitionPrefetchIndexer, i int) types.BatchID {
			return fppi.PopOrWaitArgsForCall(i)
		},
	)

	t.Run("CheckCorrectReturnValueFromPartitionIndexer", func(t *testing.T) {
		// Arrange
		test := setupPrefetchIndexTest(t)
		defer test.finish()
		batch := createTestBatch(test.shards[0], test.parties[0], 0, []int{1})
		batchPartitionIndexer := test.partitionToPartitionPrefetchIndexerMock[assembler.ShardPrimaryFromBatch(batch)]
		batchPartitionIndexer.PopOrWaitReturns(batch, nil)

		// Act
		popedBatch, err := test.prefetchIndex.PopOrWait(batch)

		// Assert
		require.NoError(t, err)
		assertBatchIdsEquals(t, batch, popedBatch)
	})

	t.Run("CheckErrRaisedFromPartitionIndexer", func(t *testing.T) {
		// Arrange
		test := setupPrefetchIndexTest(t)
		defer test.finish()
		batch := createTestBatch(test.shards[0], test.parties[0], 0, []int{1})
		batchPartitionIndexer := test.partitionToPartitionPrefetchIndexerMock[assembler.ShardPrimaryFromBatch(batch)]
		expectedErr := utils.ErrOperationCancelled
		batchPartitionIndexer.PopOrWaitReturns(nil, expectedErr)

		// Act
		_, err := test.prefetchIndex.PopOrWait(batch)

		// Assert
		require.ErrorIs(t, err, expectedErr)
	})
}

// run tests on the Put and PutForce
// putOp - should do the put operation on the indexer using the given batch, retruns the error raised from it
// callsCount - should give the number of calls on the partition indexer relevant method
// batchPerCall - should give the batch id that is given to the partition indexer method as a parameter
// putOpMockReturns - mocks the return value of the put method
func runTestsForPutOps(
	t *testing.T,
	putOp func(assembler.PrefetchIndexer, types.Batch) error,
	callsCount func(*mocks.FakePartitionPrefetchIndexer) int,
	batchPerCall func(*mocks.FakePartitionPrefetchIndexer, int) types.BatchID,
	putMockReturns func(*mocks.FakePartitionPrefetchIndexer, error),
) {
	checkCorrectPartitionIndexerCalledWithCorrectBatchId(
		t,
		func(pi assembler.PrefetchIndexer, b types.Batch) { putOp(pi, b) },
		func(fppi *mocks.FakePartitionPrefetchIndexer) int {
			return callsCount(fppi)
		},
		func(fppi *mocks.FakePartitionPrefetchIndexer, i int) types.BatchID {
			return batchPerCall(fppi, i)
		},
	)

	t.Run("CheckErrRaisedFromPartitionIndexer", func(t *testing.T) {
		// Arrange
		test := setupPrefetchIndexTest(t)
		defer test.finish()
		batch := createTestBatch(test.shards[0], test.parties[0], 0, []int{1})
		batchPartitionIndexer := test.partitionToPartitionPrefetchIndexerMock[assembler.ShardPrimaryFromBatch(batch)]
		expectedErr := utils.ErrOperationCancelled
		putMockReturns(batchPartitionIndexer, expectedErr)

		// Act
		err := putOp(test.prefetchIndex, batch)

		// Assert
		require.ErrorIs(t, err, expectedErr)
	})
}

func TestPrefetchIndex_Put(t *testing.T) {
	runTestsForPutOps(
		t,
		func(pi assembler.PrefetchIndexer, b types.Batch) error { return pi.Put(b) },
		func(fppi *mocks.FakePartitionPrefetchIndexer) int { return fppi.PutCallCount() },
		func(fppi *mocks.FakePartitionPrefetchIndexer, i int) types.BatchID { return fppi.PutArgsForCall(i) },
		func(fppi *mocks.FakePartitionPrefetchIndexer, err error) { fppi.PutReturns(err) },
	)
}

func TestPrefetchIndex_PutForce(t *testing.T) {
	runTestsForPutOps(
		t,
		func(pi assembler.PrefetchIndexer, b types.Batch) error { return pi.PutForce(b) },
		func(fppi *mocks.FakePartitionPrefetchIndexer) int { return fppi.PutForceCallCount() },
		func(fppi *mocks.FakePartitionPrefetchIndexer, i int) types.BatchID {
			return fppi.PutForceArgsForCall(i)
		},
		func(fppi *mocks.FakePartitionPrefetchIndexer, err error) { fppi.PutForceReturns(err) },
	)
}
