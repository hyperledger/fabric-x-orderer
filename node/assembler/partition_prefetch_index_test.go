/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
)

const (
	eventuallyTimeout = time.Second
	eventuallyTick    = 100 * time.Millisecond
)

type timerMock struct {
	stopCallCount int
	duration      time.Duration
	action        func()
}

func (t *timerMock) Stop() bool {
	t.stopCallCount++
	return true
}

type partitionPrefetchIndexTestVars struct {
	partition              assembler.ShardPrimary
	maxSizeBytes           int
	defaultTtl             time.Duration
	timers                 []*timerMock
	batchCache             *assembler.BatchCache
	forcePutCache          *assembler.BatchCache
	batchRequestChan       chan types.BatchID
	partitionPrefetchIndex *assembler.PartitionPrefetchIndex
}

func setupPartitionPrefetchIndexTest(t *testing.T, maxSizeBytes int) *partitionPrefetchIndexTestVars {
	defaultTtl := time.Hour
	vars := &partitionPrefetchIndexTestVars{
		partition:        assembler.ShardPrimary{Shard: 1, Primary: 1},
		maxSizeBytes:     maxSizeBytes,
		defaultTtl:       defaultTtl,
		timers:           []*timerMock{},
		batchRequestChan: make(chan types.BatchID, 100),
	}
	cacheFactory := &mocks.FakeBatchCacheFactory{}
	cacheFactory.CreateWithTagCalls(func(partition assembler.ShardPrimary, tag string) *assembler.BatchCache {
		var cache **assembler.BatchCache
		if tag == assembler.BATCH_CACHE_TAG {
			cache = &vars.batchCache
		} else if tag == assembler.FORCE_PUT_CACHE_TAG {
			cache = &vars.forcePutCache
		} else {
			require.FailNowf(t, "Unrecognized tag %s", tag)
		}
		if *cache != nil {
			require.FailNowf(t, "Trying to create multiple batch caches for a single partition index with tag %s", tag)
		}
		*cache = assembler.NewBatchCache(partition, tag)
		return *cache
	})

	timerFactoryMock := &mocks.FakeTimerFactory{}
	timerFactoryMock.CreateCalls(func(duration time.Duration, action func()) assembler.StoppableTimer {
		timer := &timerMock{duration: duration, action: action}
		vars.timers = append(vars.timers, timer)
		return timer
	})
	partitionPrefetchIndex := assembler.NewPartitionPrefetchIndex(
		vars.partition,
		testutil.CreateLogger(t, 1),
		defaultTtl,
		maxSizeBytes,
		timerFactoryMock,
		cacheFactory,
		vars.batchRequestChan,
	)
	vars.partitionPrefetchIndex = partitionPrefetchIndex
	return vars
}

func (pitv *partitionPrefetchIndexTestVars) finish() {
	pitv.partitionPrefetchIndex.Stop()
}

// These tests can match both Put and PutForce
func putCommonTests(t *testing.T, put func(*assembler.PartitionPrefetchIndex, core.Batch) error) {
	t.Run("PuttingSameBatchTwiceShouldResultInError", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batch := testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, 0, []int{1})

		// Act
		require.NoError(t, put(test.partitionPrefetchIndex, batch))
		err := put(test.partitionPrefetchIndex, batch)

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchAlreadyExists)
	})

	t.Run("PutAndThenPopShouldPopTheBatch", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batch := testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, 0, []int{1})

		// Act
		require.NoError(t, put(test.partitionPrefetchIndex, batch))
		poped_batch, err := test.partitionPrefetchIndex.PopOrWait(batch)
		require.NoError(t, err)

		// Assert
		testutil.AssertBatchIdsEquals(t, batch, poped_batch)
	})

	t.Run("PuttingBatchTooLargeWillResultInError", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batch := testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, 0, []int{test.maxSizeBytes + 1})

		// Act
		err := put(test.partitionPrefetchIndex, batch)

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchTooLarge)
	})

	t.Run("PuttingBatchesWithSameSeqAndDIfferentDigestIsPossible", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batch1 := testutil.CreateEmptyMockBatch(test.partition.Shard, test.partition.Primary, 0, []byte{1})
		batch2 := testutil.CreateEmptyMockBatch(test.partition.Shard, test.partition.Primary, 0, []byte{2})

		// Act & Assert
		require.NoError(t, put(test.partitionPrefetchIndex, batch1))
		require.NoError(t, put(test.partitionPrefetchIndex, batch2))
	})
}

func TestPartitionPrefetchIndex_AutoEviction(t *testing.T) {
	t.Run("PutItemWhichWasNotPopedWillBeEvictedAfterTtl", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batch := testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, 0, []int{1})
		test.partitionPrefetchIndex.Put(batch)

		// Act
		test.timers[0].action()

		// Assert
		require.False(t, test.batchCache.Has(batch))
		require.Zero(t, test.batchCache.SizeBytes())
	})
}

func TestPartitionPrefetchIndex_PopOrWait(t *testing.T) {
	t.Run("PopWillReturnTheCorrectBatch", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		shardId := test.partition.Shard
		batch := testutil.CreateMockBatch(shardId, test.partition.Primary, 0, []int{1})
		require.NoError(t, test.partitionPrefetchIndex.Put(batch))

		// Act
		poppedBatch, err := test.partitionPrefetchIndex.PopOrWait(batch)

		// Assert
		require.NoError(t, err)
		testutil.AssertBatchIdsEquals(t, batch, poppedBatch)
	})

	t.Run("PopWillStopBatchTtlTimer", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		shardId := test.partition.Shard
		batch := testutil.CreateMockBatch(shardId, test.partition.Primary, 0, []int{1})
		require.NoError(t, test.partitionPrefetchIndex.Put(batch))

		// Act
		test.partitionPrefetchIndex.PopOrWait(batch)

		// Assert
		require.Len(t, test.timers, 1)
		require.Equal(t, test.timers[0].stopCallCount, 1)
	})

	t.Run("PopWillRemoveBatchFromCache", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		shardId := test.partition.Shard
		batch := testutil.CreateMockBatch(shardId, test.partition.Primary, 0, []int{1})
		require.NoError(t, test.partitionPrefetchIndex.Put(batch))

		// Act
		test.partitionPrefetchIndex.PopOrWait(batch)

		// Assert
		require.Zero(t, test.batchCache.SizeBytes())
	})

	t.Run("PopBeforePutShouldRaiseRequestIfSeqLessThanLastPut", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		putBatch := testutil.CreateEmptyMockBatch(test.partition.Shard, test.partition.Primary, 9, nil)
		popBatchId := testutil.CreateMockBatchId(test.partition.Shard, test.partition.Primary, 6, nil)

		// Act
		require.NoError(t, test.partitionPrefetchIndex.Put(putBatch))
		go func() {
			_, err := test.partitionPrefetchIndex.PopOrWait(popBatchId)
			if !errors.Is(err, utils.ErrOperationCancelled) {
				t.Error("PopOrWait should have waited")
			}
		}()
		batchRequest := <-test.batchRequestChan

		// Assert
		testutil.AssertBatchIdsEquals(t, batchRequest, popBatchId)
	})

	t.Run("PopBeforePutShouldNotRaiseRequestIfSeqGreaterThanLastPut", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		putBatch := testutil.CreateEmptyMockBatch(test.partition.Shard, test.partition.Primary, 6, nil)
		popBatchId := testutil.CreateMockBatchId(test.partition.Shard, test.partition.Primary, 9, nil)

		// Act
		require.NoError(t, test.partitionPrefetchIndex.Put(putBatch))
		go func() {
			_, err := test.partitionPrefetchIndex.PopOrWait(popBatchId)
			if !errors.Is(err, utils.ErrOperationCancelled) {
				t.Error("PopOrWait should have waited")
			}
		}()

		// Assert
		time.Sleep(eventuallyTimeout)
		select {
		case <-test.batchRequestChan:
			t.Error("PopOrWait should not make a request")
		default:
			return
		}
	})

	t.Run("PopingBatchFarInTheFutureWillCausePutAndEvictOfPreviousBatches", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batches := []core.Batch{}
		for i := 1; i <= 10; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}
		popedBatchChan := make(chan core.Batch)

		// Act
		go func() {
			poppedBatch, err := test.partitionPrefetchIndex.PopOrWait(batches[9])
			require.NoError(t, err)
			popedBatchChan <- poppedBatch
		}()
		go func() {
			for _, batch := range batches {
				test.partitionPrefetchIndex.Put(batch)
			}
		}()

		// Assert
		time.Sleep(eventuallyTick)
		batch := <-popedBatchChan
		testutil.AssertBatchIdsEquals(t, batches[9], batch)
		require.True(t, test.batchCache.Has(batches[7]))
		require.True(t, test.batchCache.Has(batches[8]))
		require.Equal(t, 2, test.batchCache.SizeBytes())
	})

	t.Run("PuttingBatchWithSeqAndDigestXXXAndPoppingSeqAndDigestYYYWillRaiseARequest", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batch1 := testutil.CreateEmptyMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(1), []byte{1})
		batch2 := testutil.CreateEmptyMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(1), []byte{2})
		test.partitionPrefetchIndex.Put(batch1)

		// Act
		go func() {
			test.partitionPrefetchIndex.PopOrWait(batch2)
		}()

		// Assert
		time.Sleep(eventuallyTick)
		reqeustedBatchId := <-test.batchRequestChan
		testutil.AssertBatchIdsEquals(t, batch2, reqeustedBatchId)
	})
}

func TestPartitionPrefetchIndex_Put(t *testing.T) {
	putCommonTests(t, func(pi *assembler.PartitionPrefetchIndex, b core.Batch) error {
		return pi.Put(b)
	})

	t.Run("PuttingBatchAfterMaxMemoryForBatchWithSeqLessThanLastPopedWillWait", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		batches := []core.Batch{}
		for i := 2; i <= 6; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}
		// put seq 3
		require.NoError(t, test.partitionPrefetchIndex.Put(batches[0]))
		// pop seq 3
		test.partitionPrefetchIndex.PopOrWait(batches[0])
		// put seq 3-5
		for _, batch := range batches[1:4] {
			test.partitionPrefetchIndex.Put(batch)
		}
		var wg sync.WaitGroup
		wg.Add(1)

		// Act & Assert
		go func() {
			defer wg.Done()
			// put seq 6
			err := test.partitionPrefetchIndex.Put(batches[4])
			if !errors.Is(err, utils.ErrOperationCancelled) {
				t.Error("PopOrWait should have waited")
			}
		}()
		time.Sleep(eventuallyTick)
		test.finish()
		wg.Wait()
	})

	t.Run("PuttingBatchAfterMaxMemoryForBatchWithSeqEqualLastPopWillEvictOldestSeq", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batches := []core.Batch{}
		for i := 2; i <= 6; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}
		// put seq 2
		require.NoError(t, test.partitionPrefetchIndex.Put(batches[0]))
		// pop seq 2
		test.partitionPrefetchIndex.PopOrWait(batches[0])
		// put seq 3-5
		for _, batch := range batches[1:4] {
			test.partitionPrefetchIndex.Put(batch)
		}

		calledChan := make(chan struct{})

		// Act
		go func() {
			// put seq 6
			test.partitionPrefetchIndex.Put(batches[4])
			calledChan <- struct{}{}
		}()
		time.Sleep(eventuallyTick)
		test.partitionPrefetchIndex.PopOrWait(batches[4])

		// Assert
		<-calledChan
		require.False(t, test.batchCache.Has(batches[1]))
	})

	t.Run("PuttingBatchAfterMaxMemoryForBatchWithSeqEqualLastPopWillEvictOldestSeq", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 3)
		defer test.finish()
		batches := []core.Batch{}
		for i := 2; i <= 6; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}
		// put seq 2
		require.NoError(t, test.partitionPrefetchIndex.Put(batches[0]))
		// pop seq 2
		test.partitionPrefetchIndex.PopOrWait(batches[0])
		// put seq 3-5
		for _, batch := range batches[1:4] {
			test.partitionPrefetchIndex.Put(batch)
		}

		calledChan := make(chan struct{})

		// Act
		go func() {
			// put seq 6
			test.partitionPrefetchIndex.Put(batches[4])
			calledChan <- struct{}{}
		}()
		time.Sleep(eventuallyTick)
		test.partitionPrefetchIndex.PopOrWait(batches[4])

		// Assert
		<-calledChan
		require.False(t, test.batchCache.Has(batches[1]))
	})

	t.Run("EvictedBatchTimersWillBeStopped", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batches := []core.Batch{}
		for i := 1; i <= 4; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}

		// Act
		go func() {
			// pop last batch
			test.partitionPrefetchIndex.PopOrWait(batches[3])
		}()
		// put all the batches
		for _, batch := range batches {
			require.NoError(t, test.partitionPrefetchIndex.Put(batch))
		}

		// Assert
		// blocks with seqs 1, 2 are evicted, 4 is poped
		require.Equal(t, test.timers[0].stopCallCount, 1)
		require.Equal(t, test.timers[1].stopCallCount, 1)
	})
}

func TestPartitionPrefetchIndex_PutForce(t *testing.T) {
	putCommonTests(t, func(pi *assembler.PartitionPrefetchIndex, b core.Batch) error {
		return pi.PutForce(b)
	})

	t.Run("PuttingBatchAfterMaxMemoryWillBeCompletedWithoutEviction", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batches := []core.Batch{}
		for i := 0; i < 10; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}

		// Act
		for _, batch := range batches {
			require.NoError(t, test.partitionPrefetchIndex.PutForce(batch))
		}

		// Assert
		// all the batches are in the cache
		for _, batch := range batches {
			require.True(t, test.forcePutCache.Has(batch))
		}
		// no timer was stoped
		for _, timer := range test.timers {
			require.Zero(t, timer.stopCallCount)
		}
	})

	t.Run("BatchWillBePutOnlyInForcedPutCache", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batches := []core.Batch{}
		for i := 0; i < 10; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}

		// Act
		for _, batch := range batches {
			require.NoError(t, test.partitionPrefetchIndex.PutForce(batch))
		}

		// Assert
		for _, batch := range batches {
			require.True(t, test.forcePutCache.Has(batch))
		}
		for _, batch := range batches {
			require.False(t, test.batchCache.Has(batch))
		}
	})

	t.Run("BatchWillDeletedFromForcedPutCacheAfterPop", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batch := testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, 0, []int{1})
		require.NoError(t, test.partitionPrefetchIndex.PutForce(batch))

		// Act
		poppedBatch, err := test.partitionPrefetchIndex.PopOrWait(batch)

		// Assert
		require.NoError(t, err)
		testutil.AssertBatchIdsEquals(t, batch, poppedBatch)
		require.False(t, test.forcePutCache.Has(batch))
	})

	t.Run("PutForceBatchDoesNotCreateTtlTimer", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batch := testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, 0, []int{1})

		// Act
		require.NoError(t, test.partitionPrefetchIndex.PutForce(batch))

		// Assert
		require.Empty(t, test.timers)
	})

	t.Run("ForcePutBatchWillNotBeEvictedByOtherPut", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batches := []core.Batch{}
		for i := 0; i < 10; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}
		require.NoError(t, test.partitionPrefetchIndex.Put(batches[1]))
		require.NoError(t, test.partitionPrefetchIndex.Put(batches[2]))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			poppedBatch, err := test.partitionPrefetchIndex.PopOrWait(batches[3])
			require.NoError(t, err)
			testutil.AssertBatchIdsEquals(t, batches[3], poppedBatch)
		}()

		// Act
		require.NoError(t, test.partitionPrefetchIndex.PutForce(batches[0]))
		require.NoError(t, test.partitionPrefetchIndex.Put(batches[3]))

		// Assert
		wg.Wait()
		require.True(t, test.forcePutCache.Has(batches[0]))
		require.False(t, test.batchCache.Has(batches[1]))
	})
}

func TestPartitionPrefetchIndex_Stop(t *testing.T) {
	t.Run("WaitingPopShouldReturnProperErrorWhenPrefetchIndexStops", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batchId := testutil.CreateMockBatchId(test.partition.Shard, test.partition.Primary, 1, nil)

		// Act
		go func() {
			time.Sleep(eventuallyTick)
			test.partitionPrefetchIndex.Stop()
		}()
		_, err1 := test.partitionPrefetchIndex.PopOrWait(batchId)
		_, err2 := test.partitionPrefetchIndex.PopOrWait(batchId)

		// Assert
		require.ErrorIs(t, err1, utils.ErrOperationCancelled)
		require.ErrorIs(t, err2, utils.ErrOperationCancelled)
	})

	t.Run("WaitingPutShouldReturnProperErrorWhenPrefetchIndexStops", func(t *testing.T) {
		// Arrange
		test := setupPartitionPrefetchIndexTest(t, 2)
		defer test.finish()
		batches := []core.Batch{}
		for i := 1; i <= 4; i++ {
			batches = append(batches, testutil.CreateMockBatch(test.partition.Shard, test.partition.Primary, types.BatchSequence(i), []int{1}))
		}
		errs := []error{}

		// Act
		go func() {
			time.Sleep(eventuallyTick)
			test.partitionPrefetchIndex.Stop()
		}()
		for _, batch := range batches {
			errs = append(errs, test.partitionPrefetchIndex.Put(batch))
		}

		// Assert
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		require.ErrorIs(t, errs[2], utils.ErrOperationCancelled)
		require.ErrorIs(t, errs[3], utils.ErrOperationCancelled)
	})
}

func TestDefaultTimerFactory(t *testing.T) {
	t.Run("ShouldFireActionAfterTimeout", func(t *testing.T) {
		// Arrange
		factory := assembler.DefaultTimerFactory{}
		done := make(chan struct{})

		// Act
		factory.Create(10*time.Millisecond, func() {
			done <- struct{}{}
		})

		// Assert
		<-done
	})

	t.Run("ShouldNotFireActionIfStopped", func(t *testing.T) {
		// Arrange
		factory := assembler.DefaultTimerFactory{}
		done := make(chan struct{})

		// Act
		timer := factory.Create(10*time.Millisecond, func() {
			done <- struct{}{}
		})
		timer.Stop()

		// Assert
		select {
		case <-done:
			require.FailNow(t, "timer should have stopped")
		case <-time.After(time.Second):
		}
	})
}
