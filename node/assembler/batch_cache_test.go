/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
)

const batchCacheTestDefaultTag = "test tag"

func testCacheWrongShardOrParty(t *testing.T, cacheOp func(*assembler.BatchCache, types.BatchID)) {
	partition := assembler.ShardPrimary{Shard: 1, Primary: 2}

	t.Run("WrongShardParameterShouldPanic", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(partition, batchCacheTestDefaultTag)
		batchId := testutil.CreateMockBatchId(types.ShardID(2), types.PartyID(2), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { cacheOp(cache, batchId) })
	})

	t.Run("WrongPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(partition, batchCacheTestDefaultTag)
		batchId := testutil.CreateMockBatchId(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { cacheOp(cache, batchId) })
	})

	t.Run("WrongShardAndPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(partition, batchCacheTestDefaultTag)
		batchId := testutil.CreateMockBatchId(types.ShardID(3), types.PartyID(3), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { cacheOp(cache, batchId) })
	})
}

func TestBatchCache_Has(t *testing.T) {
	testCacheWrongShardOrParty(t, func(bc *assembler.BatchCache, batchId types.BatchID) { bc.Has(batchId) })

	t.Run("ReturnsFalseIfItemDoesNotExist", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batchId := testutil.CreateMockBatchId(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)

		// Act
		res := cache.Has(batchId)

		// Assert
		require.False(t, res)
	})

	t.Run("ReturnsTrueIfItemExist", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batches := []core.Batch{}
		for i := 0; i < 3; i++ {
			batch := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(i), nil)
			batches = append(batches, batch)
			require.NoError(t, cache.Put(batch))

		}

		// Act & Assert
		for _, batch := range batches {
			exists := cache.Has(batch)
			require.True(t, exists)
		}
	})

	t.Run("ReturnsFalseIfDigestMismatch", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batchId := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3})
		batchIdDifferentDigest := testutil.CreateMockBatchId(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{2, 3, 4})
		cache.Put(batchId)

		// Act
		exists := cache.Has(batchIdDifferentDigest)

		// Assert
		require.False(t, exists)
	})
}

func TestBatchCache_Pop(t *testing.T) {
	testCacheWrongShardOrParty(t, func(bc *assembler.BatchCache, batchId types.BatchID) { bc.Has(batchId) })

	t.Run("ReturnsTheCorrectBatch", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batches := []core.Batch{
			testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil),
			testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(2), nil),
			testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(3), nil),
		}
		for _, batch := range batches {
			require.NoError(t, cache.Put(batch))
		}

		// Act & Assert
		for _, batch := range batches {
			poppedBatch, err := cache.Pop(batch)
			require.NoError(t, err)
			testutil.AssertBatchIdsEquals(t, batch, poppedBatch)
		}
	})

	t.Run("GettingUnexistingBatchWillRaiseAnError", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		cache.Put(testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil))

		// Act
		_, err := cache.Pop(testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(2), nil))

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchDoesNotExist)
	})

	t.Run("WhenMultipleDigestsBatchesReturnsTheCorrectBatch", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batch := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3})
		batchDifferentDigest := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{2, 3, 4})
		require.NoError(t, cache.Put(batch))
		require.NoError(t, cache.Put(batchDifferentDigest))

		// Act
		poppedBatch, err1 := cache.Pop(batch)
		poppedBatchDifferentDigest, err2 := cache.Pop(batchDifferentDigest)

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		testutil.AssertBatchIdsEquals(t, batch, poppedBatch)
		testutil.AssertBatchIdsEquals(t, batchDifferentDigest, poppedBatchDifferentDigest)
	})
}

func TestBatchCache_Put(t *testing.T) {
	testCacheWrongShardOrParty(t, func(bc *assembler.BatchCache, batchId types.BatchID) { bc.Has(batchId) })

	t.Run("SinglePutAndThenGetShouldReturnTheBatch", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batch := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)

		// Act
		require.NoError(t, cache.Put(batch))
		poppedBatch, err := cache.Pop(batch)

		// Assert

		require.NoError(t, err)
		testutil.AssertBatchIdsEquals(t, batch, poppedBatch)
	})

	t.Run("MultiplePutsOnSameBatchWillRaiseAnError", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batch := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)
		require.NoError(t, cache.Put(batch))

		// Act
		err := cache.Put(batch)

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchAlreadyExists)
	})
}

func TestBatchCache_SizeBytes(t *testing.T) {
	testCacheWrongShardOrParty(t, func(bc *assembler.BatchCache, batchId types.BatchID) { bc.Has(batchId) })

	t.Run("PuttingMultipleBatchesShouldIncreaseSizeAccordingly", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batch1 := testutil.CreateMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []int{1})
		batch2 := testutil.CreateMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []int{2})
		batch3 := testutil.CreateMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []int{3})
		require.NoError(t, cache.Put(batch1))
		require.NoError(t, cache.Put(batch2))
		require.NoError(t, cache.Put(batch3))

		// Act
		size := cache.SizeBytes()

		// Assert
		require.Equal(t, 6, size)
	})

	t.Run("RemovingBatchesShouldDecreaseSizeAccordingly", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(assembler.ShardPrimary{Shard: 1, Primary: 1}, batchCacheTestDefaultTag)
		batch1 := testutil.CreateMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []int{1})
		batch2 := testutil.CreateMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []int{2})
		batch3 := testutil.CreateMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []int{3})
		require.NoError(t, cache.Put(batch1))
		require.NoError(t, cache.Put(batch2))
		require.NoError(t, cache.Put(batch3))

		// Act
		_, err := cache.Pop(batch2)
		require.NoError(t, err)
		size := cache.SizeBytes()

		// Assert
		require.Equal(t, 4, size)
	})
}
