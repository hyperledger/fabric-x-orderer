package assembler_test

import (
	"testing"

	"arma/common/types"
	"arma/core"
	"arma/node/assembler"
	"arma/testutil"

	"github.com/stretchr/testify/require"
)

func testCacheWrongShardOrParty(t *testing.T, cacheOp func(*assembler.BatchCache, types.BatchID)) {
	t.Run("WrongShardParameterShouldPanic", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(1, 2)
		batchId := testutil.CreateMockBatchId(types.ShardID(2), types.PartyID(2), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { cacheOp(cache, batchId) })
	})

	t.Run("WrongPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(1, 2)
		batchId := testutil.CreateMockBatchId(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { cacheOp(cache, batchId) })
	})

	t.Run("WrongShardAndPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(1, 2)
		batchId := testutil.CreateMockBatchId(types.ShardID(3), types.PartyID(3), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { cacheOp(cache, batchId) })
	})
}

func TestBatchCache_Has(t *testing.T) {
	testCacheWrongShardOrParty(t, func(bc *assembler.BatchCache, batchId types.BatchID) { bc.Has(batchId) })

	t.Run("ReturnsFalseIfItemDoesNotExist", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(1, 1)
		batchId := testutil.CreateMockBatchId(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)

		// Act
		res := cache.Has(batchId)

		// Assert
		require.False(t, res)
	})

	t.Run("ReturnsTrueIfItemExist", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(1, 1)
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
		cache := assembler.NewBatchCache(1, 1)
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
		cache := assembler.NewBatchCache(1, 1)
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
		cache := assembler.NewBatchCache(1, 1)
		cache.Put(testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil))

		// Act
		_, err := cache.Pop(testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(2), nil))

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchDoesNotExist)
	})

	t.Run("WhenMultipleDigestsBatchesReturnsTheCorrectBatch", func(t *testing.T) {
		// Arrange
		cache := assembler.NewBatchCache(1, 1)
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
		cache := assembler.NewBatchCache(1, 1)
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
		cache := assembler.NewBatchCache(1, 1)
		batch := testutil.CreateEmptyMockBatch(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), nil)
		require.NoError(t, cache.Put(batch))

		// Act
		err := cache.Put(batch)

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchAlreadyExists)
	})
}
