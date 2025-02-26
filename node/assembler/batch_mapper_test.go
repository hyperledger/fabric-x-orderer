package assembler_test

import (
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/assembler"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/stretchr/testify/require"
)

type fakeItem struct{ Id int }

func testMapperWrongShardOrParty(t *testing.T, mapperOp func(*assembler.BatchMapper[types.BatchID, int], types.BatchID)) {
	t.Run("WrongShardParameterShouldPanic", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 2}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(types.ShardID(2), partition.Primary, types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { mapperOp(mapper, batchId) })
	})

	t.Run("WrongPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 2}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(partition.Shard, types.PartyID(1), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { mapperOp(mapper, batchId) })
	})

	t.Run("WrongShardAndPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 2}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(types.ShardID(3), types.PartyID(3), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { mapperOp(mapper, batchId) })
	})
}

func TestBatchMapper_Has(t *testing.T) {
	testMapperWrongShardOrParty(t, func(m *assembler.BatchMapper[types.BatchID, int], batchId types.BatchID) { m.Has(batchId) })

	t.Run("ReturnsFalseIfItemDoesNotExist", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), nil)

		// Act
		res := mapper.Has(batchId)

		// Assert
		require.False(t, res)
	})

	t.Run("ReturnsTrueIfItemExist", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batches := []core.Batch{}
		for i := 0; i < 3; i++ {
			batch := testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(i), nil)
			batches = append(batches, batch)
			mapper.Put(batch, i)

		}

		// Act & Assert
		for _, batch := range batches {
			exists := mapper.Has(batch)
			require.True(t, exists)
		}
	})

	t.Run("ReturnsFalseIfDigestMismatchOnKey", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), []byte{1, 2, 3})
		batchIdDifferentDigest := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), []byte{2, 3, 4})
		mapper.Put(batchId, 1)

		// Act
		exists := mapper.Has(batchIdDifferentDigest)

		// Assert
		require.False(t, exists)
	})

	t.Run("ReturnsTrueIfNonPrimitiveItemExist", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, fakeItem](partition)
		batchId := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), nil)
		item := fakeItem{Id: 1234}
		mapper.Put(batchId, item)

		// Act
		res := mapper.Has(batchId)

		// Assert
		require.True(t, res)
	})
}

func TestBatchMapper_Get(t *testing.T) {
	testMapperWrongShardOrParty(t, func(m *assembler.BatchMapper[types.BatchID, int], batchId types.BatchID) { m.Get(batchId) })

	t.Run("ReturnsTheCorrectValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batches := []core.Batch{
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil),
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(2), nil),
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(3), nil),
		}
		for i, batch := range batches {
			mapper.Put(batch, i)
		}

		// Act & Assert
		for i, batch := range batches {
			value, err := mapper.Get(batch)
			require.NoError(t, err)
			require.Equal(t, i, value)
		}
	})

	t.Run("ReturnsTheCorrectNonePrimitiveValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, fakeItem](partition)
		batches := []core.Batch{
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil),
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(2), nil),
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(3), nil),
		}
		for i, batch := range batches {
			mapper.Put(batch, fakeItem{Id: i})
		}

		// Act & Assert
		for i, batch := range batches {
			value, err := mapper.Get(batch)
			require.NoError(t, err)
			require.Equal(t, fakeItem{Id: i}, value)
		}
	})

	t.Run("GettingUnexistingBatchWillRaiseAnError", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		mapper.Put(testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil), 0)

		// Act
		_, err := mapper.Get(testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(2), nil))

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchDoesNotExist)
	})

	t.Run("WhenMultipleDigestsBatchesReturnsTheCorrectValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), []byte{1, 2, 3})
		batchIdDifferentDigest := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), []byte{2, 3, 4})
		mapper.Put(batchId, 1)
		mapper.Put(batchIdDifferentDigest, 2)

		// Act
		val1, err1 := mapper.Get(batchId)
		val2, err2 := mapper.Get(batchIdDifferentDigest)

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Equal(t, 1, val1)
		require.Equal(t, 2, val2)
	})
}

func TestBatchMapper_Put(t *testing.T) {
	testMapperWrongShardOrParty(t, func(m *assembler.BatchMapper[types.BatchID, int], batchId types.BatchID) { m.Put(batchId, 1) })

	t.Run("SinglePutAndThenGetOnKeyShouldReturnTheValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batch := testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil)

		// Act
		mapper.Put(batch, 1)
		val, err := mapper.Get(batch)

		// Assert

		require.NoError(t, err)
		require.Equal(t, val, 1)
	})

	t.Run("MultiplePutsOnSameKeyWillReplaceOldValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batch := testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil)
		mapper.Put(batch, 1)

		// Act
		mapper.Put(batch, 2)

		// Assert
		val, err := mapper.Get(batch)
		require.NoError(t, err)
		require.Equal(t, val, 2)
	})
}

func TestBatchMapper_Insert(t *testing.T) {
	testMapperWrongShardOrParty(t, func(m *assembler.BatchMapper[types.BatchID, int], batchId types.BatchID) { m.Insert(batchId, 1) })

	t.Run("SingleInsertOnKeyWillSucceed", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batch := testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil)

		// Act
		insertSuccess := mapper.Insert(batch, 1)

		// Assert
		require.True(t, insertSuccess)
		val, err := mapper.Get(batch)
		require.NoError(t, err)
		require.Equal(t, val, 1)
	})

	t.Run("MultipleInsertsOnSameKeyShouldNotChangeTheValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batch := testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil)
		firstInsertSuccess := mapper.Insert(batch, 1)

		// Act
		secondInsertSuccess := mapper.Insert(batch, 2)

		// Assert
		require.True(t, firstInsertSuccess)
		require.False(t, secondInsertSuccess)
		val, err := mapper.Get(batch)
		require.NoError(t, err)
		require.Equal(t, val, 1)
	})
}

func TestBatchMapper_Remove(t *testing.T) {
	testMapperWrongShardOrParty(t, func(m *assembler.BatchMapper[types.BatchID, int], batchId types.BatchID) { m.Remove(batchId) })

	t.Run("RemoveWillRemoveJustTheGivenKey", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batches := []core.Batch{
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil),
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(2), nil),
			testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(3), nil),
		}
		for i, batch := range batches {
			mapper.Put(batch, i)
		}

		// Act & Assert
		for i, batch := range batches {
			value, err := mapper.Remove(batch)
			require.NoError(t, err)
			require.False(t, mapper.Has(batch))
			require.Equal(t, i, value)
			for j, otherBatch := range batches[i+1:] {
				value, err := mapper.Get(otherBatch)
				require.NoError(t, err)
				require.Equal(t, j+i+1, value)
			}
		}
	})

	t.Run("RemovingUnexistingBatchWillRaiseAnError", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		mapper.Put(testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil), 0)

		// Act
		_, err := mapper.Remove(testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(2), nil))

		// Act
		require.ErrorIs(t, err, assembler.ErrBatchDoesNotExist)
	})

	t.Run("RemovingSameBatchTwiceWillRaiseAnError", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batch := testutil.CreateEmptyMockBatch(partition.Shard, partition.Primary, types.BatchSequence(1), nil)
		mapper.Put(batch, 0)

		// Act
		_, err1 := mapper.Remove(batch)
		_, err2 := mapper.Remove(batch)

		// Assert
		require.NoError(t, err1)
		require.ErrorIs(t, err2, assembler.ErrBatchDoesNotExist)
	})

	t.Run("WhenMultipleDigestsBatchesRemovesTheCorrectOne", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		mapper := assembler.NewBatchMapper[types.BatchID, int](partition)
		batchId := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), []byte{1, 2, 3})
		batchIdDifferentDigest := testutil.CreateMockBatchId(partition.Shard, partition.Primary, types.BatchSequence(1), []byte{2, 3, 4})
		mapper.Put(batchId, 1)
		mapper.Put(batchIdDifferentDigest, 2)

		// Act
		val1, err1 := mapper.Remove(batchId)
		val2, err2 := mapper.Get(batchIdDifferentDigest)

		// Assert
		require.NoError(t, err1)
		require.Equal(t, 1, val1)
		require.False(t, mapper.Has(batchId))
		require.NoError(t, err2)
		require.Equal(t, 2, val2)
	})
}
