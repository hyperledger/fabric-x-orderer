/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/stretchr/testify/require"
)

func assertSameHeapItems[T any](t *testing.T, expected, actual *assembler.BatchHeapItem[T]) {
	require.True(t, types.BatchIDEqual(expected.Batch, actual.Batch))
	require.Equal(t, expected.Value, actual.Value)
}

func fillHeapWithBatches(t *testing.T, heap *assembler.BatchHeap[int], partition assembler.ShardPrimary, numberOfBatches int) []*assembler.BatchHeapItem[int] {
	items := []*assembler.BatchHeapItem[int]{}
	for i := 0; i < numberOfBatches; i++ {
		item := &assembler.BatchHeapItem[int]{
			Batch: createTestBatch(partition.Shard, partition.Primary, types.BatchSequence(i), []byte{byte(i)}),
			Value: i,
		}
		items = append(items, item)
		require.NoError(t, heap.Push(item))
	}
	return items
}

func sequenceComparator(item1, item2 *assembler.BatchHeapItem[int]) bool {
	return item1.Batch.Seq() < item2.Batch.Seq()
}

func testHeapWrongShardOrParty(t *testing.T, heapOp func(*assembler.BatchHeap[int], types.BatchID)) {
	t.Run("WrongShardParameterShouldPanic", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		fillHeapWithBatches(t, heap, partition, 3)
		batchId := createTestBatchId(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { heapOp(heap, batchId) })
	})

	t.Run("WrongPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		fillHeapWithBatches(t, heap, partition, 3)
		batchId := createTestBatchId(types.ShardID(1), types.PartyID(2), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { heapOp(heap, batchId) })
	})

	t.Run("WrongShardAndPartyParameterShouldPanic", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		fillHeapWithBatches(t, heap, partition, 3)
		batchId := createTestBatchId(types.ShardID(3), types.PartyID(3), types.BatchSequence(1), nil)

		// Act & Assert
		require.Panics(t, func() { heapOp(heap, batchId) })
	})
}

func TestHeap_NonPrimitiveGenericValueWorksAsExpected(t *testing.T) {
	// Arrange
	partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
	heap := assembler.NewBatchHeap(partition, func(item1, item2 *assembler.BatchHeapItem[[]byte]) bool { return item1.Value[0] < item2.Value[0] })
	items := []*assembler.BatchHeapItem[[]byte]{}

	// Act & Assert
	// push items
	for i := byte(0); i < 10; i++ {
		item := &assembler.BatchHeapItem[[]byte]{
			Batch: createTestBatch(partition.Shard, partition.Primary, types.BatchSequence(i), []byte{i}),
			Value: []byte{i, i, i},
		}
		items = append(items, item)
		require.NoError(t, heap.Push(item))
	}
	// peek returns the min item
	assertSameHeapItems(t, items[0], heap.Peek())

	// pop, and check that the now top is 2
	assertSameHeapItems(t, items[0], heap.Pop())
	assertSameHeapItems(t, items[1], heap.Peek())

	// remove 2, pop order should be 1,3
	popedItem, err := heap.Remove(items[2].Batch)
	require.NoError(t, err)
	assertSameHeapItems(t, items[2], popedItem)
	assertSameHeapItems(t, items[1], heap.Pop())
	assertSameHeapItems(t, items[3], heap.Pop())
}

func TestHeap_Peek(t *testing.T) {
	t.Run("ReturnsNilIfEmpty", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)

		// Act
		item := heap.Peek()

		// Assert
		require.Nil(t, item)
	})

	t.Run("ReturnsMinBatch", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 3)

		// Act
		item := heap.Peek()

		// Assert
		assertSameHeapItems(t, heapItems[0], item)
	})

	t.Run("MultiplePeekReturnSameValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 3)

		// Act & Assert
		for i := 0; i < 3; i++ {
			item := heap.Peek()
			assertSameHeapItems(t, heapItems[0], item)
		}
	})
}

func TestHeap_Has(t *testing.T) {
	testHeapWrongShardOrParty(t, func(heap *assembler.BatchHeap[int], batchId types.BatchID) {
		heap.Has(batchId)
	})

	t.Run("ReturnsNilIfNotExists", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)

		// Act
		exists := heap.Has(createTestBatch(1, 1, 1, []byte{1, 2, 3}))

		// Assert
		require.False(t, exists)
	})

	t.Run("ReturnsTrueIfExists", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 3)

		// Assert
		// Act & Assert
		for i := 0; i < 3; i++ {
			require.True(t, heap.Has(heapItems[i].Batch))
		}
	})

	t.Run("MultipleHasReturnSameValue", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 3)

		// Assert
		// Act & Assert
		for i := 0; i < 10; i++ {
			require.True(t, heap.Has(heapItems[0].Batch))
		}
	})
}

func TestHeap_Pop(t *testing.T) {
	t.Run("ReturnsNilIfEmpty", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)

		// Act
		top := heap.Pop()

		// Assert
		require.Nil(t, top)
	})

	t.Run("MultiplePopReturnBatchesByOrder", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 10)

		// Act & Assert
		for i := 0; i < 10; i++ {
			item := heap.Pop()
			assertSameHeapItems(t, heapItems[i], item)
		}
	})

	t.Run("PoppedBatchIsNoLongerInTheHeap", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 10)

		// Act & Assert
		for i := 0; i < 10; i++ {
			require.True(t, heap.Has(heapItems[i].Batch))
			heap.Pop()
			require.False(t, heap.Has(heapItems[i].Batch))
			// the batches that were not popped yet are still there
			for j := i + 1; j < 10; j++ {
				require.True(t, heap.Has(heapItems[j].Batch))
			}
		}
	})

	t.Run("PoppedBatchCanBePushedAgain", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 3)

		// Act
		// pop the min item, and push it back
		popedItem := heap.Pop()
		assertSameHeapItems(t, heapItems[0], popedItem)
		require.NoError(t, heap.Push(popedItem))

		// Assert
		require.True(t, heap.Has(popedItem.Batch))
		assertSameHeapItems(t, heapItems[0], heap.Peek())
		for i := 0; i < 3; i++ {
			assertSameHeapItems(t, heapItems[i], heap.Pop())
		}
	})

	t.Run("PoppedBatchCanBeRePushedAndRePopped", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 5)

		// Act & Assert
		for i := 0; i < 5; i++ {
			item := heap.Pop()
			assertSameHeapItems(t, heapItems[i], item)
			require.NoError(t, heap.Push(item))
			assertSameHeapItems(t, heapItems[i], heap.Pop())
			require.False(t, heap.Has(heapItems[i].Batch))
		}
		require.Nil(t, heap.Peek())
	})
}

func TestHeap_Remove(t *testing.T) {
	testHeapWrongShardOrParty(t, func(heap *assembler.BatchHeap[int], batchId types.BatchID) {
		heap.Remove(batchId)
	})

	t.Run("RemovingMultipleBatchesShouldWork", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 10)

		// Act & Assert
		for i := 0; i < 10; i++ {
			item, err := heap.Remove(heapItems[i].Batch)
			require.NoError(t, err)
			assertSameHeapItems(t, heapItems[i], item)
		}
	})

	t.Run("RemovingFromMiddleOfHeapKeepsItemsOrdered", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 10)
		removedIdx := 5
		item, err := heap.Remove(heapItems[removedIdx].Batch)
		require.NoError(t, err)
		assertSameHeapItems(t, heapItems[removedIdx], item)

		// Act & Assert
		for i := 0; i < 10; i++ {
			if i == removedIdx {
				continue
			}
			item := heap.Pop()
			assertSameHeapItems(t, heapItems[i], item)
		}
	})

	t.Run("RemovingAPoppedBatchReturnsAnError", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 10)
		popedItem := heap.Pop()
		assertSameHeapItems(t, heapItems[0], popedItem)

		// Act
		item, err := heap.Remove(popedItem.Batch)

		// Assert
		require.ErrorIs(t, err, assembler.ErrBatchDoesNotExist)
		require.Nil(t, item)
	})

	t.Run("RemovingAfterAPopKeepsTheRemainingItemsOrdered", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		heapItems := fillHeapWithBatches(t, heap, partition, 10)
		require.NotNil(t, heap.Pop()) // pops item 0, and swaps item 9 into its place
		removedIdx := 9

		// Act
		item, err := heap.Remove(heapItems[removedIdx].Batch)

		// Assert
		require.NoError(t, err)
		assertSameHeapItems(t, heapItems[removedIdx], item)
		for i := 1; i < removedIdx; i++ {
			assertSameHeapItems(t, heapItems[i], heap.Pop())
		}
		require.Nil(t, heap.Pop())
	})

	t.Run("RemovingBatchThatDoesNotExist", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		batch := createTestBatchId(1, 1, 1, nil)

		// Act
		_, err := heap.Remove(batch)

		// Assert
		require.Error(t, err)
	})
}

func TestHeap_Push(t *testing.T) {
	t.Run("PushSameBatchTwiceShouldRaiseAnError", func(t *testing.T) {
		// Arrange
		partition := assembler.ShardPrimary{Shard: 1, Primary: 1}
		heap := assembler.NewBatchHeap(partition, sequenceComparator)
		item1 := &assembler.BatchHeapItem[int]{
			Batch: createTestBatch(1, 1, 1, []byte{1}),
			Value: 1,
		}
		item2 := &assembler.BatchHeapItem[int]{
			Batch: createTestBatch(1, 1, 1, []byte{1}),
			Value: 2,
		}

		// Act &
		err1 := heap.Push(item1)
		err2 := heap.Push(item2)

		// Assert
		require.NoError(t, err1)
		require.ErrorIs(t, err2, assembler.ErrBatchAlreadyExists)
	})
}
