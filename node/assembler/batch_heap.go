package assembler

import (
	"container/heap"

	"arma/common/types"
	"arma/core"
)

type BatchHeapItem[T any] struct {
	Batch core.Batch
	Value T
}

type HeapLessComperator[T any] func(item1, item2 *BatchHeapItem[T]) bool

// batchHeap implements heap.Interface in order to use go's heap
type batchHeap[T any] struct {
	data         []*BatchHeapItem[T]
	batchToIndex *BatchMapper[types.BatchID, int]
	less         HeapLessComperator[T]
}

func newBatchHeap[T any](partition ShardPrimary, less HeapLessComperator[T]) *batchHeap[T] {
	return &batchHeap[T]{
		data:         []*BatchHeapItem[T]{},
		batchToIndex: NewBatchMapper[types.BatchID, int](partition),
		less:         less,
	}
}

// Len returns the size of the heap
// This method is required by heap.Interface
func (h *batchHeap[T]) Len() int {
	return len(h.data)
}

// Less compares two heap items according to less function
// This method is required by heap.Interface
func (h *batchHeap[T]) Less(i, j int) bool {
	item1 := h.data[i]
	item2 := h.data[j]
	return h.less(item1, item2)
}

// Swap two heap items
// This method is required by heap.Interface
func (h *batchHeap[T]) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
	h.batchToIndex.Put(h.data[i].Batch, i)
	h.batchToIndex.Put(h.data[j].Batch, j)
}

// Push new batch to heap
// This method is required by heap.Interface
func (h *batchHeap[T]) Push(x interface{}) {
	item := x.(*BatchHeapItem[T])
	h.batchToIndex.Put(item.Batch, len(h.data))
	h.data = append(h.data, item)
}

// Pop removes the oldest batch
// This method is required by heap.Interface
func (h *batchHeap[T]) Pop() interface{} {
	n := len(h.data)
	lastItem := h.data[n-1]
	h.data = h.data[:n-1]
	return lastItem
}

// Peek retrieves the oldest heap item
func (h *batchHeap[T]) Peek() *BatchHeapItem[T] {
	if h.Len() == 0 {
		return nil
	}
	return h.data[0]
}

// Remove the given batch from the heap
func (h *batchHeap[T]) Remove(batchId types.BatchID) (*BatchHeapItem[T], error) {
	idx, err := h.batchToIndex.Get(batchId)
	if err != nil {
		return nil, ErrBatchDoesNotExist
	}
	item := heap.Remove(h, idx).(*BatchHeapItem[T])
	_, err = h.batchToIndex.Remove(batchId)
	if err != nil {
		return nil, ErrBatchDoesNotExist
	}
	return item, nil
}

// BatchHeap is a min (according to the given less comparator) heap for batches, the top item will be the batch with the min value.
// Not thread safe.
type BatchHeap[T any] struct {
	heap *batchHeap[T]
}

func NewBatchHeap[T any](partition ShardPrimary, less HeapLessComperator[T]) *BatchHeap[T] {
	return &BatchHeap[T]{
		heap: newBatchHeap[T](partition, less),
	}
}

// Push adds new batch
func (h *BatchHeap[T]) Push(item *BatchHeapItem[T]) error {
	if h.heap.batchToIndex.Has(item.Batch) {
		return ErrBatchAlreadyExists
	}
	heap.Push(h.heap, item)
	return nil
}

// Peek retrieves next batch by order
func (h *BatchHeap[T]) Peek() *BatchHeapItem[T] {
	item := h.heap.Peek()
	if item == nil {
		return nil
	}
	return item
}

// Pop next batch by order
func (h *BatchHeap[T]) Pop() *BatchHeapItem[T] {
	if h.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(h.heap).(*BatchHeapItem[T])
}

// Has returns true if batch exists
func (h *BatchHeap[T]) Has(batchId types.BatchID) bool {
	return h.heap.batchToIndex.Has(batchId)
}

// Remove batch from heap
func (h *BatchHeap[T]) Remove(batchId types.BatchID) (*BatchHeapItem[T], error) {
	return h.heap.Remove(batchId)
}
