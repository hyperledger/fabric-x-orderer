package request

import (
	"context"
	"sync"
	"sync/atomic"
)

type BatchStore struct {
	currentBatch      *batch
	readyBatches      []*batch
	onDelete          func(key string)
	batchMaxSize      uint32
	batchMaxSizeBytes uint32
	logger            Logger
	keys2Batches      sync.Map
	lock              sync.RWMutex
	signal            sync.Cond
}

const sizeBytesOffset = 32

type batch struct {
	// use one uint64 var to represent both the size and the size in bytes of a batch
	// and increment both using one atomic operation.
	sizeBytes uint64
	enqueued  uint32
	m         sync.Map
}

func (b *batch) Load(key any) (value any, ok bool) {
	return b.m.Load(key)
}

func (b *batch) isEnqueued() bool {
	return atomic.LoadUint32(&b.enqueued) == 1
}

func (b *batch) markEnqueued() {
	atomic.StoreUint32(&b.enqueued, 1)
}

func (b *batch) Store(key, value any) {
	b.m.Store(key, value)
}

func (b *batch) Range(f func(key, value any) bool) {
	b.m.Range(f)
}

func (b *batch) Delete(key any) {
	b.m.Delete(key)
}

func (b *batch) Prune(f func(key, value any) error) {
	delFunc := func(key, value any) bool {
		if f(key, value) != nil {
			b.m.Delete(key)
		}
		return true
	}
	b.m.Range(delFunc)
}

func NewBatchStore(batchMaxSize uint32, batchMaxSizeBytes uint32, onDelete func(string), logger Logger) *BatchStore {
	bs := &BatchStore{
		currentBatch:      &batch{},
		onDelete:          onDelete,
		batchMaxSize:      batchMaxSize,
		batchMaxSizeBytes: batchMaxSizeBytes,
		logger:            logger,
	}
	bs.signal = sync.Cond{L: &bs.lock}

	return bs
}

func (bs *BatchStore) Lookup(key string) (interface{}, bool) {
	val, exists := bs.keys2Batches.Load(key)
	if !exists {
		return nil, false
	}

	batch := val.(*batch)

	return batch.Load(key)
}

func (bs *BatchStore) Insert(key string, value interface{}, size uint32) bool {
	for {
		// Try to add to the current batch. It doesn't matter if we don't end up using it,
		// we only care about if it's higher than the limit or not.
		bs.lock.RLock()
		newSize := atomic.AddUint64(&bs.currentBatch.sizeBytes, uint64(size)+(1<<sizeBytesOffset))
		full := uint32(newSize>>sizeBytesOffset) > bs.batchMaxSize
		fullBytes := uint32(newSize) > bs.batchMaxSizeBytes
		currBatch := bs.currentBatch

		if !full && !fullBytes {
			_, exists := bs.keys2Batches.LoadOrStore(key, currBatch)
			if exists {
				bs.lock.RUnlock()
				return false
			}
			currBatch.Store(key, value)
			bs.lock.RUnlock()
			return true
		}

		bs.lock.RUnlock()

		// Else, current batch is full.
		// So markEnqueued it and then create a new batch to use.
		bs.lock.Lock()
		// First, check if someone already enqueued the batch.
		if currBatch.isEnqueued() {
			// Try to insert again
			bs.lock.Unlock()
			continue
		}
		// Else, we should enqueue ourselves
		currBatch.markEnqueued()
		bs.readyBatches = append(bs.readyBatches, currBatch)
		// Create an empty batch to be used
		bs.currentBatch = &batch{}
		// If we have a waiting fetch, notify it
		bs.signal.Signal()
		bs.lock.Unlock()
	}
}

func (bs *BatchStore) ForEach(f func(k, v interface{})) {
	bs.lock.RLock()
	defer bs.lock.RUnlock()

	for _, batch := range bs.readyBatches {
		batch.Range(func(k, v interface{}) bool {
			f(k, v)
			return true
		})
	}

	bs.currentBatch.Range(func(k, v interface{}) bool {
		f(k, v)
		return true
	})
}

func (bs *BatchStore) Prune(f func(k, v interface{}) error) {
	bs.lock.RLock()
	defer bs.lock.RUnlock()

	for _, batch := range bs.readyBatches {
		batch.Prune(f)
	}

	bs.currentBatch.Prune(f)
}

func (bs *BatchStore) Remove(key string) {
	val, exists := bs.keys2Batches.LoadAndDelete(key)
	if !exists {
		return
	}

	batch := val.(*batch)
	batch.Delete(key)
	bs.onDelete(key)
}

func (bs *BatchStore) Fetch(ctx context.Context) []interface{} {
	// Do we have a batch ready for us?
	bs.lock.Lock()
	defer bs.lock.Unlock()

	finished := make(chan struct{})

	defer func() {
		close(finished)
	}()

	go func() {
		select {
		case <-ctx.Done():
			bs.signal.Signal()
			return
		case <-finished:
			return
		}
	}()

	if len(bs.readyBatches) > 0 {
		return bs.dequeueBatch()
	}

	// Else, either wait for the timeout
	// or for a new batch to be enqueued.
	bs.signal.Wait()

	// Prefer a ready and full batch over a non-empty one
	for len(bs.readyBatches) > 0 {
		dequeued := bs.dequeueBatch()
		if len(dequeued) == 0 { // still might be empty if requests were pruned
			continue
		}
		return dequeued
	}

	// But if no full batch can be found, use the non-empty one
	returnedBatch := bs.currentBatch
	// If no request is found, return nil
	if atomic.LoadUint64(&returnedBatch.sizeBytes) == 0 {
		return nil
	}
	// Mark the current batch as empty, since we are returning its content
	// to the caller.
	bs.currentBatch = &batch{}
	return bs.prepareBatch(returnedBatch)
}

func (bs *BatchStore) dequeueBatch() []interface{} {
	result := bs.prepareBatch(bs.readyBatches[0])
	batches := bs.readyBatches[1:]
	bs.readyBatches = make([]*batch, len(bs.readyBatches)-1)
	copy(bs.readyBatches, batches)
	return result
}

func (bs *BatchStore) prepareBatch(readyBatch *batch) []interface{} {
	readyBatch.markEnqueued() // make sure it is marked as enqueued before fetch returns it

	batch := make([]interface{}, 0, bs.batchMaxSize*2)

	readyBatch.Range(func(k, v interface{}) bool {
		batch = append(batch, v)
		return true
	})

	return batch
}
