package request

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type BatchStore struct {
	batchReady      chan struct{}
	batchingEnabled uint32
	currentBatch    *batch
	readyBatches    []*batch
	onDelete        func(key string)
	batchMaxSize    uint32
	maxCapacity     int
	keys2Batches    sync.Map
	lock            sync.RWMutex
}

type batch struct {
	lock     sync.RWMutex
	size     uint32
	enqueued bool
	m        sync.Map
}

func (b *batch) Load(key any) (value any, ok bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.m.Load(key)
}

func (b *batch) isEnqueued() bool {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.enqueued
}

func (b *batch) markEnqueued() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.enqueued = true
}

func (b *batch) Store(key, value any) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	b.m.Store(key, value)
}

func (b *batch) Range(f func(key, value any) bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	b.m.Range(f)
}

func (b *batch) Delete(key any) {
	b.m.Delete(key)
}

func NewBatchStore(maxCapacity int, batchMaxSize int, onDelete func(string)) *BatchStore {
	// Ensure the max capacity divides by the batchMaxSize of each batch
	maxCapacity = maxCapacity - (maxCapacity % batchMaxSize)

	if maxCapacity/batchMaxSize == 0 {
		panic(fmt.Sprintf("Max capacity (%d) cannot be lower than batch max size (%d)", maxCapacity, batchMaxSize))
	}

	bs := &BatchStore{
		batchReady:   make(chan struct{}),
		currentBatch: &batch{},
		onDelete:     onDelete,
		batchMaxSize: uint32(batchMaxSize),
		maxCapacity:  maxCapacity,
	}

	return bs
}

func (bs *BatchStore) printSizes() {
	bs.lock.RLock()
	defer bs.lock.RUnlock()
	s := bytes.Buffer{}
	s.WriteString(fmt.Sprintf(">>> Total %d batches\n", len(bs.readyBatches)))
	s.WriteString(fmt.Sprintf("current batch: %d\n", atomic.LoadUint32(&bs.currentBatch.size)))
	for i, batch := range bs.readyBatches {
		s.WriteString(fmt.Sprintf("batch %d: %d\n", i, atomic.LoadUint32(&batch.size)))
	}

	fmt.Println(s.String())
}

func (bs *BatchStore) Lookup(key string) (interface{}, bool) {
	val, exists := bs.keys2Batches.Load(key)
	if !exists {
		return nil, false
	}

	batch := val.(*batch)

	return batch.Load(key)
}

func (bs *BatchStore) Insert(key string, value interface{}) bool {
	for {
		// Try to add to the current batch. It doesn't matter if we don't end up using it,
		// we only care about if it's higher than the limit or not.
		bs.lock.RLock()
		full := atomic.AddUint32(&bs.currentBatch.size, 1) > bs.batchMaxSize
		currBatch := bs.currentBatch
		bs.lock.RUnlock()

		if !full {
			_, exists := bs.keys2Batches.LoadOrStore(key, currBatch)
			if exists {
				return false
			}
			currBatch.Store(key, value)
			return true
		}

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
		if bs.batchReady != nil {
			select {
			case <-bs.batchReady:
				// Already closed
			default:
				close(bs.batchReady)
			}
		}
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

func (bs *BatchStore) SetBatching(enabled bool) {
	if enabled {
		atomic.StoreUint32(&bs.batchingEnabled, 1)
	} else {
		atomic.StoreUint32(&bs.batchingEnabled, 0)
	}
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
	defer func() {
		bs.lock.Lock()
		defer bs.lock.Unlock()
		bs.batchReady = nil
	}()

	// Do we have a batch ready for us?
	bs.lock.Lock()
	if len(bs.readyBatches) > 0 {
		result := bs.prepareBatch(bs.readyBatches[0])
		batches := bs.readyBatches[1:]
		bs.readyBatches = make([]*batch, len(bs.readyBatches)-1)
		copy(bs.readyBatches, batches)
		bs.lock.Unlock()
		return result
	}
	bs.lock.Unlock()

	// Else, either wait for the timeout
	// or for a new batch to be enqueued.

	select {
	case <-bs.batchReady:
		// We have a batch ready,
		// dequeue it and return it.
		return bs.dequeueBatch()
	case <-ctx.Done():
		// Timeout expired, check if we have a batch ready.
		bs.lock.Lock()
		if len(bs.readyBatches) > 1 {
			bs.lock.Unlock()
			// We have a batch ready, so dequeue it and return it.
			return bs.dequeueBatch()
		}
		defer bs.lock.Unlock()
		// Else, we don't have a batch ready.
		// If the current batch is empty, return an empty batch.
		if atomic.LoadUint32(&bs.currentBatch.size) == 0 {
			return nil
		}
		// Otherwise, return what we have in the batch at the moment.
		returnedBatch := bs.currentBatch
		// Make a new batch to be the current batch
		bs.currentBatch = &batch{}
		return bs.prepareBatch(returnedBatch)
	}
}

func (bs *BatchStore) dequeueBatch() []interface{} {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	result := bs.prepareBatch(bs.readyBatches[0])
	tmp := bs.readyBatches[1:]

	bs.readyBatches = make([]*batch, len(bs.readyBatches)-1)
	copy(bs.readyBatches, tmp)

	return result
}

func (bs *BatchStore) prepareBatch(readyBatch *batch) []interface{} {
	batch := make([]interface{}, 0, bs.batchMaxSize*2)

	readyBatch.Range(func(k, v interface{}) bool {
		batch = append(batch, v)
		return true
	})

	return batch
}
