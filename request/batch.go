package request

import (
	"sync"
	"sync/atomic"
)

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
