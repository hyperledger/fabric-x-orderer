/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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

// Prune invokes onPrune for every key whose predicate f returns a non-nil error.
// The batch does not touch its own map here: onPrune (supplied by BatchStore) is
// responsible for the full removal so that pruned keys go through the same
// cleanup as Remove (batch map, keys2Batches index, and the pool's onDelete).
func (b *batch) Prune(f func(key, value any) error, onPrune func(key any)) {
	b.m.Range(func(key, value any) bool {
		if f(key, value) != nil {
			onPrune(key)
		}
		return true
	})
}
