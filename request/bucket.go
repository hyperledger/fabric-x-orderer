package request

import (
	"sync"
	"sync/atomic"
	"time"
)

type bucket struct {
	id                   uint64
	reqID2Bucket         *sync.Map
	size                 uint32
	lock                 sync.RWMutex
	lastTimestamp        time.Time
	firstStrikeTimestamp time.Time
	requests             sync.Map
	processedTimestamp   time.Time
}

func newBucket(reqID2Bucket *sync.Map, id uint64) *bucket {
	return &bucket{reqID2Bucket: reqID2Bucket, id: id}
}

func (b *bucket) isDummyBucket() bool {
	return b.id == 0
}

func (b *bucket) getSize() uint32 {
	return atomic.LoadUint32(&b.size)
}

func (b *bucket) seal(now time.Time) *bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastTimestamp = now

	return newBucket(b.reqID2Bucket, b.id+1)
}

func (b *bucket) resetTimestamp(t time.Time) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastTimestamp = t
	b.firstStrikeTimestamp = time.Time{}
}

func (b *bucket) setFirstStrikeTimestamp(t time.Time) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.firstStrikeTimestamp = t
}

func (b *bucket) getFirstStrikeTimestamp() time.Time {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.firstStrikeTimestamp
}

func (b *bucket) tryInsert(reqID string, request []byte) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	if !b.lastTimestamp.IsZero() {
		return false
	}

	if _, existed := b.reqID2Bucket.LoadOrStore(reqID, b); existed {
		return true
	}
	b.requests.Store(reqID, request)
	atomic.AddUint32(&b.size, 1)

	return true
}

func (b *bucket) delete(reqID string) bool {
	_, existed := b.requests.LoadAndDelete(reqID)
	if !existed {
		return false
	}

	b.reqID2Bucket.Delete(reqID)

	atomic.AddUint32(&b.size, ^uint32(0))
	return true
}
