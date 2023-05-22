package request

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Semaphore interface {
	Acquire(ctx context.Context, n int64) error

	Release(n int64)
}

type PendingStore struct {
	Logger                      Logger
	Inspector                   RequestInspector
	FirstStrikeRemovalDeadline  time.Duration
	SecondStrikeRemovalDeadline time.Duration
	FirstStrikeCallback         func([]byte)
	SecondStrikeCallback        func([]byte)
	Time                        chan time.Time
	StartTime                   time.Time
	Epoch                       time.Duration
	Semaphore                   Semaphore
	lastTick                    atomic.Value
	reqID2Bucket                *sync.Map
	processed                   sync.Map
	currentBucket               atomic.Value
	buckets                     []*bucket
}

func (ps *PendingStore) Init() {
	ps.reqID2Bucket = new(sync.Map)
	ps.currentBucket.Store(&bucket{reqID2Bucket: ps.reqID2Bucket})
	ps.lastTick.Store(ps.StartTime)
}

func (ps *PendingStore) changeEpochs() {
	lastEpochChange := <-ps.Time
	for {
		now := <-ps.Time
		ps.lastTick.Store(now)
		if now.Sub(lastEpochChange) <= ps.Epoch {
			continue
		}

		ps.rotateBuckets(now)
	}
}

func (ps *PendingStore) checkFirstStrike(now time.Time) {

	var buckets []*bucket

	for _, bucket := range ps.buckets {
		if !bucket.firstStrikeTimestamp.IsZero() {
			continue
		}

		if now.Sub(bucket.lastTimestamp) <= ps.FirstStrikeRemovalDeadline {
			continue
		}

		bucket.firstStrikeTimestamp = now
		buckets = append(buckets, bucket)
	}

	go func() {
		for _, bucket := range buckets {
			bucket.requests.Range(func(_, v interface{}) bool {
				ps.FirstStrikeCallback(v.([]byte))
				return true
			})
		}
	}()
}

func (ps *PendingStore) rotateBuckets(now time.Time) {
	currentBucket := ps.currentBucket.Load().(*bucket)

	if !ps.currentBucket.CompareAndSwap(currentBucket, currentBucket.seal(now)) {
		panic("programming error: swap should not have failed")
	}

	ps.buckets = append(ps.buckets, currentBucket)
}

func (ps *PendingStore) RemoveRequests(requestIDs ...string) {
	workerNum := runtime.NumCPU()

	var wg sync.WaitGroup
	wg.Add(workerNum)

	now := ps.now()

	var ensureSingleDelete sync.Map

	for workerID := 0; workerID < workerNum; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			ps.removeRequestsByWorker(workerID, requestIDs, workerNum, &ensureSingleDelete, now)
		}(workerID)
	}

	wg.Wait()
}

func (ps *PendingStore) removeRequestsByWorker(workerID int, requestIDs []string, workerNum int, ensureSingleDelete *sync.Map, now time.Time) {
	for i, reqID := range requestIDs {
		if i%workerNum != workerID {
			continue
		}

		// We can only insert a request once, so we should ensure
		// we cannot delete it twice.
		// Deleting it twice will mess up our accounting and will cause an overflow
		// in the amount of requests in the bucket.
		if _, duplicateReq := ensureSingleDelete.LoadOrStore(reqID, struct{}{}); duplicateReq {
			continue
		}

		_, insertPending := ps.processed.LoadOrStore(reqID, now)

		// If we tried to store and succeeded, it means no one inserted before us,
		// and no one will insert after us.
		// Therefore, we can ignore the for block.

		// However, if we were too late to store, then either an insert takes place
		// concurrently, or happened in the past.
		// We need to wait for the insert to complete before we continue to deletion,
		// otherwise we will have a zombie request that will never be deleted.

		for insertPending {
			b, exists := ps.reqID2Bucket.Load(reqID)
			if !exists {
				continue
			}

			insertPending = !b.(*bucket).Delete(reqID)
		}
	}
}

func (ps *PendingStore) Submit(request []byte, ctx context.Context) error {
	if err := ps.Semaphore.Acquire(ctx, 1); err != nil {
		return err
	}

	reqID := ps.Inspector.RequestID(request)

	if _, loaded := ps.processed.LoadOrStore(reqID, ps.now()); loaded {
		ps.Logger.Debugf("request %s already processed", reqID)
		ps.Semaphore.Release(1)
		return nil
	}

	for {
		currentBucket := ps.currentBucket.Load().(*bucket)
		if !currentBucket.TryInsert(reqID, request) {
			continue
		}
		return nil
	}

}

func (ps *PendingStore) now() time.Time {
	return ps.lastTick.Load().(time.Time)
}

type bucket struct {
	reqID2Bucket         *sync.Map
	size                 uint32
	lock                 sync.RWMutex
	lastTimestamp        time.Time
	firstStrikeTimestamp time.Time
	requests             sync.Map
}

func (b *bucket) seal(now time.Time) *bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastTimestamp = now

	return &bucket{
		reqID2Bucket: b.reqID2Bucket,
	}
}

func (b *bucket) TryInsert(reqID string, request []byte) bool {
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

func (b *bucket) Delete(reqID string) bool {
	if _, existed := b.reqID2Bucket.LoadAndDelete(reqID); !existed {
		return false
	}

	_, existed := b.requests.LoadAndDelete(reqID)
	if !existed {
		return false
	}

	atomic.AddUint32(&b.size, ^0)
	return true
}
