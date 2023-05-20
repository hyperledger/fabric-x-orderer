package request

import (
	"context"
	"golang.org/x/sync/semaphore"
	"sync"
	"sync/atomic"
	"time"
)

type PendingStore struct {
	Logger                      Logger
	Inspector                   RequestInspector
	FirstStrikeRemovalDeadline  time.Duration
	SecondStrikeRemovalDeadline time.Duration
	FirstStrikeCallback         func([]byte)
	SecondStrikeCallback        func([]byte)
	MaxCapacity                 int
	Time                        chan time.Time
	Epoch                       time.Duration
	semaphore                   *semaphore.Weighted
	lock                        sync.Mutex
	reqID2Bucket                sync.Map
	processed                   sync.Map
	currentBucket               atomic.Value
	buckets                     []*bucket
}

func (ps *PendingStore) Init() {
	ps.semaphore = semaphore.NewWeighted(int64(ps.MaxCapacity))
	ps.currentBucket.Store(&bucket{})
}

func (ps *PendingStore) changeEpochs() {
	lastEpochChange := <-ps.Time
	for {
		now := <-ps.Time
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
	if !ps.currentBucket.CompareAndSwap(currentBucket, &bucket{}) {
		panic("programming error: swap should not have failed")
	}

	currentBucket.lock.Lock()
	currentBucket.lastTimestamp = now
	currentBucket.lock.Unlock()

	ps.buckets = append(ps.buckets, currentBucket)
}

func (ps *PendingStore) Submit(request []byte, ctx context.Context) error {
	if err := ps.semaphore.Acquire(ctx, 1); err != nil {
		return err
	}

	reqID := ps.Inspector.RequestID(request)

	if _, loaded := ps.processed.LoadOrStore(reqID, struct{}{}); loaded {
		ps.Logger.Debugf("request %s already processed", reqID)
		ps.semaphore.Release(1)
		return nil
	}

	for {
		currentBucket := ps.currentBucket.Load().(*bucket)
		currentBucket.lock.RLock()
		closed := currentBucket.lastTimestamp.IsZero()
		if closed {
			currentBucket.lock.RUnlock()
			continue
		}

		currentBucket.requests.Store(reqID, request)
		ps.reqID2Bucket.Store(reqID, currentBucket)
		currentBucket.lock.RUnlock()
		return nil
	}

}

type bucket struct {
	lock                 sync.RWMutex
	lastTimestamp        time.Time
	firstStrikeTimestamp time.Time
	requests             sync.Map
}
