package request

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
)

type Semaphore interface {
	Acquire(ctx context.Context, n int64) error

	Release(n int64)
}

type PendingStore struct {
	ReqIDGCInterval       time.Duration
	ReqIDLifetime         time.Duration
	Logger                types.Logger
	Inspector             RequestInspector
	FirstStrikeThreshold  time.Duration
	SecondStrikeThreshold time.Duration
	FirstStrikeCallback   func([]byte)
	SecondStrikeCallback  func()
	Time                  <-chan time.Time
	StartTime             time.Time
	Epoch                 time.Duration
	OnDelete              func(key string)
	lastTick              atomic.Value
	lastEpochChange       time.Time
	lastProcessedGC       time.Time
	reqID2Bucket          *sync.Map
	currentBucket         atomic.Value
	buckets               []*bucket
	stopped               uint32
	closedWG              sync.WaitGroup
	closeOnce             sync.Once
	closeChan             chan struct{}
	resetChan             chan struct{}
	finishResetChan       chan struct{}
}

func (ps *PendingStore) Init() {
	ps.reqID2Bucket = new(sync.Map)
	ps.currentBucket.Store(newBucket(ps.reqID2Bucket, 1))
	ps.lastTick.Store(ps.StartTime)
	ps.closeChan = make(chan struct{})
	ps.closeOnce = sync.Once{}
	ps.resetChan = make(chan struct{})
	ps.finishResetChan = make(chan struct{})
	ps.lastEpochChange = ps.StartTime
	ps.lastProcessedGC = ps.StartTime
}

func (ps *PendingStore) Start() {
	ps.closedWG.Add(1)
	go ps.run()
}

func (ps *PendingStore) run() {
	defer ps.closedWG.Done()
	for {
		select {
		case <-ps.closeChan:
			return
		case <-ps.resetChan:
			ps.reset()
		case now := <-ps.Time:
			ps.lastTick.Store(now)
			ps.changeEpochs(now)
		}
	}
}

func (ps *PendingStore) ResetTimestamps() {
	select {
	case <-ps.closeChan:
		return
	case ps.resetChan <- struct{}{}:
	}
	select {
	case <-ps.closeChan: // if closed simply return
		return
	case <-ps.finishResetChan: // wait for reset to finish
	}
}

func (ps *PendingStore) reset() {
	ps.Stop()
	defer atomic.StoreUint32(&ps.stopped, 0)

	now := ps.now()
	for _, bucket := range ps.buckets {
		bucket.resetTimestamp(now)
	}

	select {
	case ps.finishResetChan <- struct{}{}:
	case <-ps.closeChan:
	}
}

func (ps *PendingStore) Stop() {
	atomic.StoreUint32(&ps.stopped, 1)
}

func (ps *PendingStore) isStopped() bool {
	return atomic.LoadUint32(&ps.stopped) == 1
}

func (ps *PendingStore) Close() {
	ps.Stop()
	ps.closeOnce.Do(func() {
		if ps.closeChan == nil {
			return
		}
		close(ps.closeChan)
	})
	ps.closedWG.Wait()
}

func (ps *PendingStore) isClosed() bool {
	select {
	case <-ps.closeChan:
		return true
	default:
		return false
	}
}

func (ps *PendingStore) changeEpochs(now time.Time) {
	if ps.isStopped() {
		return
	}

	if now.Sub(ps.lastEpochChange) <= ps.Epoch {
		return
	}

	ps.lastEpochChange = now

	ps.rotateBuckets(now)
	ps.garbageCollectEmptyBuckets()
	if now.Sub(ps.lastProcessedGC) > ps.ReqIDGCInterval {
		ps.lastProcessedGC = now
		ps.garbageCollectProcessed(now)
	}

	ps.checkFirstStrike(now)
	if ps.checkSecondStrike(now) {
		ps.Logger.Infof("second strike")
		ps.SecondStrikeCallback()
		return
	}
}

func (ps *PendingStore) garbageCollectProcessed(now time.Time) {
	ps.reqID2Bucket.Range(func(k, v interface{}) bool {
		b := v.(*bucket)

		if b.isDummyBucket() {
			if now.Sub(b.processedTimestamp) > ps.ReqIDLifetime {
				ps.reqID2Bucket.Delete(k)
			}
		}

		return true
	})
}

func (ps *PendingStore) garbageCollectEmptyBuckets() {
	var newBuckets []*bucket

	for _, bucket := range ps.buckets {
		if bucket.getSize() > 0 {
			newBuckets = append(newBuckets, bucket)
		} else {
			ps.Logger.Debugf("Garbage collected bucket %d", bucket.id)
		}
	}

	ps.buckets = newBuckets
}

func (ps *PendingStore) checkFirstStrike(now time.Time) {
	var buckets []*bucket

	for _, bucket := range ps.buckets {
		if !bucket.firstStrikeTimestamp.IsZero() {
			continue
		}

		if now.Sub(bucket.lastTimestamp) <= ps.FirstStrikeThreshold {
			continue
		}

		bucket.setFirstStrikeTimestamp(now)
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

func (ps *PendingStore) checkSecondStrike(now time.Time) bool {
	var detectedCensorship bool

	for _, bucket := range ps.buckets {
		if bucket.firstStrikeTimestamp.IsZero() {
			continue
		}

		if now.Sub(bucket.firstStrikeTimestamp) <= ps.SecondStrikeThreshold {
			continue
		}

		bucket.resetTimestamp(ps.now())
		detectedCensorship = true
		break
	}

	return detectedCensorship
}

func (ps *PendingStore) rotateBuckets(now time.Time) {
	currentBucket := ps.currentBucket.Load().(*bucket)

	if currentBucket.getSize() == 0 {
		return
	}

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

	for workerID := 0; workerID < workerNum; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			ps.removeRequestsByWorker(workerID, requestIDs, workerNum, now)
		}(workerID)
	}

	wg.Wait()
}

func (ps *PendingStore) removeRequestsByWorker(workerID int, requestIDs []string, workerNum int, now time.Time) {
	for i, reqID := range requestIDs {
		if ps.isClosed() {
			return
		}

		if i%workerNum != workerID {
			continue
		}

		ps.removeRequest(reqID, now)
	}
}

func (ps *PendingStore) removeRequest(reqID string, now time.Time) {
	b, existed := ps.reqID2Bucket.LoadOrStore(reqID, &bucket{id: 0, processedTimestamp: now})
	bucket := b.(*bucket)
	if !existed || bucket.isDummyBucket() {
		return
	}

	bucket.Delete(reqID)

	ps.OnDelete(reqID)
}

func (ps *PendingStore) Prune(predicate func([]byte) error) {
	ps.reqID2Bucket.Range(func(key, value any) bool {
		reqID := key.(string)
		b := value.(*bucket)
		request, exists := b.requests.Load(reqID)
		if !exists {
			return true
		}
		if predicate(request.([]byte)) == nil {
			return true
		}
		b.Delete(reqID)
		ps.OnDelete(reqID)
		return true
	})
}

func (ps *PendingStore) Submit(request []byte) error {
	if ps.isClosed() {
		return errors.Errorf("pending store closed, request rejected")
	}

	reqID := ps.Inspector.RequestID(request)

	// Insertion may fail if we have a concurrent sealing of the bucket.
	// In such a case, wait for a new un-sealed bucket to replace the current bucket.
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

// GetAllRequests returns all stored requests in the same order of their arrival, the oldest one will be the first
func (ps *PendingStore) GetAllRequests(max uint64) [][]byte {
	if !ps.isStopped() {
		ps.Stop()
		ps.Logger.Warnf("GetAllRequests should be called only when the pending store is stopped")
	}

	requests := make([][]byte, 0, max*2)

	for _, b := range ps.buckets {
		b.requests.Range(func(_, v interface{}) bool {
			requests = append(requests, v.([]byte))
			return true
		})
	}

	currentBucket := ps.currentBucket.Load().(*bucket)
	currentBucket.requests.Range(func(_, v interface{}) bool {
		requests = append(requests, v.([]byte))
		return true
	})

	return requests
}

type bucket struct {
	id                   uint64
	reqID2Bucket         *sync.Map
	size                 uint32
	lock                 sync.RWMutex
	lastTimestamp        time.Time
	firstStrikeTimestamp time.Time
	zeroTime             time.Time
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
	b.firstStrikeTimestamp = b.zeroTime
}

func (b *bucket) setFirstStrikeTimestamp(t time.Time) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.firstStrikeTimestamp = t
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
	_, existed := b.requests.LoadAndDelete(reqID)
	if !existed {
		return false
	}

	b.reqID2Bucket.Delete(reqID)

	atomic.AddUint32(&b.size, ^uint32(0))
	return true
}
