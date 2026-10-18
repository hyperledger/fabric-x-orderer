/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/pkg/errors"
)

type PendingStore struct {
	ReqIDGCInterval       time.Duration
	ReqIDLifetime         time.Duration
	Logger                *flogging.FabricLogger
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
	running               uint32
	closedWG              sync.WaitGroup
	closeOnce             sync.Once
	closeChan             chan struct{}
	resetChan             chan struct{}
	finishResetChan       chan struct{}
	stopChan              chan struct{}
}

func (ps *PendingStore) Init() {
	ps.reqID2Bucket = new(sync.Map)
	ps.currentBucket.Store(newBucket(ps.reqID2Bucket, 1))
	ps.lastTick.Store(ps.StartTime)
	ps.closeChan = make(chan struct{})
	ps.closeOnce = sync.Once{}
	ps.resetChan = make(chan struct{})
	ps.finishResetChan = make(chan struct{})
	ps.stopChan = make(chan struct{})
	ps.lastEpochChange = ps.StartTime
	ps.lastProcessedGC = ps.StartTime
}

func (ps *PendingStore) Start() {
	ps.closedWG.Add(1)
	go ps.run()
}

func (ps *PendingStore) run() {
	defer ps.closedWG.Done()
	atomic.StoreUint32(&ps.running, 1)
	for {
		select {
		case <-ps.closeChan:
			return
		case <-ps.stopChan:
			ps.Logger.Info("Stopped - waiting for reset or close")
			continue
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
	ps.stop()
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

func (ps *PendingStore) stop() {
	atomic.StoreUint32(&ps.stopped, 1)
}

// Stop temporarily halts processing while keeping the pending store alive.
// The store can be resumed via ResetTimestamps().
// Use Close() for permanent shutdown.
func (ps *PendingStore) Stop() {
	if ps.isClosed() {
		return
	}
	ps.stop()
	if atomic.LoadUint32(&ps.running) == 0 {
		return
	}
	select {
	case ps.stopChan <- struct{}{}:
	case <-ps.closeChan:
	}
}

func (ps *PendingStore) isStopped() bool {
	return atomic.LoadUint32(&ps.stopped) == 1
}

func (ps *PendingStore) Close() {
	ps.stop()
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
				// A concurrent removal may have replaced the tombstone with a fresh one.
				ps.reqID2Bucket.CompareAndDelete(k, v)
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
		if !bucket.getFirstStrikeTimestamp().IsZero() {
			continue
		}

		if now.Sub(bucket.lastTimestamp) <= ps.FirstStrikeThreshold {
			continue
		}

		ps.Logger.Infof("First strike occurred for bucket id %d of size %d", bucket.id, bucket.getSize())
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
		if bucket.getFirstStrikeTimestamp().IsZero() {
			continue
		}

		if now.Sub(bucket.getFirstStrikeTimestamp()) <= ps.SecondStrikeThreshold {
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
	// All ids removed together share one tombstone.
	tombstone := &bucket{id: 0, processedTimestamp: ps.now()}

	parallelForEachKey(requestIDs, func(reqID string) bool {
		if ps.isClosed() {
			return false
		}
		ps.removeRequest(reqID, tombstone)
		return true
	})
}

// removeRequest deletes the request if it is held, and otherwise tombstones its id so that a Submit of
// the request that arrives later is rejected. Swapping in the tombstone decides who deletes a held
// request: only the caller that swapped out the bucket holding it does, so concurrent removals of the
// same id (e.g., a duplicate id in requestIDs, or a Prune) delete it once. A deleted request's id is
// then erased, so the request can be resubmitted (e.g., when the batcher revives the requests of a
// pending BAF after a term change).
func (ps *PendingStore) removeRequest(reqID string, tombstone *bucket) {
	b, existed := ps.reqID2Bucket.Swap(reqID, tombstone)
	if !existed {
		return
	}
	bucket := b.(*bucket)
	if bucket.isDummyBucket() {
		return
	}

	if bucket.delete(reqID) {
		ps.OnDelete(reqID)
		ps.reqID2Bucket.CompareAndDelete(reqID, tombstone)
	}
}

// Contains reports whether a request with the given id is currently held in the store.
// A tombstoned (already-removed) request is reported as absent.
func (ps *PendingStore) Contains(reqID string) bool {
	v, ok := ps.reqID2Bucket.Load(reqID)
	if !ok {
		return false
	}
	b := v.(*bucket)
	if b.isDummyBucket() {
		return false
	}
	_, exists := b.requests.Load(reqID)
	return exists
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
		// The id is erased rather than tombstoned, so the request may be resubmitted. Erasing it only
		// if it still maps to b leaves a concurrent removal or resubmission of the request intact.
		if ps.reqID2Bucket.CompareAndDelete(reqID, b) && b.delete(reqID) {
			ps.OnDelete(reqID)
		}
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
		if err := currentBucket.tryInsert(reqID, request); !errors.Is(err, errBucketSealed) {
			return err
		}
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
