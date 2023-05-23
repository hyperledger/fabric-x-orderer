// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package request

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRequestTimeout             = 10 * time.Second // for unit tests only
	defaultMaxBytes                   = 100 * 1024       // default max request size would be of size 100Kb
	defaultDelMapSwitch               = time.Second * 20 // for cicle erase silice of delete elements
	defaultTimestampIncrementDuration = time.Second * 1
)

var (
	ErrReqAlreadyExists    = fmt.Errorf("request already exists")
	ErrReqAlreadyProcessed = fmt.Errorf("request already processed")
	ErrRequestTooBig       = fmt.Errorf("submitted request is too big")
	ErrSubmitTimeout       = fmt.Errorf("timeout submitting to request pool")
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

type RequestInspector interface {
	// RequestID returns info about the given request.
	RequestID(req []byte) string
}

// Pool implements requests pool, maintains pool of given size provided during
// construction. In case there are more incoming request than given size it will
// block during submit until there will be place to submit new ones.
type Pool struct {
	lock            sync.Mutex
	timestamp       uint64
	pending         *PendingStore
	logger          Logger
	inspector       RequestInspector
	options         PoolOptions
	batchStore      *BatchStore
	cancel          context.CancelFunc
	semaphore       *semaphore.Weighted
	closed          uint32
	stopped         uint32
	batchingEnabled uint32
}

type delMap struct {
	sync.Map
}

func (dm *delMap) Exists(key any) bool {
	_, exists := dm.Map.Load(key)
	return exists
}

func (dm *delMap) Store(key string, timestamp uint64) {
	dm.Map.Store(key, timestamp)
}

func (dm *delMap) GC(latestTimestamp uint64) {
	t1 := time.Now()
	var cleaned int
	var total int
	dm.Map.Range(func(k any, v any) bool {
		total++
		ts := v.(uint64)
		if ts < latestTimestamp {
			dm.Map.Delete(k)
			cleaned++
		}
		return true
	})

	fmt.Println("Garbage collection of", cleaned, "out of", total, "processed request references older than", latestTimestamp, "took", time.Since(t1))
}

func (dm *delMap) Size() int {
	var count int
	dm.Map.Range(func(_ any, _ any) bool {
		count++
		return true
	})
	return count
}

// requestItem captures request related information
type requestItem struct {
	request []byte
}

// PoolOptions is the pool configuration
type PoolOptions struct {
	MaxSize           int
	BatchMaxSize      int
	SubmitTimeout     time.Duration
	AutoRemoveTimeout time.Duration
}

// NewPool constructs new requests pool
func NewPool(log Logger, inspector RequestInspector, options PoolOptions) *Pool {
	if options.SubmitTimeout == 0 {
		options.SubmitTimeout = defaultRequestTimeout
	}
	if options.BatchMaxSize == 0 {
		options.BatchMaxSize = 1000
	}
	if options.MaxSize == 0 {
		options.MaxSize = 10000
	}

	ps := &PendingStore{
		ReqIDGCInterval:       options.AutoRemoveTimeout / 4,
		ReqIDLifetime:         options.AutoRemoveTimeout,
		Time:                  time.NewTicker(time.Second).C,
		StartTime:             time.Now(),
		Logger:                log,
		SecondStrikeThreshold: time.Minute / 2, // TODO: move this to pool options
		FirstStrikeThreshold:  time.Second * 3,
		Semaphore:             semaphore.NewWeighted(int64(options.MaxSize)),
		Epoch:                 time.Second,
		FirstStrikeCallback: func([]byte) {

		},
		SecondStrikeCallback: func() {},
	}

	rp := &Pool{
		pending:   ps,
		logger:    log,
		inspector: inspector,
		semaphore: semaphore.NewWeighted(int64(options.MaxSize)),
		options:   options,
	}

	rp.batchStore = NewBatchStore(options.MaxSize, options.BatchMaxSize, func(key string) {
		rp.semaphore.Release(1)
	})

	return rp
}

func (rp *Pool) SetBatching(enabled bool) {
	rp.batchStore.SetBatching(enabled)
	if enabled {
		atomic.StoreUint32(&rp.batchingEnabled, 1)
	} else {
		atomic.StoreUint32(&rp.batchingEnabled, 0)
	}
}

func (rp *Pool) isClosed() bool {
	return atomic.LoadUint32(&rp.closed) == 1
}

func (rp *Pool) Clear() {
	panic("should not have been called")
}

type pendingRequest struct {
	request      []byte
	ri           string
	arriveTime   uint64
	next         *pendingRequest
	forwarded    bool
	forwardTime  uint64
	complainedAt uint64
}

func (pr *pendingRequest) reset(now uint64) {
	atomic.StoreUint64(&pr.complainedAt, 0)
	atomic.StoreUint64(&pr.forwardTime, now)
	atomic.StoreUint64(&pr.arriveTime, now)
	pr.forwarded = false
	pr.next = nil
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	reqID := rp.inspector.RequestID(request)
	if rp.isClosed() {
		return errors.Errorf("pool closed, request rejected: %s", reqID)
	}

	if atomic.LoadUint32(&rp.batchingEnabled) == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), rp.options.SubmitTimeout)
		defer cancel()

		return rp.pending.Submit(request, ctx)
	}

	ctx, cancel := context.WithTimeout(context.Background(), rp.options.SubmitTimeout)
	defer cancel()

	if err := rp.semaphore.Acquire(ctx, 1); err != nil {
		rp.logger.Warnf("Timed out enqueuing request %s to pool", reqID)
		return fmt.Errorf("timed out")
	}

	reqCopy := make([]byte, len(request))
	copy(reqCopy, request)

	reqItem := &requestItem{
		request: reqCopy,
	}

	inserted := rp.batchStore.Insert(reqID, reqItem)
	if !inserted {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already added to the pool", reqID)
		return ErrReqAlreadyExists
	}

	return nil
}

// NextRequests returns the next requests to be batched.
// It returns at most maxCount requests, and at most maxSizeBytes, in a newly allocated slice.
// Return variable full indicates that the batch cannot be increased further by calling again with the same arguments.
func (rp *Pool) NextRequests(ctx context.Context) [][]byte {
	requests := rp.batchStore.Fetch(ctx)

	rawRequests := make([][]byte, len(requests))
	for i := 0; i < len(requests); i++ {
		rawRequests[i] = requests[i].(*requestItem).request
	}

	return rawRequests
}

func (rp *Pool) RemoveRequests(requests ...string) error {
	if atomic.LoadUint32(&rp.batchingEnabled) == 0 {
		rp.pending.RemoveRequests(requests...)
		return nil
	}

	for _, requestID := range requests {
		rp.batchStore.Remove(requestID)
	}
	return nil
}

// Close removes all the requests, stops all the timeout timers.
func (rp *Pool) Close() {
	atomic.StoreUint32(&rp.closed, 1)
	rp.Clear()
}

// StopTimers stops all the timeout timers attached to the pending requests, and marks the pool as "stopped".
// This which prevents submission of new requests, and renewal of timeouts by timer go-routines that where running
// at the time of the call to StopTimers().
func (rp *Pool) StopTimers() {
	atomic.StoreUint32(&rp.stopped, 1)

	rp.logger.Debugf("Stopped all timers")
}

// RestartTimers restarts all the timeout timers attached to the pending requests, as RequestForwardTimeout, and re-allows
// submission of new requests.
func (rp *Pool) RestartTimers() {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	// TODO
	rp.logger.Debugf("Restarted all timers")
}
