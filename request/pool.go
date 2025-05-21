/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"golang.org/x/sync/semaphore"
)

// RequestInspector extracts the id of a given request.
type RequestInspector interface {
	// RequestID returns the id of the given request.
	RequestID(req []byte) string
}

//go:generate counterfeiter -o mocks/striker.go . Striker

// Striker defines the actions taken after timeouts occur
type Striker interface {
	OnFirstStrikeTimeout([]byte)
	OnSecondStrikeTimeout()
}

// Pool implements requests pool, maintains pool of given size provided during
// construction. In case there are more incoming request than the given size it will
// block during submit until there will be space to submit new ones.
type Pool struct {
	lock            sync.RWMutex
	pending         *PendingStore
	logger          types.Logger
	inspector       RequestInspector
	options         PoolOptions
	striker         Striker
	batchStore      *BatchStore
	semaphore       *semaphore.Weighted
	closed          uint32
	stopped         uint32
	batchingEnabled bool
}

// requestItem captures request related information
type requestItem struct {
	request []byte
}

// PoolOptions is the pool configuration
type PoolOptions struct {
	MaxSize               uint64
	BatchMaxSize          uint32
	BatchMaxSizeBytes     uint32
	RequestMaxBytes       uint64
	SubmitTimeout         time.Duration
	FirstStrikeThreshold  time.Duration
	SecondStrikeThreshold time.Duration
	AutoRemoveTimeout     time.Duration
}

// NewPool constructs a new requests pool
func NewPool(logger types.Logger, inspector RequestInspector, options PoolOptions, striker Striker) *Pool {
	rp := &Pool{
		logger:    logger,
		inspector: inspector,
		semaphore: semaphore.NewWeighted(int64(options.MaxSize)),
		options:   options,
		striker:   striker,
	}

	rp.start()

	return rp
}

func (rp *Pool) start() {
	rp.batchStore = rp.createBatchStore()
	rp.pending = rp.createPendingStore()
	rp.pending.Init()
	rp.pending.Start()
}

func (rp *Pool) createPendingStore() *PendingStore {
	return &PendingStore{
		Inspector:             rp.inspector,
		ReqIDGCInterval:       rp.options.AutoRemoveTimeout / 4,
		ReqIDLifetime:         rp.options.AutoRemoveTimeout,
		Time:                  time.NewTicker(time.Second).C,
		StartTime:             time.Now(),
		Logger:                rp.logger,
		SecondStrikeThreshold: rp.options.SecondStrikeThreshold,
		FirstStrikeThreshold:  rp.options.FirstStrikeThreshold,
		OnDelete: func(key string) {
			rp.semaphore.Release(1)
		},
		Epoch:                time.Second,
		FirstStrikeCallback:  rp.striker.OnFirstStrikeTimeout,
		SecondStrikeCallback: rp.striker.OnSecondStrikeTimeout,
	}
}

func (rp *Pool) createBatchStore() *BatchStore {
	return NewBatchStore(rp.options.BatchMaxSize, rp.options.BatchMaxSizeBytes, func(key string) {
		rp.semaphore.Release(1)
	}, rp.logger)
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if rp.isClosed() || rp.isStopped() {
		return errors.Errorf("pool halted or closed, request rejected")
	}

	if uint64(len(request)) > rp.options.RequestMaxBytes {
		return fmt.Errorf(
			"submitted request (%d) is bigger than request max bytes (%d)",
			len(request),
			rp.options.RequestMaxBytes,
		)
	}

	reqID := rp.inspector.RequestID(request)

	ctx, cancel := context.WithTimeout(context.Background(), rp.options.SubmitTimeout)
	defer cancel()

	if err := rp.semaphore.Acquire(ctx, 1); err != nil {
		rp.logger.Warnf("timed out enqueuing request %s to pool", reqID)
		return fmt.Errorf("timed out enqueuing request %s to pool", reqID)
	}

	if !rp.isBatchingEnabled() {
		return rp.submitToPendingStore(reqID, request)
	}

	return rp.submitToBatchStore(reqID, request)
}

func (rp *Pool) submitToPendingStore(reqID string, request []byte) error {
	rp.logger.Debugf("submitting request %s to pending store", reqID)
	err := rp.pending.Submit(request)
	if err != nil {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already added to the pool", reqID)
		return err
	}
	rp.logger.Debugf("submitted request %s to pending store", reqID)
	return nil
}

func (rp *Pool) submitToBatchStore(reqID string, request []byte) error {
	rp.logger.Debugf("submitting request %s to batch store", reqID)

	reqCopy := make([]byte, len(request))
	copy(reqCopy, request)

	reqItem := &requestItem{
		request: reqCopy,
	}

	inserted := rp.batchStore.Insert(reqID, reqItem, uint32(len(request)))
	if !inserted {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already added to the pool", reqID)
		return errors.Errorf("request %s already inserted", reqID)
	}

	rp.logger.Debugf("submitted request %s to batch store", reqID)
	return nil
}

// NextRequests returns the next requests to be batched.
func (rp *Pool) NextRequests(ctx context.Context) [][]byte {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if rp.isClosed() || rp.isStopped() {
		rp.logger.Warnf("pool halted or closed, returning nil")
		return nil
	}

	if !rp.isBatchingEnabled() {
		rp.logger.Warnf("NextRequests is called when batching is not enabled")
		return nil
	}

	requests := rp.batchStore.Fetch(ctx)

	rawRequests := make([][]byte, len(requests))
	for i := 0; i < len(requests); i++ {
		rawRequests[i] = requests[i].(*requestItem).request
	}

	return rawRequests
}

func (rp *Pool) RemoveRequests(requestsIDs ...string) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if rp.isClosed() {
		return
	}

	if !rp.isBatchingEnabled() {
		rp.pending.RemoveRequests(requestsIDs...)
		return
	}

	for _, requestID := range requestsIDs {
		rp.batchStore.Remove(requestID)
	}
}

func (rp *Pool) Prune(predicate func([]byte) error) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if rp.isBatchingEnabled() {
		rp.batchStore.Prune(func(_, v interface{}) error {
			req := v.(*requestItem).request
			return predicate(req)
		})
	} else {
		rp.pending.Prune(predicate)
	}
}

// Close closes the pool
func (rp *Pool) Close() {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	atomic.StoreUint32(&rp.closed, 1)
	if rp.pending != nil {
		rp.pending.Close()
	}
	rp.pending = nil
	rp.batchStore = nil
}

func (rp *Pool) isClosed() bool {
	return atomic.LoadUint32(&rp.closed) == 1
}

// Halt stops the callbacks of the first and second strikes.
func (rp *Pool) Halt() {
	atomic.StoreUint32(&rp.stopped, 1)
	if !rp.isBatchingEnabled() {
		rp.pending.Stop()
	}
}

func (rp *Pool) isStopped() bool {
	return atomic.LoadUint32(&rp.stopped) == 1
}

// Restart restarts the pool.
// When batching is set to true the pool is expected to respond to NextRequests.
func (rp *Pool) Restart(batching bool) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	defer atomic.StoreUint32(&rp.stopped, 0)

	if rp.isClosed() {
		return
	}

	rp.Halt()

	batchingWasEnabled := rp.isBatchingEnabled()

	if batchingWasEnabled && batching {
		// if batching was already enabled there is nothing to do
		return
	}

	if !batchingWasEnabled && !batching {
		// if batching was not enabled anyway just reset timestamps of the pending store
		rp.pending.ResetTimestamps()
		return
	}

	rp.setBatching(batching) // change the batching

	if batchingWasEnabled { // batching was enabled and now it is not
		rp.moveToPendingStore()
		return
	}

	// batching was not enabled but now it is
	rp.moveToBatchStore()
}

func (rp *Pool) setBatching(enabled bool) {
	rp.batchingEnabled = enabled
}

func (rp *Pool) isBatchingEnabled() bool {
	return rp.batchingEnabled
}

func (rp *Pool) moveToPendingStore() {
	requests := make([][]byte, 0, rp.options.MaxSize)
	rp.batchStore.ForEach(func(_, v interface{}) {
		requests = append(requests, v.(*requestItem).request)
	})
	rp.pending = rp.createPendingStore()
	rp.pending.Init()
	for _, req := range requests {
		reqInfo := rp.inspector.RequestID(req)
		if err := rp.submitToPendingStore(reqInfo, req); err != nil {
			rp.logger.Errorf("Could not submit request to pending store; error: %s", err)
			return
		}
	}
	rp.pending.Start()
	rp.batchStore = nil
}

func (rp *Pool) moveToBatchStore() {
	rp.pending.Close()
	requests := rp.pending.GetAllRequests(rp.options.MaxSize)
	rp.batchStore = rp.createBatchStore()
	for _, req := range requests {
		reqInfo := rp.inspector.RequestID(req)
		if err := rp.submitToBatchStore(reqInfo, req); err != nil {
			rp.logger.Errorf("Could not submit request into batch store; error: %s", err)
			return
		}
	}
	rp.pending = nil
}
