/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
)

const (
	BATCH_CACHE_TAG     = "cache"
	FORCE_PUT_CACHE_TAG = "force put batches"
)

type StoppableTimer interface {
	Stop() bool
}

//go:generate counterfeiter -o ./mocks/timer_factory.go . TimerFactory
type TimerFactory interface {
	Create(time.Duration, func()) StoppableTimer
}

type DefaultTimerFactory struct{}

func (dtf *DefaultTimerFactory) Create(d time.Duration, f func()) StoppableTimer {
	return time.AfterFunc(d, f)
}

type ShardPrimary struct {
	Shard   types.ShardID
	Primary types.PartyID
}

func ShardPrimaryFromBatch(batchId types.BatchID) ShardPrimary {
	return ShardPrimary{
		Shard:   batchId.Shard(),
		Primary: batchId.Primary(),
	}
}

//go:generate counterfeiter -o ./mocks/partition_prefetch_index.go . PartitionPrefetchIndexer
type PartitionPrefetchIndexer interface {
	PopOrWait(batchId types.BatchID) (types.Batch, error)
	Put(batch types.Batch) error
	PutForce(batch types.Batch) error
	Stop()
}

//go:generate counterfeiter -o ./mocks/partition_prefetch_index_factory.go . PartitionPrefetchIndexerFactory
type PartitionPrefetchIndexerFactory interface {
	Create(
		partition ShardPrimary,
		logger types.Logger,
		defaultTtl time.Duration,
		maxSizeBytes int,
		timerFactory TimerFactory,
		batchCacheFactory BatchCacheFactory,
		batchRequestChan chan types.BatchID,
		popWaitMonitorTimeout time.Duration) PartitionPrefetchIndexer
}

type DefaultPartitionPrefetchIndexerFactory struct{}

func (f *DefaultPartitionPrefetchIndexerFactory) Create(partition ShardPrimary, logger types.Logger, defaultTtl time.Duration, maxSizeBytes int, timerFactory TimerFactory, batchCacheFactory BatchCacheFactory, batchRequestChan chan types.BatchID, popWaitMonitorTimeout time.Duration) PartitionPrefetchIndexer {
	return NewPartitionPrefetchIndex(
		partition,
		logger,
		defaultTtl,
		maxSizeBytes,
		timerFactory,
		batchCacheFactory,
		batchRequestChan,
		popWaitMonitorTimeout,
	)
}

// PartitionPrefetchIndex handles the prefetch actions for a specific partition (shard + primary),
//
// The core purpose of this struct is to pair batch ids with full batches:
//  1. An external module, with separate a goroutine, is getting batch ids from the Consensus and is calling PopOrWait to get the full batch.
//     Please note that there is no constraint on the sequence of the batch.
//  2. An external module, with separate a goroutine, "stream fetcher" is getting the full batch from the Batcher and using Put to insert the batch into the indexer.
//     Please note that it is assumed that the batches are arriving and inserted with Put in a stream of increasing sequence numbers.
//  3. An external module, with separate a goroutine, "unary fetcher" is fetching a single full batch upon specific request from the Batcher and using PutForce to insert the batch into the indexer.
//
// Here is a detailed technical explanation:
//
// Getting batches - PopOrWait:
//
//  1. A batch id is extracted from a Consensus decision, PopOrWait is called with this batch id.
//
//  2. PopOrWait updates the max requested batch sequence - maxPoppedCallSeq and Broadcasts on stateCond.
//     It is important for the operation of Put - this variable indicates for the Put method if the batch it is trying to put is relevant or it should wait.
//
//  3. PopOrWait tries to pop the relevant batch from it's cache, if the batch already exists, the batch is removed from the indexer and returned.
//
//  4. If the batch does not exist, we request the batch from the unary fetcher external module only if:
//     a. This batch was not requested by the current call to the method (we avoid multiple requests).
//     b. This batch sequence is less than the sequence of the most recently Put batch
//     (since Put is called with increasing sequence, and we already passed it, we need to make a separate request).
//     c. This batch sequence is equal to the sequence of the most recently Put batch -
//     in this case this is a batch with the same <shard, primary, sequence> but with different digests (otherwise we would have returned it).
//
//  5. We keep waiting on stateCond, waiting for Put or PutForce to add the desired batch.
//
//     Note:
//     a. If the sequence is higher than the sequence of the most recently Put batch - we wait for it to be added to the index using Put.
//     b. If the context is cancelled, the method will return with an error.
//
// Put:
//
//  1. If the batch is too large, it will return ErrBatchTooLarge.
//
//  2. If the batch is already in the index, it will return ErrBatchAlreadyExists.
//
//  3. Aquire the lock.
//
//  4. We update lastPutSeqRequest with the sequence of the batch.
//
//  5. If there is space in the index - put the batch, Broadcast on stateCond and return.
//
//  6. While there is no space for the batch:
//     a. If the sequence of the batch is lower than the max popped sequence (stored in `maxPoppedCallSeq`) we are prefetching too fast,
//     wait (sync.Cond wait) for the batches in the index to be popped prior to putting this batch.
//     b. Else - we remove the oldest batch from the index.
//
//  7. There is a free space - insert the batch into the index.
//
//  8. Release the lock.
//
//     Note: If the context is cancelled, the method will return with an error.
//
// PutForce:
//
// If PopOrWait has requested a batch, this batch needs to be put into the index from an external module using PutForce,
// unlike the regular Put that has space constraints, this put always succeeds immediately, since this batch is needed now.
//
// Auto Eviction:
// Each batch is put with a TTL timer, after it is expired, we automatically remove the batch and Broadcast on stateCond
type PartitionPrefetchIndex struct {
	logger    types.Logger
	partition ShardPrimary
	// last seq with call to put
	lastPutSeqRequest types.BatchSequence
	// last seq that was put into cache by Put or PutForce
	lastPutSeq types.BatchSequence
	// the max pop call batch seq
	maxPoppedCallSeq    types.BatchSequence
	cache               *BatchCache
	forcedPutCache      *BatchCache
	sequenceHeap        *BatchHeap[StoppableTimer]
	stateCond           *sync.Cond
	maxSizeBytes        int
	batchRequestChan    chan types.BatchID
	defaultTtl          time.Duration
	timerFactory        TimerFactory
	cancellationContext context.Context
	cancelContextFunc   context.CancelFunc
	// delay before PopOrWait requests the batch
	popWaitMonitorTimeout time.Duration
}

func NewPartitionPrefetchIndex(partition ShardPrimary, logger types.Logger, defaultTtl time.Duration, maxSizeBytes int, timerFactory TimerFactory, batchCacheFactory BatchCacheFactory, batchRequestChan chan types.BatchID, popWaitMonitorTimeout time.Duration) *PartitionPrefetchIndex {
	ctx, cancel := context.WithCancel(context.Background())
	pi := &PartitionPrefetchIndex{
		logger:                logger,
		partition:             partition,
		cache:                 batchCacheFactory.CreateWithTag(partition, BATCH_CACHE_TAG),
		forcedPutCache:        batchCacheFactory.CreateWithTag(partition, FORCE_PUT_CACHE_TAG),
		sequenceHeap:          NewBatchHeap(partition, func(item1, item2 *BatchHeapItem[StoppableTimer]) bool { return item1.Batch.Seq() < item2.Batch.Seq() }),
		stateCond:             sync.NewCond(&sync.Mutex{}),
		maxSizeBytes:          maxSizeBytes,
		batchRequestChan:      batchRequestChan,
		defaultTtl:            defaultTtl,
		timerFactory:          timerFactory,
		cancellationContext:   ctx,
		cancelContextFunc:     cancel,
		popWaitMonitorTimeout: popWaitMonitorTimeout,
	}
	return pi
}

func (pi *PartitionPrefetchIndex) getName() string {
	return fmt.Sprintf("partition <Sh: %d, Pr: %d>", pi.partition.Shard, pi.partition.Primary)
}

func (pi *PartitionPrefetchIndex) assertBatchPartition(batchId types.BatchID) {
	partition := ShardPrimaryFromBatch(batchId)
	if partition != pi.partition {
		panic(fmt.Sprintf("batch partition %v does not match the partition of the store %v", partition, pi.partition))
	}
}

func (pi *PartitionPrefetchIndex) PopOrWait(batchId types.BatchID) (types.Batch, error) {
	pi.logger.Debugf("Entry: PopOrWait partition %s on batch %s", pi.getName(), BatchToString(batchId))

	pi.assertBatchPartition(batchId)
	pi.stateCond.L.Lock()
	defer pi.stateCond.L.Unlock()
	if pi.maxPoppedCallSeq < batchId.Seq() {
		pi.maxPoppedCallSeq = batchId.Seq()
		pi.stateCond.Broadcast()
	}

	var wasBatchRequestedFlag int32
	var censorMonitor *time.Timer

	// while not exists, wait
	for {
		if pi.cancellationContext.Err() != nil {
			return nil, utils.ErrOperationCancelled
		}
		batch, err := pi.removeUnsafe(batchId)
		if err != nil && !errors.Is(err, ErrBatchDoesNotExist) {
			panic(fmt.Sprintf("unexpected error occured while poping batch %s: %v", BatchToString(batchId), err))
		}
		if err == nil {
			return batch, nil
		}

		pi.logger.Debugf("PopOrWait %s, not in cache; maxPoppedCallSeq: %d, lastPutSeqRequest: %d, lastPutSeq: %d; batch %s, err: %s",
			pi.getName(), pi.maxPoppedCallSeq, pi.lastPutSeqRequest, pi.lastPutSeq, BatchToString(batchId), err)

		// Here we wait for the batch with `batchID` to arrive from our own party batcher.
		// Normally it will get to us through one of delivery the stream we opened to it.
		// We may need to issue a search request if:
		// 1. There was already a batch with same <sh,Pr,Sq> but with different digest; or
		// 2. We have a primary that had failed and never got to give said batch to our batcher; or
		// 3. We have primary that is censoring our batcher.

		// batchId.Seq() == bs.lastPutSeq is true in case we have 2 digests
		doubleDigest := (batchId.Seq() < pi.lastPutSeqRequest || batchId.Seq() == pi.lastPutSeq)
		if atomic.LoadInt32(&wasBatchRequestedFlag) == 0 && doubleDigest {
			pi.logger.Debugf("PopOrWait %s, double digest, requesting batch %s", pi.getName(), BatchToString(batchId))
			atomic.StoreInt32(&wasBatchRequestedFlag, 0x1)
			pi.batchRequestChan <- batchId
		}

		if atomic.LoadInt32(&wasBatchRequestedFlag) == 0 {
			censorMonitor = time.AfterFunc(pi.popWaitMonitorTimeout, func() {
				atomic.StoreInt32(&wasBatchRequestedFlag, 0x1)
				pi.logger.Debugf("PopOrWait %s, censorship monitor, requesting batch %s", pi.getName(), BatchToString(batchId))
				pi.batchRequestChan <- batchId
				pi.stateCond.Broadcast()
			})
			defer censorMonitor.Stop()
		}

		pi.logger.Debugf("PopOrWait %s, going to wait for batch %s", pi.getName(), BatchToString(batchId))
		pi.stateCond.Wait()
	}
}

func (pi *PartitionPrefetchIndex) saveOrdinaryBatch(batch types.Batch) error {
	err := pi.cache.Put(batch)
	if err != nil {
		return err
	}
	var timer StoppableTimer
	pi.lastPutSeq = batch.Seq()
	timer = pi.timerFactory.Create(pi.defaultTtl, func() {
		pi.logger.Infof("TTL handler executed for batch %s", BatchToString(batch))
		pi.stateCond.L.Lock()
		_, err := pi.removeUnsafe(batch)
		pi.stateCond.L.Unlock()
		if err != nil {
			pi.logger.Errorf("there was unexpected error which removing a batch with expired TTL: %v", err)
		}
	})
	pi.sequenceHeap.Push(&BatchHeapItem[StoppableTimer]{Batch: batch, Value: timer})
	pi.stateCond.Broadcast()
	return nil
}

func (pi *PartitionPrefetchIndex) Put(batch types.Batch) error {
	pi.logger.Debugf("Put called for %s on batch %s", pi.getName(), BatchToString(batch))
	pi.assertBatchPartition(batch)
	batchSize := batchSizeBytes(batch)
	if batchSize > pi.maxSizeBytes {
		return ErrBatchTooLarge
	}
	pi.stateCond.L.Lock()
	defer pi.stateCond.L.Unlock()
	if pi.cache.Has(batch) {
		return ErrBatchAlreadyExists
	}
	pi.lastPutSeqRequest = batch.Seq()
	for pi.cache.SizeBytes()+batchSize > pi.maxSizeBytes {
		if pi.cancellationContext.Err() != nil {
			return utils.ErrOperationCancelled
		}
		if pi.maxPoppedCallSeq < batch.Seq() {
			// we are prefetching too fast, wait for the batches in the index to be used
			pi.stateCond.Wait()
			continue
		}
		// the batch we are trying to put is needed, we will evict a batch
		err := pi.evictOldestBatch()
		if err != nil {
			return fmt.Errorf("failed to evict batch: %w", err)
		}
	}
	return pi.saveOrdinaryBatch(batch)
}

func (bs *PartitionPrefetchIndex) PutForce(batch types.Batch) error {
	bs.logger.Debugf("PutForce called for %s on batch %s", bs.getName(), BatchToString(batch))
	bs.assertBatchPartition(batch)
	batchSize := batchSizeBytes(batch)
	if batchSize > bs.maxSizeBytes {
		return ErrBatchTooLarge
	}
	bs.stateCond.L.Lock()
	defer bs.stateCond.L.Unlock()
	err := bs.forcedPutCache.Put(batch)
	if err != nil {
		return err
	}
	bs.stateCond.Broadcast()
	return nil
}

func (pi *PartitionPrefetchIndex) evictOldestBatch() error {
	batchAndTimer := pi.sequenceHeap.Pop()
	batchAndTimer.Value.Stop()
	_, err := pi.cache.Pop(batchAndTimer.Batch)
	if err != nil {
		return err
	}
	pi.stateCond.Broadcast()
	return nil
}

func (pi *PartitionPrefetchIndex) removeUnsafe(batchId types.BatchID) (types.Batch, error) {
	// try to pop from the cache
	batch, err := pi.cache.Pop(batchId)
	if errors.Is(err, ErrBatchDoesNotExist) {
		// not in the cache, check if a force put
		batch, err = pi.forcedPutCache.Pop(batchId)
		if err == nil {
			pi.stateCond.Broadcast()
			return batch, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to remove batch from cache: %w", err)
	}
	// if here, a batch was found in the cache
	batchAndTimer, err := pi.sequenceHeap.Remove(batchId)
	if err != nil {
		return nil, fmt.Errorf("failed to remove batch from heap: %w", err)
	}
	batchAndTimer.Value.Stop()
	pi.stateCond.Broadcast()
	return batch, nil
}

func (bs *PartitionPrefetchIndex) Stop() {
	bs.cancelContextFunc()
	bs.stateCond.Broadcast()
	// Stop all the timers
	bs.stateCond.L.Lock()
	for {
		batchAndTimer := bs.sequenceHeap.Pop()
		if batchAndTimer == nil {
			break
		}
		batchAndTimer.Value.Stop()
	}
	bs.stateCond.L.Unlock()
}
