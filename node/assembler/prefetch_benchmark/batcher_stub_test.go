package prefetch_benchmark_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/assembler"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"go.uber.org/zap/zapcore"
)

type batchCacheWithLock struct {
	lock                    sync.Mutex
	cache                   *assembler.BatchCache
	lastSameShardPrimarySeq types.BatchSequence
}

type batcherStub struct {
	logger              types.Logger
	shardId             types.ShardID
	parties             []types.PartyID
	batchGenerator      *batchGenerator
	primaryToWaitChan   map[types.PartyID]chan struct{}
	consensusChan       chan<- types.BatchID
	deliveryServiceChan chan core.Batch
	// prob of primary change
	primaryChangeProb           float64
	sameShardPartySeqProb       float64
	timeBetweenBatchGeneration  time.Duration
	primaryToBatchCacheWithLock map[types.PartyID]*batchCacheWithLock
	monitor                     *statsMonitor
	cancellationContext         context.Context
	cancelContextFunc           context.CancelFunc
	doneWg                      sync.WaitGroup
}

func newBatcherStub(
	t *testing.T,
	logLevel zapcore.Level,
	shardId types.ShardID,
	parties []types.PartyID,
	consensusChan chan types.BatchID,
	primaryChangeProb float64,
	sameShardPartySeqProb float64,
	batchesPerSecond int,
	txSize int,
	txInBatch int,
	monitor *statsMonitor,
) *batcherStub {
	deliveryServiceChan := make(chan core.Batch, PREFETCH_STRESS_TEST_MEMORY_FACTOR*batchesPerSecond)
	ctx, cancel := context.WithCancel(context.Background())

	bs := &batcherStub{
		logger:                      testutil.CreateLoggerForModule(t, fmt.Sprintf("BatcherStub <Shard:%d>", shardId), logLevel),
		shardId:                     shardId,
		parties:                     parties,
		batchGenerator:              newBatchGenerator(txInBatch, txSize),
		primaryToWaitChan:           make(map[types.PartyID]chan struct{}),
		consensusChan:               consensusChan,
		deliveryServiceChan:         deliveryServiceChan,
		primaryChangeProb:           primaryChangeProb,
		sameShardPartySeqProb:       sameShardPartySeqProb,
		timeBetweenBatchGeneration:  time.Second / time.Duration(batchesPerSecond),
		monitor:                     monitor,
		primaryToBatchCacheWithLock: make(map[types.PartyID]*batchCacheWithLock),
		cancellationContext:         ctx,
		cancelContextFunc:           cancel,
	}
	for _, partyId := range bs.parties {
		bs.primaryToWaitChan[partyId] = make(chan struct{}, 1)
		bs.primaryToBatchCacheWithLock[partyId] = &batchCacheWithLock{
			cache:                   assembler.NewBatchCache(assembler.ShardPrimary{Shard: shardId, Primary: partyId}, "test"),
			lastSameShardPrimarySeq: 0,
		}
	}
	return bs
}

func (bs *batcherStub) DeliveryChan() <-chan core.Batch {
	return bs.deliveryServiceChan
}

func (bs *batcherStub) Start() {
	for _, partyId := range bs.parties {
		bs.doneWg.Add(1)
		go func(partyId types.PartyID) {
			bs.logger.Debugf("Starting goroutine of batcher stub: shard=%d party=%d", bs.shardId, partyId)
			defer func() {
				bs.logger.Debugf("Exiting goroutine of batcher stub: shard=%d party=%d", bs.shardId, partyId)
				bs.doneWg.Done()
			}()
			var seq types.BatchSequence
			for {
				bs.logger.Debugf("Batcher shard:%d party:%d is waiting to be the leader", bs.shardId, partyId)
				select {
				case <-bs.primaryToWaitChan[partyId]:
				case <-bs.cancellationContext.Done():
					return
				}
				bs.logger.Debugf("%d is the leader", partyId)
				ticker := time.NewTicker(bs.timeBetweenBatchGeneration)
				bs.logger.Debugf("Created new ticker for every %v", bs.timeBetweenBatchGeneration)
				loop := true
				for loop {
					select {
					case <-ticker.C:
						bs.logger.Debugf("Tick occured")
						bs.generateBatch(partyId, &seq)
						if tossCoin(bs.primaryChangeProb) {
							ticker.Stop()
							newLeader := choosePrimary(bs.parties, partyId)
							bs.primaryToWaitChan[newLeader] <- struct{}{}
							bs.logger.Debugf("Changing batcher leader to %d", newLeader)
							loop = false
						}
					case <-bs.cancellationContext.Done():
						ticker.Stop()
						return
					}
				}
			}
		}(partyId)
	}
	// choose the first primary
	bs.primaryToWaitChan[choosePrimary(bs.parties, 9999)] <- struct{}{}
}

func (bs *batcherStub) Stop() {
	bs.cancelContextFunc()
	bs.doneWg.Wait()
	close(bs.deliveryServiceChan)
	for _, waitChan := range bs.primaryToWaitChan {
		close(waitChan)
	}
}

func (bs *batcherStub) GetBatch(batchId types.BatchID) (core.Batch, error) {
	cacheWithLock := bs.primaryToBatchCacheWithLock[batchId.Primary()]
	cacheWithLock.lock.Lock()
	defer cacheWithLock.lock.Unlock()
	return cacheWithLock.cache.Get(batchId)
}

func (bs *batcherStub) generateBatch(primary types.PartyID, seq *types.BatchSequence) {
	cacheWithLock := bs.primaryToBatchCacheWithLock[primary]
	cacheWithLock.lock.Lock()
	isSameShardPrimarySequence := false
	if cacheWithLock.lastSameShardPrimarySeq != *seq {
		isSameShardPrimarySequence = tossCoin(bs.sameShardPartySeqProb)
		if isSameShardPrimarySequence {
			cacheWithLock.lastSameShardPrimarySeq = *seq
		}
	}
	if !isSameShardPrimarySequence {
		*seq += 1
	}
	batch := bs.batchGenerator.GenerateBatch(bs.shardId, primary, *seq, !isSameShardPrimarySequence)
	cacheWithLock.cache.Put(batch)
	cacheWithLock.lock.Unlock()
	bs.monitor.EventsChan <- &PrefetchBenchEvent{Event: EventBatchGenerated, Data: batch}
	bs.deliveryServiceChan <- batch
	bs.monitor.EventsChan <- &PrefetchBenchEvent{Event: EventBatchDeliveredToAssembler, Data: batch}
	bs.consensusChan <- batch
	bs.monitor.EventsChan <- &PrefetchBenchEvent{Event: EventBatchDeliveredToConsensus, Data: batch}
	bs.logger.Debugf("Generated a new batch %s, sent to Assembler and to Consensus", assembler.BatchToString(batch))
}
