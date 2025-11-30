/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

//go:generate counterfeiter -o ./mocks/prefetch_index.go . PrefetchIndexer
type PrefetchIndexer interface {
	PopOrWait(batchId types.BatchID) (types.Batch, error)
	Put(batch types.Batch) error
	PutForce(batch types.Batch) error
	Requests() <-chan types.BatchID
	Metrics() *PrefetchIndexMetrics
	Stop()
}

//go:generate counterfeiter -o ./mocks/prefetch_index_factory.go . PrefetchIndexerFactory
type PrefetchIndexerFactory interface {
	Create(
		shards []types.ShardID,
		parties []types.PartyID,
		logger types.Logger,
		defaultTtl time.Duration,
		maxPartitionSizeBytes int,
		requestChannelSize int,
		timerFactory TimerFactory,
		batchCacheFactory BatchCacheFactory,
		partitionPrefetchIndexerFactory PartitionPrefetchIndexerFactory,
		popWaitMonitorTimeout time.Duration,
	) PrefetchIndexer
}

type DefaultPrefetchIndexerFactory struct{}

func (f *DefaultPrefetchIndexerFactory) Create(
	shards []types.ShardID,
	parties []types.PartyID,
	logger types.Logger,
	defaultTtl time.Duration,
	maxPartitionSizeBytes int,
	requestChannelSize int,
	timerFactory TimerFactory,
	batchCacheFactory BatchCacheFactory,
	partitionPrefetchIndexerFactory PartitionPrefetchIndexerFactory,
	popWaitMonitorTimeout time.Duration,
) PrefetchIndexer {
	return NewPrefetchIndex(
		shards,
		parties,
		logger,
		defaultTtl,
		maxPartitionSizeBytes,
		requestChannelSize,
		timerFactory,
		batchCacheFactory,
		partitionPrefetchIndexerFactory,
		popWaitMonitorTimeout,
	)
}

type PrefetchIndexMetrics map[ShardPrimary]*PartitionPrefetchIndexMetrics

type PrefetchIndex struct {
	logger           types.Logger
	partitionToIndex map[ShardPrimary]PartitionPrefetchIndexer
	batchRequestChan chan types.BatchID
	metrics          PrefetchIndexMetrics
}

func NewPrefetchIndex(
	shards []types.ShardID,
	parties []types.PartyID,
	logger types.Logger,
	defaultTtl time.Duration,
	maxPartitionSizeBytes int,
	requestChannelSize int,
	timerFactory TimerFactory,
	batchCacheFactory BatchCacheFactory,
	partitionPrefetchIndexerFactory PartitionPrefetchIndexerFactory,
	popWaitMonitorTimeout time.Duration,
) *PrefetchIndex {
	pi := &PrefetchIndex{
		logger:           logger,
		partitionToIndex: make(map[ShardPrimary]PartitionPrefetchIndexer, len(shards)*len(parties)),
		batchRequestChan: make(chan types.BatchID, requestChannelSize),
		metrics:          make(map[ShardPrimary]*PartitionPrefetchIndexMetrics, len(shards)*len(parties)),
	}
	for _, shardId := range shards {
		for _, partyId := range parties {
			partition := ShardPrimary{Shard: shardId, Primary: partyId}
			pi.partitionToIndex[partition] = partitionPrefetchIndexerFactory.Create(
				partition,
				logger,
				defaultTtl,
				maxPartitionSizeBytes,
				timerFactory,
				batchCacheFactory,
				pi.batchRequestChan,
				popWaitMonitorTimeout,
			)
			pi.metrics[partition] = pi.partitionToIndex[partition].Metrics()
		}
	}
	return pi
}

func (pi *PrefetchIndex) Metrics() *PrefetchIndexMetrics {
	return &pi.metrics
}

func (pi *PrefetchIndex) PopOrWait(batchId types.BatchID) (types.Batch, error) {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex PopOrWait %s in %v", BatchToString(batchId), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batchId)]
	return partitionIndex.PopOrWait(batchId)
}

func (pi *PrefetchIndex) Put(batch types.Batch) error {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex Put %s in %v", BatchToString(batch), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batch)]
	return partitionIndex.Put(batch)
}

func (pi *PrefetchIndex) PutForce(batch types.Batch) error {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex PutForce with force %s in %v", BatchToString(batch), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batch)]
	return partitionIndex.PutForce(batch)
}

func (pi *PrefetchIndex) Requests() <-chan types.BatchID {
	return pi.batchRequestChan
}

func (pi *PrefetchIndex) Stop() {
	for _, index := range pi.partitionToIndex {
		index.Stop()
	}
}
