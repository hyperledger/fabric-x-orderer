/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

//go:generate counterfeiter -o ./mocks/prefetch_index.go . PrefetchIndexer
type PrefetchIndexer interface {
	PopOrWait(batchId types.BatchID) (types.Batch, error)
	Put(batch types.Batch) error
	PutForce(batch types.Batch) error
	Requests() <-chan types.BatchID
	Stop()
}

//go:generate counterfeiter -o ./mocks/prefetch_index_factory.go . PrefetchIndexerFactory
type PrefetchIndexerFactory interface {
	Create(
		shards []types.ShardID,
		parties []types.PartyID,
		logger *flogging.FabricLogger,
		defaultTtl time.Duration,
		maxPartitionSizeBytes int,
		requestChannelSize int,
		timerFactory TimerFactory,
		batchCacheFactory BatchCacheFactory,
		partitionPrefetchIndexerFactory PartitionPrefetchIndexerFactory,
		popWaitMonitorTimeout time.Duration,
		metrics *Metrics,
	) PrefetchIndexer
}

type DefaultPrefetchIndexerFactory struct{}

func (f *DefaultPrefetchIndexerFactory) Create(
	shards []types.ShardID,
	parties []types.PartyID,
	logger *flogging.FabricLogger,
	defaultTtl time.Duration,
	maxPartitionSizeBytes int,
	requestChannelSize int,
	timerFactory TimerFactory,
	batchCacheFactory BatchCacheFactory,
	partitionPrefetchIndexerFactory PartitionPrefetchIndexerFactory,
	popWaitMonitorTimeout time.Duration,
	metrics *Metrics,
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
		metrics,
	)
}

type PrefetchIndex struct {
	logger           *flogging.FabricLogger
	partitionToIndex map[ShardPrimary]PartitionPrefetchIndexer
	batchRequestChan chan types.BatchID
	metrics          *Metrics
}

func NewPrefetchIndex(
	shards []types.ShardID,
	parties []types.PartyID,
	logger *flogging.FabricLogger,
	defaultTtl time.Duration,
	maxPartitionSizeBytes int,
	requestChannelSize int,
	timerFactory TimerFactory,
	batchCacheFactory BatchCacheFactory,
	partitionPrefetchIndexerFactory PartitionPrefetchIndexerFactory,
	popWaitMonitorTimeout time.Duration,
	metrics *Metrics,
) *PrefetchIndex {
	pi := &PrefetchIndex{
		logger:           logger,
		partitionToIndex: make(map[ShardPrimary]PartitionPrefetchIndexer, len(shards)*len(parties)),
		batchRequestChan: make(chan types.BatchID, requestChannelSize),
		metrics:          metrics,
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
				metrics,
			)
		}
	}
	return pi
}

func (pi *PrefetchIndex) PopOrWait(batchId types.BatchID) (types.Batch, error) {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex PopOrWait %s in %v", BatchToString(batchId), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batchId)]
	batch, err := partitionIndex.PopOrWait(batchId)
	if err == nil {
		pi.metrics.updatePrefetchIndexSize(batch.Shard(), -batchSizeBytes(batch))
	}
	return batch, err
}

func (pi *PrefetchIndex) Put(batch types.Batch) error {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex Put %s in %v", BatchToString(batch), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batch)]
	err := partitionIndex.Put(batch)
	if err == nil {
		pi.metrics.updatePrefetchIndexSize(batch.Shard(), batchSizeBytes(batch))
	}
	return err
}

func (pi *PrefetchIndex) PutForce(batch types.Batch) error {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex PutForce with force %s in %v", BatchToString(batch), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batch)]
	err := partitionIndex.PutForce(batch)
	if err == nil {
		pi.metrics.updatePrefetchIndexSize(batch.Shard(), batchSizeBytes(batch))
	}
	return err
}

func (pi *PrefetchIndex) Requests() <-chan types.BatchID {
	return pi.batchRequestChan
}

func (pi *PrefetchIndex) Stop() {
	shards := make(map[types.ShardID]struct{})

	for partition, index := range pi.partitionToIndex {
		index.Stop()
		shards[partition.Shard] = struct{}{}
	}

	for shardID := range shards {
		pi.metrics.resetPrefetchIndexSize(shardID)
	}
}
