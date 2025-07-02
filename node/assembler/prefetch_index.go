/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
)

//go:generate counterfeiter -o ./mocks/prefetch_index.go . PrefetchIndexer
type PrefetchIndexer interface {
	PopOrWait(batchId types.BatchID) (core.Batch, error)
	Put(batch core.Batch) error
	PutForce(batch core.Batch) error
	Requests() <-chan types.BatchID
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
	)
}

type PrefetchIndex struct {
	logger           types.Logger
	partitionToIndex map[ShardPrimary]PartitionPrefetchIndexer
	batchRequestChan chan types.BatchID
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
) *PrefetchIndex {
	pi := &PrefetchIndex{
		logger:           logger,
		partitionToIndex: make(map[ShardPrimary]PartitionPrefetchIndexer, len(shards)*len(parties)),
		batchRequestChan: make(chan types.BatchID, requestChannelSize),
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
			)
		}
	}
	return pi
}

func (pi *PrefetchIndex) PopOrWait(batchId types.BatchID) (core.Batch, error) {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex PopOrWait %s in %v", BatchToString(batchId), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batchId)]
	return partitionIndex.PopOrWait(batchId)
}

func (pi *PrefetchIndex) Put(batch core.Batch) error {
	t1 := time.Now()
	defer func() {
		pi.logger.Debugf("PrefetchIndex Put %s in %v", BatchToString(batch), time.Since(t1))
	}()
	partitionIndex := pi.partitionToIndex[ShardPrimaryFromBatch(batch)]
	return partitionIndex.Put(batch)
}

func (pi *PrefetchIndex) PutForce(batch core.Batch) error {
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
