/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"sync/atomic"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type BatchCache struct {
	tag              string
	shardBatchMapper *BatchMapper[types.BatchID, types.Batch]
	partition        ShardPrimary
	sizeBytes        uint64
}

//go:generate counterfeiter -o ./mocks/batch_cache_factory.go . BatchCacheFactory
type BatchCacheFactory interface {
	CreateWithTag(partition ShardPrimary, tag string) *BatchCache
	Create(partition ShardPrimary) *BatchCache
}

type DefaultBatchCacheFactory struct{}

func (dbcf *DefaultBatchCacheFactory) CreateWithTag(partition ShardPrimary, tag string) *BatchCache {
	return NewBatchCache(partition, tag)
}

func (dbcf *DefaultBatchCacheFactory) Create(partition ShardPrimary) *BatchCache {
	return dbcf.CreateWithTag(partition, "")
}

func NewBatchCache(partition ShardPrimary, tag string) *BatchCache {
	sc := &BatchCache{
		tag:              tag,
		partition:        partition,
		shardBatchMapper: NewBatchMapper[types.BatchID, types.Batch](partition),
	}
	return sc
}

func (bc *BatchCache) Pop(batchId types.BatchID) (types.Batch, error) {
	batch, err := bc.shardBatchMapper.Remove(batchId)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&bc.sizeBytes, ^(uint64(batchSizeBytes(batch)) - 1))
	return batch, nil
}

func (bc *BatchCache) Put(batch types.Batch) error {
	inserted := bc.shardBatchMapper.Insert(batch, batch)
	if !inserted {
		return ErrBatchAlreadyExists
	}
	if inserted {
		atomic.AddUint64(&bc.sizeBytes, uint64(batchSizeBytes(batch)))
	}
	return nil
}

func (bc *BatchCache) Has(batchId types.BatchID) bool {
	return bc.shardBatchMapper.Has(batchId)
}

func (bc *BatchCache) Get(batchId types.BatchID) (types.Batch, error) {
	return bc.shardBatchMapper.Get(batchId)
}

func (bc *BatchCache) SizeBytes() int {
	return int(atomic.LoadUint64(&bc.sizeBytes))
}
