package assembler

import (
	"arma/common/types"
	"arma/core"
)

type BatchCache struct {
	tag              string
	shardBatchMapper *BatchMapper[types.BatchID, core.Batch]
	partition        ShardPrimary
	sizeBytes        int
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
		shardBatchMapper: NewBatchMapper[types.BatchID, core.Batch](partition),
	}
	return sc
}

func (bc *BatchCache) Pop(batchId types.BatchID) (core.Batch, error) {
	batch, err := bc.shardBatchMapper.Remove(batchId)
	if err != nil {
		return nil, err
	}
	bc.sizeBytes -= batchSizeBytes(batch)
	return batch, nil
}

func (bc *BatchCache) Put(batch core.Batch) error {
	inserted := bc.shardBatchMapper.Insert(batch, batch)
	if !inserted {
		return ErrBatchAlreadyExists
	}
	if inserted {
		bc.sizeBytes += batchSizeBytes(batch)
	}
	return nil
}

func (bc *BatchCache) Has(batchId types.BatchID) bool {
	return bc.shardBatchMapper.Has(batchId)
}

func (bc *BatchCache) Get(batchId types.BatchID) (core.Batch, error) {
	return bc.shardBatchMapper.Get(batchId)
}

func (bc *BatchCache) SizeBytes() int {
	return bc.sizeBytes
}
