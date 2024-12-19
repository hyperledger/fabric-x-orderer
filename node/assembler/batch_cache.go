package assembler

import (
	"arma/common/types"
	"arma/core"
)

type BatchCache struct {
	shardBatchMapper *BatchMapper[types.BatchID, core.Batch]
	shardId          types.ShardID
	partyId          types.PartyID
	sizeBytes        int
}

//go:generate counterfeiter -o ./mocks/batch_cache_factory.go . BatchCacheFactory
type BatchCacheFactory interface {
	Create(shardId types.ShardID, partyId types.PartyID) *BatchCache
}

func NewBatchCache(shardId types.ShardID, partyId types.PartyID) *BatchCache {
	sc := &BatchCache{
		shardId:          shardId,
		partyId:          partyId,
		shardBatchMapper: NewBatchMapper[types.BatchID, core.Batch](shardId, partyId),
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

func (bc *BatchCache) SizeBytes() int {
	return bc.sizeBytes
}
