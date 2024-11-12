package assembler

import (
	"bytes"
	"fmt"

	"arma/common/types"
)

type batchWithValue[K types.BatchID, V any] struct {
	batch K
	value V
}

// ShardBatchMapper is an efficient generic mapper from types.BatchID derived type to any type.
// Note that shard of all the batches need to be the same or else a panic will be raised.
// Not thread safe.
type ShardBatchMapper[K types.BatchID, V any] struct {
	primaryToSeqToBatches map[types.PartyID]map[types.BatchSequence][]*batchWithValue[K, V]
	shardId               types.ShardID
}

func NewShardBatchMapper[K types.BatchID, V any](shardId types.ShardID) *ShardBatchMapper[K, V] {
	return &ShardBatchMapper[K, V]{
		primaryToSeqToBatches: make(map[types.PartyID]map[types.BatchSequence][]*batchWithValue[K, V]),
		shardId:               shardId,
	}
}

func (sbm *ShardBatchMapper[K, V]) assertShard(batchId K) {
	if sbm.shardId != batchId.Shard() {
		panic(fmt.Sprintf("batch of shard %d is not compatible with cache shard %d", batchId.Shard(), sbm.shardId))
	}
}

// insert puts batch-value pair and returns if the action.
// If overwrite controls what happens if the batch is already exists, true will overwrite the value, false will do nothing.
// Returns true if the operation has succeeded.
func (sbm *ShardBatchMapper[K, V]) insert(batchId K, value V, overwrite bool) bool {
	sbm.assertShard(batchId)
	itemToPut := &batchWithValue[K, V]{
		batch: batchId,
		value: value,
	}
	seqToBatches, primaryExists := sbm.primaryToSeqToBatches[batchId.Primary()]
	if !primaryExists {
		seqToBatches = make(map[types.BatchSequence][]*batchWithValue[K, V])
		sbm.primaryToSeqToBatches[batchId.Primary()] = seqToBatches
	}
	batchesWithVars, seqExists := seqToBatches[batchId.Seq()]
	if !seqExists {
		seqToBatches[batchId.Seq()] = []*batchWithValue[K, V]{itemToPut}
		return true
	}
	for _, batchWithVar := range batchesWithVars {
		if bytes.Equal(batchWithVar.batch.Digest(), batchId.Digest()) {
			if !overwrite {
				return false
			}
			batchWithVar.value = value
			return true
		}
	}
	seqToBatches[batchId.Seq()] = append(batchesWithVars, itemToPut)
	return true
}

func (sbm *ShardBatchMapper[K, V]) Put(batchId K, value V) {
	sbm.insert(batchId, value, true)
}

func (sbm *ShardBatchMapper[K, V]) Insert(batchId K, value V) bool {
	return sbm.insert(batchId, value, false)
}

func (sbm *ShardBatchMapper[K, V]) Has(batchId K) bool {
	sbm.assertShard(batchId)
	_, err := sbm.Get(batchId)
	return err == nil
}

func (sbm *ShardBatchMapper[K, V]) Get(batchId K) (V, error) {
	sbm.assertShard(batchId)
	seqToBatches, primaryExists := sbm.primaryToSeqToBatches[batchId.Primary()]
	if primaryExists {
		batchesWithVars, seqExists := seqToBatches[batchId.Seq()]
		if seqExists {
			for _, batchWithVar := range batchesWithVars {
				if bytes.Equal(batchWithVar.batch.Digest(), batchId.Digest()) {
					return batchWithVar.value, nil
				}
			}
		}
	}
	return *new(V), ErrBatchNotExists
}

func (sbm *ShardBatchMapper[K, V]) Remove(batchId K) (V, error) {
	sbm.assertShard(batchId)
	seqToBatches, primaryExists := sbm.primaryToSeqToBatches[batchId.Primary()]
	if primaryExists {
		batchesWithVars, seqExists := seqToBatches[batchId.Seq()]
		if seqExists {
			for idx, batchWithVar := range batchesWithVars {
				if bytes.Equal(batchWithVar.batch.Digest(), batchId.Digest()) {
					withoutBatch := append(batchesWithVars[:idx], batchesWithVars[idx+1:]...)
					if len(withoutBatch) == 0 {
						delete(seqToBatches, batchId.Seq())
					} else {
						seqToBatches[batchId.Seq()] = withoutBatch
					}

					return batchWithVar.value, nil
				}
			}
		}
	}
	return *new(V), ErrBatchNotExists
}
