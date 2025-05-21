/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"bytes"
	"fmt"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
)

type batchWithValue[K types.BatchID, V any] struct {
	batch K
	value V
}

// BatchMapper is an efficient generic mapper from types.BatchID derived type to any type.
// Note that shard and party of all the batches need to be the same or else a panic will be raised.
// Not thread safe.
type BatchMapper[K types.BatchID, V any] struct {
	seqToBatches map[types.BatchSequence][]*batchWithValue[K, V]
	partition    ShardPrimary
}

func NewBatchMapper[K types.BatchID, V any](partition ShardPrimary) *BatchMapper[K, V] {
	return &BatchMapper[K, V]{
		seqToBatches: make(map[types.BatchSequence][]*batchWithValue[K, V]),
		partition:    partition,
	}
}

func (m *BatchMapper[K, V]) assertShardAndParty(batchId K) {
	if m.partition.Shard != batchId.Shard() || m.partition.Primary != batchId.Primary() {
		panic(fmt.Sprintf("batch of shard %d and primary %d is not compatible with mapper <%d,%d>", batchId.Shard(), batchId.Primary(), m.partition.Shard, m.partition.Primary))
	}
}

// insert puts batch-value pair and returns if the new value was written successfully.
// Parameter overwrite controls what happens if the batch already exists, true will overwrite the value, false will do nothing.
// Returns true if the operation has succeeded.
func (m *BatchMapper[K, V]) insert(batchId K, value V, overwrite bool) bool {
	m.assertShardAndParty(batchId)
	itemToPut := &batchWithValue[K, V]{
		batch: batchId,
		value: value,
	}
	batchesWithVars, seqExists := m.seqToBatches[batchId.Seq()]
	if !seqExists {
		m.seqToBatches[batchId.Seq()] = []*batchWithValue[K, V]{itemToPut}
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
	m.seqToBatches[batchId.Seq()] = append(batchesWithVars, itemToPut)
	return true
}

func (m *BatchMapper[K, V]) Put(batchId K, value V) {
	m.insert(batchId, value, true)
}

func (m *BatchMapper[K, V]) Insert(batchId K, value V) bool {
	return m.insert(batchId, value, false)
}

func (m *BatchMapper[K, V]) Has(batchId K) bool {
	m.assertShardAndParty(batchId)
	_, err := m.Get(batchId)
	return err == nil
}

func (m *BatchMapper[K, V]) Get(batchId K) (V, error) {
	m.assertShardAndParty(batchId)
	batchesWithVars, seqExists := m.seqToBatches[batchId.Seq()]
	if seqExists {
		for _, batchWithVar := range batchesWithVars {
			if bytes.Equal(batchWithVar.batch.Digest(), batchId.Digest()) {
				return batchWithVar.value, nil
			}
		}
	}
	return *new(V), ErrBatchDoesNotExist
}

func (m *BatchMapper[K, V]) Remove(batchId K) (V, error) {
	m.assertShardAndParty(batchId)
	batchesWithVars, seqExists := m.seqToBatches[batchId.Seq()]
	if seqExists {
		for idx, batchWithVar := range batchesWithVars {
			if bytes.Equal(batchWithVar.batch.Digest(), batchId.Digest()) {
				withoutBatch := append(batchesWithVars[:idx], batchesWithVars[idx+1:]...)
				if len(withoutBatch) == 0 {
					delete(m.seqToBatches, batchId.Seq())
				} else {
					m.seqToBatches[batchId.Seq()] = withoutBatch
				}

				return batchWithVar.value, nil
			}
		}
	}
	return *new(V), ErrBatchDoesNotExist
}
