/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/assert"
)

type naiveOrderedBatchAttestationReplicator chan types.OrderedBatchAttestation

func (n naiveOrderedBatchAttestationReplicator) Replicate() <-chan types.OrderedBatchAttestation {
	return n
}

type naiveIndex struct {
	sync.Map
}

func (n *naiveIndex) Put(batch types.Batch) error {
	n.Store(string(batch.Digest()), batch)
	return nil
}

func (n *naiveIndex) PopOrWait(batchId types.BatchID) (types.Batch, error) {
	for {
		val, exists := n.Load(string(batchId.Digest()))

		if !exists {
			time.Sleep(time.Millisecond)
			continue
		}
		defer func() {
			n.Delete(string(batchId.Digest()))
		}()
		return val.(types.Batch), nil
	}
}

func (n *naiveIndex) Stop() {}

type naiveAssemblerLedger chan types.OrderedBatchAttestation

func (n naiveAssemblerLedger) Append(batch types.Batch, orderingInfo types.OrderingInfo) {
	noba := &state.AvailableBatchOrdered{
		AvailableBatch:      state.NewAvailableBatch(batch.Primary(), batch.Shard(), batch.Seq(), batch.Digest()),
		OrderingInformation: orderingInfo.(*state.OrderingInformation),
	}

	n <- noba
}

func (n naiveAssemblerLedger) Close() {
	close(n)
}

func TestAssembler(t *testing.T) {
	shardCount := 4
	batchNum := 20

	digests := make(map[string]struct{})

	var batches [][]types.Batch
	for shardID := types.ShardID(0); shardID < types.ShardID(shardCount); shardID++ {
		var batchesForShard []types.Batch
		for seq := types.BatchSequence(0); seq < types.BatchSequence(batchNum); seq++ {
			buff := make([]byte, 1024)
			binary.BigEndian.PutUint16(buff, uint16(shardID))
			binary.BigEndian.PutUint16(buff[100:], uint16(seq))
			batch := types.NewSimpleBatch(shardID, 1, seq, [][]byte{buff}, 0)
			digests[string(batch.Digest())] = struct{}{}
			batchesForShard = append(batchesForShard, batch)
		}
		batches = append(batches, batchesForShard)
	}

	index, ledger, nobar, assembler := createAssemblerRole(t, shardCount)

	assembler.Run()

	totalOrder := make(chan *state.AvailableBatch)

	for shardID := 0; shardID < shardCount; shardID++ {
		go func(shard int) {
			for _, batch := range batches[shard] {
				index.Put(batch)
				nba := state.NewAvailableBatch(batch.Primary(), batch.Shard(), batch.Seq(), batch.Digest())
				totalOrder <- nba
			}
		}(shardID)
	}

	go func() {
		var num uint64

		for nba := range totalOrder {
			noba := &state.AvailableBatchOrdered{
				AvailableBatch: nba,
				OrderingInformation: &state.OrderingInformation{
					BlockHeader: &state.BlockHeader{Number: num, PrevHash: []byte{0x08}, Digest: []byte{0x09}},
					DecisionNum: types.DecisionNum(num),
					BatchIndex:  0,
					BatchCount:  1,
				},
			}
			nobar <- noba
			num++
		}
	}()

	for i := uint64(0); i < uint64(batchNum*shardCount); i++ {
		noba := <-ledger
		assert.Equal(t, fmt.Sprintf("DecisionNum: %d, BatchIndex: 0, BatchCount: 1; No. Sigs: 0, BlockHeader: Number: %d, PrevHash: 08, Digest: 09, Common Block: <nil>", i, i), noba.OrderingInfo().String())
		delete(digests, string(noba.BatchAttestation().Digest()))
	}

	assert.Len(t, digests, 0)
}

func createAssemblerRole(t *testing.T, shardCount int) (
	*naiveIndex,
	naiveAssemblerLedger,
	naiveOrderedBatchAttestationReplicator,
	*assembler.AssemblerRole,
) {
	index := &naiveIndex{}

	var shards []types.ShardID
	for i := 0; i < shardCount; i++ {
		shards = append(shards, types.ShardID(i))
	}

	ledger := make(naiveAssemblerLedger, 10)

	nobar := make(naiveOrderedBatchAttestationReplicator)

	assembler := &assembler.AssemblerRole{
		Shards:                            shards,
		Logger:                            testutil.CreateLogger(t, 0),
		Ledger:                            ledger,
		ShardCount:                        shardCount,
		OrderedBatchAttestationReplicator: nobar,
		Index:                             index,
	}
	return index, ledger, nobar, assembler
}
