/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/assert"
)

type naiveOrderedBatchAttestationReplicator chan types.OrderedBatchAttestation

func (n naiveOrderedBatchAttestationReplicator) Replicate() <-chan types.OrderedBatchAttestation {
	return n
}

type naiveReplication struct {
	subscribers []chan types.Batch
	i           uint32
	stopped     int32
}

func (r *naiveReplication) Replicate(_ types.ShardID) <-chan types.Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) PullBatches(_ types.PartyID) <-chan types.Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) Stop() {
	atomic.StoreInt32(&r.stopped, 0x1)
}

func (r *naiveReplication) Append(partyID types.PartyID, batchSeq types.BatchSequence, batchedRequests types.BatchedRequests) {
	for _, s := range r.subscribers {
		s <- types.NewSimpleBatch(batchSeq, 0, partyID, batchedRequests)
	}
}

func (r *naiveReplication) Height(partyID types.PartyID) uint64 {
	// TODO use in test
	return 0
}

func (r *naiveReplication) RetrieveBatchByNumber(partyID types.PartyID, seq uint64) types.Batch {
	// TODO use in test
	return nil
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

type naiveBatchAttestation struct {
	primary types.PartyID
	seq     types.BatchSequence
	node    types.PartyID
	shard   types.ShardID
	digest  []byte
}

func (nba *naiveBatchAttestation) Epoch() uint64 {
	return 0
}

func (nba *naiveBatchAttestation) GarbageCollect() [][]byte {
	return nil
}

func (nba *naiveBatchAttestation) Signer() types.PartyID {
	return nba.node
}

func (nba *naiveBatchAttestation) Primary() types.PartyID {
	return nba.primary
}

func (nba *naiveBatchAttestation) Seq() types.BatchSequence {
	return nba.seq
}

func (nba *naiveBatchAttestation) Shard() types.ShardID {
	return nba.shard
}

func (nba *naiveBatchAttestation) Digest() []byte {
	return nba.digest
}

func (nba *naiveBatchAttestation) Fragments() []types.BatchAttestationFragment {
	return nil
}

func (nba *naiveBatchAttestation) Serialize() []byte {
	m := make(map[string]interface{})
	m["seq"] = nba.seq
	m["node"] = nba.node
	m["shard"] = nba.shard
	m["digest"] = hex.EncodeToString(nba.digest)

	bytes, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return bytes
}

func (nba *naiveBatchAttestation) Deserialize(bytes []byte) error {
	m := make(map[string]interface{})
	if err := json.Unmarshal(bytes, &m); err != nil {
		panic(err)
	}

	node := m["node"]
	shard := m["shard"]
	seq := m["seq"]
	dig := m["digest"]

	nba.node = types.PartyID(node.(float64))
	nba.shard = types.ShardID(shard.(float64))
	nba.seq = types.BatchSequence(seq.(float64))
	nba.digest, _ = hex.DecodeString(dig.(string))

	return nil
}

type naiveOrderingInfo struct {
	num uint64
}

func (noi *naiveOrderingInfo) String() string {
	return fmt.Sprintf("BlockNumber: %d", noi.num)
}

type naiveOrderedBatchAttestation struct {
	ba           types.BatchAttestation
	orderingInfo types.OrderingInfo
}

func (noba *naiveOrderedBatchAttestation) BatchAttestation() types.BatchAttestation {
	return noba.ba
}

func (noba *naiveOrderedBatchAttestation) OrderingInfo() types.OrderingInfo {
	return noba.orderingInfo
}

type naiveAssemblerLedger chan types.OrderedBatchAttestation

func (n naiveAssemblerLedger) Append(batch types.Batch, orderingInfo types.OrderingInfo) {
	noba := &naiveOrderedBatchAttestation{
		ba: &naiveBatchAttestation{
			primary: batch.Primary(),
			seq:     batch.Seq(),
			node:    batch.Primary(),
			shard:   batch.Shard(),
			digest:  batch.Digest(),
		},
		orderingInfo: orderingInfo,
	}
	n <- noba
}

func (n naiveAssemblerLedger) Close() {
	close(n)
}

func TestNaive(t *testing.T) {
	nba := &naiveBatchAttestation{}
	nba.seq = 100
	nba.node = 1
	nba.digest = []byte{1, 2, 3, 4, 5, 6, 7, 8}

	nba2 := &naiveBatchAttestation{}
	nba2.Deserialize(nba.Serialize())
	assert.Equal(t, nba, nba2)
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
			batch := types.NewSimpleBatch(seq, shardID, 1, [][]byte{buff})
			digests[string(batch.Digest())] = struct{}{}
			batchesForShard = append(batchesForShard, batch)
		}
		batches = append(batches, batchesForShard)
	}

	r, ledger, nobar, assembler := createAssembler(t, shardCount)

	assembler.Run()

	// Wait for all subscribers of all shards to be connected
	for atomic.LoadUint32(&r.i) < uint32(shardCount) {
		time.Sleep(time.Millisecond)
	}

	totalOrder := make(chan *naiveBatchAttestation)

	for shardID := 0; shardID < shardCount; shardID++ {
		go func(shard int) {
			for _, batch := range batches[shard] {

				r.subscribers[shard] <- batch

				nba := &naiveBatchAttestation{
					primary: batch.Primary(),
					seq:     batch.Seq(),
					shard:   batch.Shard(),
					digest:  batch.Digest(),
				}
				totalOrder <- nba
			}
		}(shardID)
	}

	go func() {
		var num uint64

		for nba := range totalOrder {
			noba := &naiveOrderedBatchAttestation{
				ba:           nba,
				orderingInfo: &naiveOrderingInfo{num: num},
			}
			nobar <- noba
			num++
		}
	}()

	for i := uint64(0); i < uint64(batchNum*shardCount); i++ {
		noba := <-ledger
		assert.Equal(t, fmt.Sprintf("BlockNumber: %d", i), noba.OrderingInfo().String())
		delete(digests, string(noba.BatchAttestation().Digest()))
	}

	assert.Len(t, digests, 0)
}

func createAssembler(t *testing.T, shardCount int) (*naiveReplication, naiveAssemblerLedger, naiveOrderedBatchAttestationReplicator, *assembler.AssemblerRole) {
	index := &naiveIndex{}

	r := &naiveReplication{}

	var shards []types.ShardID
	for i := 0; i < shardCount; i++ {
		r.subscribers = append(r.subscribers, make(chan types.Batch, 100))
		shards = append(shards, types.ShardID(i))
	}

	ledger := make(naiveAssemblerLedger, 10)

	nobar := make(naiveOrderedBatchAttestationReplicator)

	assembler := &assembler.AssemblerRole{
		Shards:                            shards,
		Logger:                            testutil.CreateLogger(t, 0),
		Replicator:                        r,
		Ledger:                            ledger,
		ShardCount:                        shardCount,
		OrderedBatchAttestationReplicator: nobar,
		Index:                             index,
	}
	return r, ledger, nobar, assembler
}
