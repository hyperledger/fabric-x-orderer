package core_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/testutil"

	"github.com/stretchr/testify/assert"
)

type naiveOrderedBatchAttestationReplicator chan core.OrderedBatchAttestation

func (n naiveOrderedBatchAttestationReplicator) Replicate(u uint64) <-chan core.OrderedBatchAttestation {
	return n
}

type naiveIndex struct {
	sync.Map
}

func (n *naiveIndex) Index(_ types.PartyID, _ types.ShardID, _ types.BatchSequence, batch core.Batch) {
	n.Store(string(batch.Digest()), batch)
}

func (n *naiveIndex) Retrieve(_ types.PartyID, _ types.ShardID, _ types.BatchSequence, digest []byte) (core.Batch, bool) {
	val, exists := n.Load(string(digest))

	if !exists {
		return nil, false
	}

	defer func() {
		n.Delete(string(digest))
	}()
	return val.(core.Batch), true
}

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

func (nba *naiveBatchAttestation) Fragments() []core.BatchAttestationFragment {
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

type naiveOrderedBatchAttestation struct {
	ba           core.BatchAttestation
	orderingInfo int
}

func (noba *naiveOrderedBatchAttestation) BatchAttestation() core.BatchAttestation {
	return noba.ba
}

func (noba *naiveOrderedBatchAttestation) OrderingInfo() interface{} {
	return noba.orderingInfo
}

type naiveAssemblerLedger chan core.BatchAttestation

func (n naiveAssemblerLedger) Append(_ uint64, _ core.Batch, attestation core.BatchAttestation) {
	n <- attestation
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
	shardCount := 10
	batchNum := 100

	digests := make(map[string]struct{})

	var batches [][]core.Batch
	for shardID := 0; shardID < shardCount; shardID++ {
		var batchesForShard []core.Batch
		for j := 0; j < batchNum; j++ {
			buff := make([]byte, 1024)
			binary.BigEndian.PutUint16(buff, uint16(shardID))
			binary.BigEndian.PutUint16(buff[100:], uint16(j))
			batch := &naiveBatch{
				requests: [][]byte{buff},
			}
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
				totalOrder <- &naiveBatchAttestation{
					digest: batch.Digest(),
				}
			}
		}(shardID)
	}

	go func() {
		var seq uint64

		for nba := range totalOrder {
			nba.seq = types.BatchSequence(seq)
			seq++
			noba := &naiveOrderedBatchAttestation{
				ba:           nba,
				orderingInfo: 0,
			}
			nobar <- noba
		}
	}()

	for i := 0; i < 1000; i++ {
		block := <-ledger
		assert.Equal(t, types.BatchSequence(i), block.Seq())
		delete(digests, string(block.Digest()))
	}

	assert.Len(t, digests, 0)
}

func createAssembler(t *testing.T, shardCount int) (*naiveReplication, naiveAssemblerLedger, naiveOrderedBatchAttestationReplicator, *core.Assembler) {
	index := &naiveIndex{}

	r := &naiveReplication{}

	var shards []types.ShardID
	for i := 0; i < shardCount; i++ {
		r.subscribers = append(r.subscribers, make(chan core.Batch, 100))
		shards = append(shards, types.ShardID(i))
	}

	ledger := make(naiveAssemblerLedger, 10)

	nobar := make(naiveOrderedBatchAttestationReplicator)

	assembler := &core.Assembler{
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
