package core_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"arma/core"
	"arma/testutil"

	"github.com/stretchr/testify/assert"
)

type naiveBatchAttestationReplicator chan core.BatchAttestation

func (n naiveBatchAttestationReplicator) Replicate(u uint64) <-chan core.BatchAttestation {
	return n
}

type naiveIndex struct {
	sync.Map
}

func (n *naiveIndex) Index(_ core.PartyID, _ core.ShardID, _ core.BatchSequence, batch core.Batch) {
	n.Store(string(batch.Digest()), batch)
}

func (n *naiveIndex) Retrieve(_ core.PartyID, _ core.ShardID, _ core.BatchSequence, digest []byte) (core.Batch, bool) {
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
	primary core.PartyID
	seq     core.BatchSequence
	node    core.PartyID
	shard   core.ShardID
	digest  []byte
}

func (nba *naiveBatchAttestation) Epoch() uint64 {
	return 0
}

func (nba *naiveBatchAttestation) GarbageCollect() [][]byte {
	return nil
}

func (nba *naiveBatchAttestation) Signer() core.PartyID {
	return nba.node
}

func (nba *naiveBatchAttestation) Primary() core.PartyID {
	return nba.primary
}

func (nba *naiveBatchAttestation) Seq() core.BatchSequence {
	return nba.seq
}

func (nba *naiveBatchAttestation) Shard() core.ShardID {
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

	seq := m["seq"]
	dig := m["digest"]

	nba.seq = core.BatchSequence(seq.(float64))
	nba.digest, _ = hex.DecodeString(dig.(string))
	return nil
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
	nba.Deserialize(nba.Serialize())
	fmt.Println(nba2)
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

	r, ledger, nbar, assembler := createAssembler(t, shardCount)

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
			nba.seq = core.BatchSequence(seq)
			seq++
			nbar <- nba
		}
	}()

	for i := 0; i < 1000; i++ {
		block := <-ledger
		assert.Equal(t, core.BatchSequence(i), block.Seq())
		delete(digests, string(block.Digest()))
	}

	assert.Len(t, digests, 0)
}

func createAssembler(t *testing.T, shardCount int) (*naiveReplication, naiveAssemblerLedger, naiveBatchAttestationReplicator, *core.Assembler) {
	index := &naiveIndex{}

	r := &naiveReplication{}

	var shards []core.ShardID
	for i := 0; i < shardCount; i++ {
		r.subscribers = append(r.subscribers, make(chan core.Batch, 100))
		shards = append(shards, core.ShardID(i))
	}

	ledger := make(naiveAssemblerLedger, 10)

	nbar := make(naiveBatchAttestationReplicator)

	assembler := &core.Assembler{
		Shards:                     shards,
		Logger:                     testutil.CreateLogger(t, 0),
		Replicator:                 r,
		Ledger:                     ledger,
		ShardCount:                 shardCount,
		BatchAttestationReplicator: nbar,
		Index:                      index,
	}
	return r, ledger, nbar, assembler
}
