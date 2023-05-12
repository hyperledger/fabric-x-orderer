package arma

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type naiveBatchAttestationReplicator chan BatchAttestation

func (n naiveBatchAttestationReplicator) Replicate(u uint64) <-chan BatchAttestation {
	return n
}

type naiveIndex struct {
	sync.Map
}

func (n *naiveIndex) Index(_ uint16, _ uint16, _ uint64, batch Batch) {
	n.Store(string(batch.Digest()), batch)
}

func (n *naiveIndex) Retrieve(_ uint16, _ uint16, _ uint64, digest []byte) (Batch, bool) {
	val, exists := n.Load(string(digest))

	if !exists {
		return nil, false
	}

	return val.(Batch), true
}

type naiveBatchAttestation struct {
	seq    uint64
	node   uint16
	shard  uint16
	digest []byte
}

func (nba *naiveBatchAttestation) VerifyBatch(bytes []byte) error {
	return nil
}

func (nba *naiveBatchAttestation) Seq() uint64 {
	return nba.seq
}

func (nba *naiveBatchAttestation) Party() uint16 {
	return nba.node
}

func (nba *naiveBatchAttestation) Shard() uint16 {
	return nba.shard
}

func (nba *naiveBatchAttestation) Digest() []byte {
	return nba.digest
}

type naiveAssemblerLedger chan BatchAttestation

func (n naiveAssemblerLedger) Append(_ uint64, _ Batch, attestation BatchAttestation) {
	n <- attestation
}

func TestAssembler(t *testing.T) {
	shardCount := 10
	batchNum := 100

	digests := make(map[string]struct{})

	var batches [][]Batch
	for shardID := 0; shardID < shardCount; shardID++ {
		var batchesForShard []Batch
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

	index := &naiveIndex{}

	r := &naiveReplication{}

	for i := 0; i < shardCount; i++ {
		r.subscribers = append(r.subscribers, make(chan Batch, 100))
	}

	ledger := make(naiveAssemblerLedger, 10)

	nbar := make(naiveBatchAttestationReplicator)

	assembler := &Assembler{
		Logger:                     createLogger(t, 0),
		Replicator:                 r,
		Ledger:                     ledger,
		ShardCount:                 shardCount,
		BatchAttestationReplicator: nbar,
		Index:                      index,
	}
	assembler.signal = sync.Cond{L: &assembler.lock}

	assembler.run()

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
			nba.seq = seq
			seq++
			nbar <- nba
		}
	}()

	for i := 0; i < 1000; i++ {
		block := <-ledger
		assert.Equal(t, uint64(i), block.Seq())
		delete(digests, string(block.Digest()))
	}

	assert.Len(t, digests, 0)

}
