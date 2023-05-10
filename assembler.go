package arma

import (
	"sync"
)

type BatchAttestation interface {
	VerifyBatch([]byte) error
	Seq() uint64
}

type AssemblerIndex interface {
	Index(party uint16, shard uint16, sequence uint64, batch Batch)
	Retrieve(party uint16, shard uint16, sequence uint64) (Batch, bool)
}

type AssemblerLedger interface {
	Append(uint64, Batch, BatchAttestation)
}

type BatchAttestationReplicator interface {
	Replicate(uint64) <-chan BatchAttestation
}

type Assembler struct {
	ShardCount                 int
	BatchAttestationReplicator BatchAttestationReplicator
	Replicator                 BatchReplicator
	Index                      AssemblerIndex
	batchesFromShard           []chan []byte
	signal                     sync.Cond
	lock                       sync.Mutex
}

func (a *Assembler) run() {
	for shardID := 0; shardID < a.ShardCount; shardID++ {
		c := make(chan []byte, 20)
		a.batchesFromShard = append(a.batchesFromShard, c)
		go func(shardID uint16, c <-chan []byte) {
			var seq uint64
			batches := a.Replicator.Replicate(shardID, seq)

			for batch := range batches {
				a.lock.Lock()
				a.Index.Index(batch.Party(), shardID, seq, batch)
				a.signal.Signal()
				a.lock.Unlock()
				seq++
			}

		}(uint16(shardID), c)
	}

	attestations := a.BatchAttestationReplicator.Replicate(0)

}
