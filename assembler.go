package arma

import (
	"sync"
)

type BatchAttestation interface {
	VerifyBatch([]byte) error
	Seq() uint64
	Party() uint16
	Shard() uint16
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
	Ledger                     AssemblerLedger
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
	go func(attestations <-chan BatchAttestation) {
		for ba := range attestations {
			batch := a.processAttestations(ba)
			a.Ledger.Append(ba.Seq(), batch, ba)
		}
	}(attestations)

}

func (a *Assembler) processAttestations(ba BatchAttestation) Batch {
	a.lock.Lock()
	defer a.lock.Unlock()

	for {
		batch, retrieved := a.Index.Retrieve(ba.Party(), ba.Shard(), ba.Seq())
		if !retrieved {
			a.signal.Wait()
			continue
		}
		return batch
	}

}
