package arma

import (
	"sync"
)

type BatchAttestation interface {
	VerifyBatch([]byte) error
	Seq() uint64
	Party() uint16
	Shard() uint16
	Digest() []byte
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
	Logger                     Logger
	BatchAttestationReplicator BatchAttestationReplicator
	Replicator                 BatchReplicator
	Index                      AssemblerIndex
	signal                     sync.Cond
	lock                       sync.RWMutex
}

func (a *Assembler) run() {
	var replicationSources []<-chan Batch

	for shardID := 0; shardID < a.ShardCount; shardID++ {
		batches := a.Replicator.Replicate(uint16(shardID), 0)
		replicationSources = append(replicationSources, batches)
	}

	for shardID := 0; shardID < a.ShardCount; shardID++ {
		go func(shardID uint16) {
			var seq uint64
			batches := replicationSources[int(shardID)]
			for batch := range batches {
				a.lock.RLock()
				a.Index.Index(batch.Party(), shardID, seq, batch)
				a.signal.Signal()
				a.lock.RUnlock()
				seq++
			}

		}(uint16(shardID))
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
