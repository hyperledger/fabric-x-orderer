package arma

import (
	"encoding/hex"
	"sync"
	"time"
)

type BatchAttestation interface {
	Fragments() []BatchAttestationFragment
	Digest() []byte
	Seq() uint64
	Primary() uint16
	Shard() uint16
	Serialize() []byte
	Deserialize([]byte) error
}

type BatchAttestationFragment interface {
	Seq() uint64
	Primary() uint16
	Signer() uint16
	Shard() uint16
	Digest() []byte
	Serialize() []byte
	Deserialize([]byte) error
	GarbageCollect() [][]byte
	Epoch() uint64
}

type BatchReplicator interface {
	Replicate(shardID uint16, startSeq uint64) <-chan Batch
}

type AssemblerIndex interface {
	Index(party uint16, shard uint16, sequence uint64, batch Batch)
	Retrieve(party uint16, shard uint16, sequence uint64, digest []byte) (Batch, bool)
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

func (a *Assembler) Run() {
	a.signal = sync.Cond{L: &a.lock}

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
				a.Logger.Infof("Got batch of %d requests for shard %d", len(batch.Requests()), shardID)
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
			a.Logger.Infof("Received attestation with digest %s", hex.EncodeToString(ba.Digest()[:8]))
			t1 := time.Now()
			batch := a.processAttestations(ba)
			a.Logger.Infof("Located batch for digest %s within %v", hex.EncodeToString(ba.Digest()[:8]), time.Since(t1))
			a.Ledger.Append(ba.Seq(), batch, ba)
		}
	}(attestations)

}

func (a *Assembler) processAttestations(ba BatchAttestation) Batch {
	a.lock.Lock()
	defer a.lock.Unlock()

	for {
		t1 := time.Now()
		batch, retrieved := a.Index.Retrieve(ba.Primary(), ba.Shard(), ba.Seq(), ba.Digest())
		if !retrieved {
			a.signal.Wait()
			continue
		}
		a.Logger.Infof("Retrieved batch with %d requests for attestation %s from index within %v", len(batch.Requests()), hex.EncodeToString(ba.Digest()[:8]), time.Since(t1))
		return batch
	}

}
