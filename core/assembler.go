package core

import (
	"encoding/hex"
	"sync"
	"time"
)

//go:generate counterfeiter -o mocks/batch_attestation.go . BatchAttestation
type BatchAttestation interface {
	Fragments() []BatchAttestationFragment
	Digest() []byte
	Seq() uint64
	Primary() PartyID
	Shard() ShardID
	Serialize() []byte
	Deserialize([]byte) error
}

//go:generate counterfeiter -o mocks/batch_attestation_fragment.go . BatchAttestationFragment
type BatchAttestationFragment interface {
	Seq() uint64
	Primary() PartyID
	Shard() ShardID
	Signer() PartyID
	Digest() []byte
	Serialize() []byte
	Deserialize([]byte) error
	GarbageCollect() [][]byte
	Epoch() uint64
}

type BatchReplicator interface {
	Replicate(shardID ShardID) <-chan Batch
}

type AssemblerIndex interface {
	Index(party PartyID, shard ShardID, sequence uint64, batch Batch)
	Retrieve(party PartyID, shard ShardID, sequence uint64, digest []byte) (Batch, bool)
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
	Shards                     []ShardID

	lock   sync.RWMutex
	signal sync.Cond
}

func (a *Assembler) Run() {
	a.signal = sync.Cond{L: &a.lock}

	for _, shardID := range a.Shards {
		go a.fetchBatchesFromShard(shardID)
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

func (a *Assembler) fetchBatchesFromShard(shardID ShardID) {
	a.Logger.Infof("Starting to fetch batches from shard: %d", shardID)

	batchCh := a.Replicator.Replicate(shardID)
	for batch := range batchCh {
		a.Logger.Infof("Got batch of %d requests for shard %d", len(batch.Requests()), shardID)
		a.lock.RLock()
		a.Index.Index(batch.Party(), shardID, batch.Seq(), batch)
		a.signal.Signal()
		a.lock.RUnlock()
	}

	a.Logger.Infof("Finished fetching batches from shard: %d", shardID)
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
