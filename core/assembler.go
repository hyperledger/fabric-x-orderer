package core

import (
	"encoding/hex"
	"sync"
	"time"

	"arma/common/types"
)

//go:generate counterfeiter -o mocks/batch_attestation.go . BatchAttestation
type BatchAttestation interface {
	Fragments() []BatchAttestationFragment
	Digest() []byte
	Seq() types.BatchSequence
	Primary() types.PartyID
	Shard() types.ShardID
	Serialize() []byte
	Deserialize([]byte) error
}

//go:generate counterfeiter -o mocks/batch_attestation_fragment.go . BatchAttestationFragment
type BatchAttestationFragment interface {
	Seq() types.BatchSequence
	Primary() types.PartyID
	Shard() types.ShardID
	Signer() types.PartyID
	Signature() []byte
	Digest() []byte
	Serialize() []byte
	Deserialize([]byte) error
	GarbageCollect() [][]byte
	Epoch() int64
}

// OrderedBatchAttestation carries the BatchAttestation and information on the actual order of the batch from the
// consensus cluster. This information is used when appending to the ledger, and helps the assembler to recover
// following a shutdown or a failure.
type OrderedBatchAttestation interface {
	BatchAttestation() BatchAttestation
	// OrderingInfo is an opaque object that provides extra information on the order of the batch attestation and
	// metadata to be used in the construction of the block.
	OrderingInfo() interface{}
}

type BatchReplicator interface {
	Replicate(shardID types.ShardID) <-chan Batch
}

type AssemblerIndex interface {
	Index(party types.PartyID, shard types.ShardID, sequence types.BatchSequence, batch Batch)
	Retrieve(party types.PartyID, shard types.ShardID, sequence types.BatchSequence, digest []byte) (Batch, bool)
}

type AssemblerLedger interface {
	Append(uint64, Batch, BatchAttestation)
}

type OrderedBatchAttestationReplicator interface {
	Replicate(uint64) <-chan OrderedBatchAttestation
}

type Assembler struct {
	ShardCount                        int
	Ledger                            AssemblerLedger
	Logger                            types.Logger
	OrderedBatchAttestationReplicator OrderedBatchAttestationReplicator
	Replicator                        BatchReplicator
	Index                             AssemblerIndex
	Shards                            []types.ShardID

	lock   sync.RWMutex
	signal sync.Cond
}

func (a *Assembler) Run() {
	a.signal = sync.Cond{L: &a.lock}

	// TODO we need to be able to stop these goroutines when we stop the assembler server
	for _, shardID := range a.Shards {
		go a.fetchBatchesFromShard(shardID)
	}

	go a.processOrderedBatchAttestations()
}

func (a *Assembler) fetchBatchesFromShard(shardID types.ShardID) {
	a.Logger.Infof("Starting to fetch batches from shard: %d", shardID)

	batchCh := a.Replicator.Replicate(shardID)
	for batch := range batchCh {
		a.Logger.Infof("Got batch of %d requests for shard %d", len(batch.Requests()), shardID)
		a.lock.RLock()
		a.Index.Index(batch.Primary(), shardID, batch.Seq(), batch)
		a.signal.Signal()
		a.lock.RUnlock()
	}

	a.Logger.Infof("Finished fetching batches from shard: %d", shardID)
}

func (a *Assembler) processOrderedBatchAttestations() {
	a.Logger.Infof("Starting to process incoming OrderedBatchAttestations from consensus")

	// TODO after recovery support is added, start replicating from the last decision, not 0 (separate issue).
	attestations := a.OrderedBatchAttestationReplicator.Replicate(0) // TODO change type to OrderedBatchAttestation (next commit).
	for oba := range attestations {
		a.Logger.Infof("Received attestation with digest %s", shortDigestString(oba.BatchAttestation().Digest()))
		t1 := time.Now()
		batch := a.collateAttestationWithBatch(oba.BatchAttestation())
		a.Logger.Infof("Located batch for digest %s within %v", shortDigestString(oba.BatchAttestation().Digest()), time.Since(t1))
		a.Ledger.Append(uint64(0), batch, oba.BatchAttestation()) // TODO this is the wrong sequence number, it should be the block number from the ba header (next commit)
	}

	a.Logger.Infof("Finished processing incoming OrderedBatchAttestations from consensus")
}

func (a *Assembler) collateAttestationWithBatch(ba BatchAttestation) Batch {
	a.lock.Lock()
	defer a.lock.Unlock()

	for {
		t1 := time.Now()
		batch, retrieved := a.Index.Retrieve(ba.Primary(), ba.Shard(), ba.Seq(), ba.Digest())
		if !retrieved {
			a.signal.Wait()
			continue
		}
		a.Logger.Infof("Retrieved batch with %d requests for attestation %s from index within %v", len(batch.Requests()), shortDigestString(ba.Digest()), time.Since(t1))
		return batch
	}
}

// shortDigestString provides a short string from a potentially long (32B) digest.
func shortDigestString(digest []byte) string {
	if digest == nil {
		return "nil"
	}
	if len(digest) <= 8 {
		return hex.EncodeToString(digest)
	}

	return hex.EncodeToString(digest[:8])
}
