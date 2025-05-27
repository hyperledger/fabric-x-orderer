/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
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
	PopOrWait(batchId types.BatchID) (Batch, error)
	Put(batch Batch) error
	Stop()
}

type AssemblerLedgerWriter interface {
	Append(batch Batch, orderingInfo interface{})
	Close()
}

type AssemblerConsensusPosition struct {
	DecisionNum types.DecisionNum
	BatchIndex  int
}

type OrderedBatchAttestationReplicator interface {
	Replicate() <-chan OrderedBatchAttestation
}

type Assembler struct {
	ShardCount                        int
	Ledger                            AssemblerLedgerWriter
	Logger                            types.Logger
	OrderedBatchAttestationReplicator OrderedBatchAttestationReplicator
	Replicator                        BatchReplicator
	Index                             AssemblerIndex
	Shards                            []types.ShardID
	runningWG                         sync.WaitGroup
}

func (a *Assembler) Run() {
	// TODO we need to be able to stop these goroutines when we stop the assembler server
	for _, shardID := range a.Shards {
		a.runningWG.Add(1)
		go a.fetchBatchesFromShard(shardID)
	}
	a.runningWG.Add(1)
	go a.processOrderedBatchAttestations()
}

// Core Assebler stops by the node Assebler when the channels for batches and BAs are closed.
// This methods only waits for the core go routines to finish.
func (a *Assembler) WaitTermination() {
	a.runningWG.Wait()
}

func (a *Assembler) fetchBatchesFromShard(shardID types.ShardID) {
	defer a.runningWG.Done()
	a.Logger.Infof("Starting to fetch batches from shard: %d", shardID)

	batchCh := a.Replicator.Replicate(shardID)
	for batch := range batchCh {
		a.Logger.Infof("Got batch of %d requests for shard %d", len(batch.Requests()), shardID)
		a.Index.Put(batch)
	}

	a.Logger.Infof("Finished fetching batches from shard: %d", shardID)
}

func (a *Assembler) processOrderedBatchAttestations() {
	defer a.runningWG.Done()
	a.Logger.Infof("Starting to process incoming OrderedBatchAttestations from consensus")

	orderedBatchAttestations := a.OrderedBatchAttestationReplicator.Replicate()
	for oba := range orderedBatchAttestations {
		a.Logger.Infof("Received ordered batch attestation with BatchID primary=%d, shard=%d, seq=%d; digest %s",
			oba.BatchAttestation().Primary(), oba.BatchAttestation().Shard(), oba.BatchAttestation().Seq(),
			ShortDigestString(oba.BatchAttestation().Digest()))
		a.Logger.Infof("Received ordered batch attestation with OrderingInfo: %+v; digest %s",
			oba.OrderingInfo(),
			ShortDigestString(oba.BatchAttestation().Digest()))

		if oba.BatchAttestation().Shard() == types.ShardIDConsensus {
			a.Logger.Infof("Config decision: shard: %d, primary: %d, Ignoring!", oba.BatchAttestation().Shard(), oba.BatchAttestation().Primary())
			return
		}

		t1 := time.Now()
		batch, err := a.collateAttestationWithBatch(oba.BatchAttestation())
		if err != nil {
			if errors.Is(err, utils.ErrOperationCancelled) {
				a.Logger.Warnf("Collating Attestation with batch %v was cancelled.", oba.BatchAttestation())
				break
			}
			a.Logger.Panicf("Something went wrong while fetching the batch %v", oba.BatchAttestation())
		}
		a.Logger.Infof("Located batch for digest %s within %v", ShortDigestString(oba.BatchAttestation().Digest()), time.Since(t1))
		a.Ledger.Append(batch, oba.OrderingInfo())
	}
	a.Logger.Infof("Finished processing incoming OrderedBatchAttestations from consensus")
}

func (a *Assembler) collateAttestationWithBatch(ba BatchAttestation) (Batch, error) {
	t1 := time.Now()
	batch, err := a.Index.PopOrWait(ba)
	if err != nil {
		return nil, err
	}
	a.Logger.Infof("Retrieved batch with %d requests for attestation %s from index within %v", len(batch.Requests()), ShortDigestString(ba.Digest()), time.Since(t1))
	return batch, nil
}

// ShortDigestString provides a short string from a potentially long (32B) digest.
func ShortDigestString(digest []byte) string {
	if digest == nil {
		return "nil"
	}
	if len(digest) <= 8 {
		return hex.EncodeToString(digest)
	}

	return hex.EncodeToString(digest[:8])
}
