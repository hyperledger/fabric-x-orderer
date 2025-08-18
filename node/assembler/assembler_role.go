/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
)

type BatchReplicator interface {
	Replicate(shardID types.ShardID) <-chan types.Batch
}

type AssemblerIndex interface {
	PopOrWait(batchId types.BatchID) (types.Batch, error)
	Put(batch types.Batch) error
	Stop()
}

type AssemblerLedgerWriter interface {
	Append(batch types.Batch, orderingInfo types.OrderingInfo)
	Close()
}

type OrderedBatchAttestationReplicator interface {
	Replicate() <-chan types.OrderedBatchAttestation
}

type AssemblerRole struct {
	ShardCount                        int
	Ledger                            AssemblerLedgerWriter
	Logger                            types.Logger
	OrderedBatchAttestationReplicator OrderedBatchAttestationReplicator
	Replicator                        BatchReplicator
	Index                             AssemblerIndex
	Shards                            []types.ShardID
	runningWG                         sync.WaitGroup
}

func (a *AssemblerRole) Run() {
	// TODO we need to be able to stop these goroutines when we stop the assembler server
	for _, shardID := range a.Shards {
		a.runningWG.Add(1)
		go a.fetchBatchesFromShard(shardID)
	}
	a.runningWG.Add(1)
	go a.processOrderedBatchAttestations()
}

// WaitTermination the core Assembler is stopped by the node Assembler when the channels for batches and BAs are closed.
// This methods only waits for the core go routines to finish.
func (a *AssemblerRole) WaitTermination() {
	a.runningWG.Wait()
}

func (a *AssemblerRole) fetchBatchesFromShard(shardID types.ShardID) {
	defer a.runningWG.Done()
	a.Logger.Infof("Starting to fetch batches from shard: %d", shardID)

	batchCh := a.Replicator.Replicate(shardID)
	for batch := range batchCh {
		a.Logger.Infof("Got batch of %d requests for shard %d", len(batch.Requests()), shardID)
		a.Index.Put(batch)
	}

	a.Logger.Infof("Finished fetching batches from shard: %d", shardID)
}

func (a *AssemblerRole) processOrderedBatchAttestations() {
	defer a.runningWG.Done()
	a.Logger.Infof("Starting to process incoming OrderedBatchAttestations from consensus")

	orderedBatchAttestations := a.OrderedBatchAttestationReplicator.Replicate()
	for oba := range orderedBatchAttestations {
		a.Logger.Infof("Received ordered batch attestation with BatchID: %s; OrderingInfo: %s", types.BatchIDToString(oba.BatchAttestation()), oba.OrderingInfo().String())

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

func (a *AssemblerRole) collateAttestationWithBatch(ba types.BatchAttestation) (types.Batch, error) {
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
