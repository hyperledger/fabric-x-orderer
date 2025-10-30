/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"errors"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
)

type AssemblerIndex interface {
	PopOrWait(batchId types.BatchID) (types.Batch, error)
	Put(batch types.Batch) error
	Stop()
}

type AssemblerLedgerWriter interface {
	Append(batch types.Batch, orderingInfo types.OrderingInfo)
	AppendConfig(configBlock *common.Block, decisionNum types.DecisionNum)
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
	Index                             AssemblerIndex
	Shards                            []types.ShardID
	runningWG                         sync.WaitGroup
}

// Run starts a go routine which processes incoming ordered batch attestations from consensus
// and collates them with batches retrieved from the index.
func (a *AssemblerRole) Run() {
	a.runningWG.Add(1)
	go a.processOrderedBatchAttestations()
}

// WaitTermination the core Assembler is stopped by the node Assembler when the channels for batches and BAs are closed.
// This methods only waits for the core go routines to finish.
func (a *AssemblerRole) WaitTermination() {
	a.runningWG.Wait()
}

func (a *AssemblerRole) processOrderedBatchAttestations() {
	defer a.runningWG.Done()
	a.Logger.Infof("Starting to process incoming OrderedBatchAttestations from consensus")

	orderedBatchAttestations := a.OrderedBatchAttestationReplicator.Replicate()
	for oba := range orderedBatchAttestations {
		a.Logger.Infof("Received ordered batch attestation with BatchID: %s; OrderingInfo: %s", types.BatchIDToString(oba.BatchAttestation()), oba.OrderingInfo().String())

		if oba.BatchAttestation().Shard() == types.ShardIDConsensus {
			orderingInfo := oba.OrderingInfo()
			a.Logger.Infof("Config decision: shard: %d, Ordering Info: %s", oba.BatchAttestation().Shard(), oba.OrderingInfo().String())
			// TODO break the abstraction of oba.OrderingInfo().String()
			ordInfo := orderingInfo.(*state.OrderingInformation)
			block := ordInfo.CommonBlock
			a.Ledger.AppendConfig(block, ordInfo.DecisionNum)
			// TODO apply new config
			return
		}

		batch, err := a.collateAttestationWithBatch(oba.BatchAttestation())
		if err != nil {
			if errors.Is(err, utils.ErrOperationCancelled) {
				a.Logger.Warnf("Collating Attestation with batch %v was cancelled.", oba.BatchAttestation())
				break
			}
			a.Logger.Panicf("Something went wrong while fetching the batch %v", oba.BatchAttestation())
		}
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
	a.Logger.Infof("Retrieved full batch with %d requests from index within %s, BatchID: %s", len(batch.Requests()), time.Since(t1), types.BatchIDToString(ba))
	return batch, nil
}
