/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"errors"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
)

//go:generate counterfeiter -o ./mocks/assembler_restarter.go . AssemblerRestarter
type AssemblerRestarter interface {
	SoftStop()
	ConfigBlockNumber() uint64
}

type AssemblerIndex interface {
	PopOrWait(batchId types.BatchID) (types.Batch, error)
	Put(batch types.Batch) error
	Stop()
}

type AssemblerLedgerWriter interface {
	Append(batch types.Batch, orderingInfo *state.OrderingInformation)
	AppendConfig(orderingInfo *state.OrderingInformation)
	Close()
}

type OrderedBatchAttestationReplicator interface {
	Replicate() <-chan *state.AvailableBatchOrdered
}

type Collator struct {
	ShardCount                        int
	Ledger                            AssemblerLedgerWriter
	Logger                            types.Logger
	OrderedBatchAttestationReplicator OrderedBatchAttestationReplicator
	Index                             AssemblerIndex
	Shards                            []types.ShardID
	runningWG                         sync.WaitGroup
	AssemblerRestarter                AssemblerRestarter
}

// Run starts a go routine which processes incoming ordered batch attestations from consensus
// and collates them with batches retrieved from the index.
func (c *Collator) Run() {
	c.runningWG.Add(1)
	go c.processOrderedBatchAttestations()
}

// Stop waits for the collator's goroutines to finish.
func (c *Collator) Stop() {
	c.runningWG.Wait()
}

func (c *Collator) processOrderedBatchAttestations() {
	defer c.runningWG.Done()
	c.Logger.Infof("Starting to process incoming OrderedBatchAttestations from consensus")

	orderedBatchAttestationsChan := c.OrderedBatchAttestationReplicator.Replicate()
	for oba := range orderedBatchAttestationsChan {
		c.Logger.Debugf("Received ordered batch attestation with BatchID: %s; OrderingInfo: %s", types.BatchIDToString(oba.BatchAttestation()), oba.OrderingInformation.String())

		if oba.BatchAttestation().Shard() == types.ShardIDConsensus {
			orderingInfo := oba.OrderingInformation
			c.Logger.Infof("Config decision: shard: %d, Ordering Info: %s", oba.BatchAttestation().Shard(), orderingInfo.String())
			c.Ledger.AppendConfig(orderingInfo)

			// if the configBlcok number is greater than current config sequence, we need to restart the assembler
			if orderingInfo.CommonBlock.GetHeader().GetNumber() > c.AssemblerRestarter.ConfigBlockNumber() {
				c.Logger.Infof("Config block number %d is greater than assembler's current config block number %d, initiating soft stop", orderingInfo.CommonBlock.GetHeader().GetNumber(), c.AssemblerRestarter.ConfigBlockNumber())
				go c.AssemblerRestarter.SoftStop()
				// TODO apply new config and update lastConfigBlockNumber in assembler
				return
			}
		}

		batch, err := c.collateAttestationWithBatch(oba.BatchAttestation())
		if err != nil {
			if errors.Is(err, utils.ErrOperationCancelled) {
				c.Logger.Warnf("Collating Attestation with batch %v was cancelled.", oba.BatchAttestation())
				break
			}
			c.Logger.Panicf("Something went wrong while fetching the batch %v", oba.BatchAttestation())
		}
		c.Ledger.Append(batch, oba.OrderingInformation)
	}
	c.Logger.Infof("Finished processing incoming OrderedBatchAttestations from consensus")
}

func (c *Collator) collateAttestationWithBatch(ba types.BatchAttestation) (types.Batch, error) {
	t1 := time.Now()
	batch, err := c.Index.PopOrWait(ba)
	if err != nil {
		return nil, err
	}
	c.Logger.Debugf("Retrieved full batch with %d requests from index within %s, BatchID: %s", len(batch.Requests()), time.Since(t1), types.BatchIDToString(ba))
	return batch, nil
}
