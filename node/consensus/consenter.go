/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"fmt"
	"sync/atomic"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/committedindex"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
)

type TotalOrder interface {
	SubmitRequest(req []byte) error
}

//go:generate counterfeiter -o mocks/batch_attestation_db.go . BatchAttestationDB
type BatchAttestationDB interface {
	Exists(digest []byte) bool
	Put(digest [][]byte, epoch []uint64) // TODO remove epochs from BADB
	Clean(epoch uint64)                  // TODO remove clean by epoch
}

type Consenter struct {
	Logger *flogging.FabricLogger
	DB     BatchAttestationDB
	// Committed indexes the committed batches that are not pruned yet, from which the prune points are
	// computed. It is fed at commit, by IndexCommitted, and only read while simulating a decision.
	Committed committedindex.Index
	// nextSizeWarning is the index size at which IndexCommitted warns next.
	nextSizeWarning atomic.Int64
}

// committedIndexWarnSize is the index size at which IndexCommitted first warns; it warns again each time
// the size doubles. The index grows when the durable watermark stalls, while batchers cannot prune either.
const committedIndexWarnSize = 1 << 20

func (c *Consenter) SimulateStateTransition(prevState *state.State, configSeq types.ConfigSequence, requests [][]byte) (*state.State, [][]types.BatchAttestationFragment, []*state.ConfigRequest) {
	controlEvents, err := requestsToControlEvents(requests)
	if err != nil {
		panic(err)
	}

	filteredControlEvents := make([]state.ControlEvent, 0, len(controlEvents))

	// Iterate over all control events and prune those that already exist in our DB, or that attest a
	// batch below the prune point, which batchers may already have dropped.
	for _, ce := range controlEvents {
		if ce.BAF != nil {
			if ce.BAF.Seq() < prevState.PrunePoint(ce.BAF.Shard(), ce.BAF.Primary()) {
				c.Logger.Debugf("Batch attestation fragment below the prune point; %s", ce.BAF.String())
				continue
			}
			if c.DB.Exists(ce.BAF.Digest()) {
				c.Logger.Debugf("Batch attestation for digest %x already exists", ce.BAF.Digest())
				continue
			}
		}
		filteredControlEvents = append(filteredControlEvents, ce)
	}

	newState, fragments, configRequests := prevState.Process(c.Logger, configSeq, filteredControlEvents...)
	batchAttestations := aggregateFragments(fragments)
	c.updatePrunePoints(prevState, newState, fragments)

	return newState, batchAttestations, configRequests
}

// updatePrunePoints advances the prune points of next when its durable watermark advanced, over the batches
// the index holds as committed in decisions up to the watermark, and records where to replay the index from.
// A prune point never passes a seq that this decision commits, given by the extracted fragments: the index
// holds only committed decisions, so it would not stop at such a seq. It reads the index and never changes
// it, so every node computes the same next state.
func (c *Consenter) updatePrunePoints(prev, next *state.State, committing []types.BatchAttestationFragment) {
	if next.DurableWatermark > prev.DurableWatermark {
		lowestCommitting := make(map[committedindex.LedgerKey]types.BatchSequence)
		for _, baf := range committing {
			key := committedindex.LedgerKey{Shard: baf.Shard(), Primary: baf.Primary()}
			if seq, exists := lowestCommitting[key]; !exists || baf.Seq() < seq {
				lowestCommitting[key] = baf.Seq()
			}
		}

		for _, key := range c.Committed.Ledgers() {
			seq := c.Committed.DurableFrontier(key.Shard, key.Primary, next.PrunePoint(key.Shard, key.Primary), next.DurableWatermark)
			if lowest, exists := lowestCommitting[key]; exists {
				seq = min(seq, lowest)
			}
			next.SetPrunePoint(key.Shard, key.Primary, seq)
		}
		next.DropPendingBelowPrunePoints(c.Logger)
	}
	next.ReplayFrom = c.Committed.ReplayFrom()
}

// Index indexes BAs (digests).
// Note that this must hold: Index(digests) with the same digests is idempotent.
// TODO revise the recovery from failure or shutdown, specifically the order of Index and Append.
func (c *Consenter) Index(digests [][]byte) {
	if len(digests) > 0 {
		epochs := make([]uint64, len(digests)) // TODO remove
		c.DB.Put(digests, epochs)
	}
}

// IndexCommitted adds the batches that the decision with the given header committed to the index of
// committed batches, and drops the batches below the decision's prune points. It panics if a batch ID
// cannot be read.
func (c *Consenter) IndexCommitted(hdr *state.Header) {
	ids, err := committedBatchIDs(hdr)
	if err != nil {
		c.Logger.Panicf("Failed indexing the batches of decision %d: %v", hdr.Num, err)
	}
	if err := c.Committed.Add(hdr.Num, ids); err != nil {
		c.Logger.Panicf("Failed indexing the batches of decision %d: %v", hdr.Num, err)
	}
	if hdr.State != nil {
		for _, p := range hdr.State.PrunePoints {
			c.Committed.DropBelow(p.Shard, p.Primary, p.Seq)
		}
	}

	c.nextSizeWarning.CompareAndSwap(0, committedIndexWarnSize)
	if size := int64(c.Committed.Size()); size >= c.nextSizeWarning.Load() {
		c.nextSizeWarning.Store(2 * size)
		c.Logger.Warnf("The index of committed batches holds %d batches; it shrinks only as the durable watermark advances", size)
	}
}

// committedBatchIDs returns the batch IDs of the batches that the decision with the given header committed.
// It reads only what proposal verification fixes: a config decision carries its config block last, after
// the data blocks, and the batch ID of a data block is in its ORDERER metadata and its header data hash.
func committedBatchIDs(hdr *state.Header) ([]types.BatchID, error) {
	dataBlocks := hdr.AvailableCommonBlocks
	if hdr.Num == hdr.DecisionNumOfLastConfigBlock && len(dataBlocks) > 0 {
		dataBlocks = dataBlocks[:len(dataBlocks)-1]
	}

	ids := make([]types.BatchID, 0, len(dataBlocks))
	for _, block := range dataBlocks {
		primary, shard, seq, _, _, _, _, err := ledger.AssemblerBlockMetadataFromBytes(block.GetMetadata().GetMetadata()[common.BlockMetadataIndex_ORDERER])
		if err != nil {
			return nil, fmt.Errorf("failed reading the batch ID of block %d: %w", block.GetHeader().GetNumber(), err)
		}
		ids = append(ids, state.NewAvailableBatch(primary, shard, seq, block.GetHeader().GetDataHash()))
	}
	return ids, nil
}

// batchIDKey is the comparable form of types.BatchID (<shard, primary, seq, digest>): the
// tuple that identifies a batch. digest is a string because BatchID.Digest() ([]byte) is not
// map-key-comparable. Fragments are grouped by batch identity so that each block corresponds
// to exactly one batch. In the honest case there is one batch per <seq, shard> (unchanged
// behavior); distinct digests (an equivocating primary) or distinct primaries (e.g. the same
// batch reproduced across a rotation) are different BatchIDs and therefore different blocks.
// This guarantees every group is a single batch, so consumers that read only the first
// fragment (ba[0]) always see a threshold-attested batch rather than a stale/minority digest
// that happened to be first in Pending order.
type batchIDKey struct {
	shard   types.ShardID
	primary types.PartyID
	seq     types.BatchSequence
	digest  string
}

func batchIDKeyOf(baf types.BatchAttestationFragment) batchIDKey {
	return batchIDKey{shard: baf.Shard(), primary: baf.Primary(), seq: baf.Seq(), digest: string(baf.Digest())}
}

// aggregateFragments groups fragments by batch identity (batchIDKey), preserving the order in
// which each group's key first appears. Each returned group holds all fragments of one batch.
func aggregateFragments(batchAttestationFragments []types.BatchAttestationFragment) [][]types.BatchAttestationFragment {
	groups := make(map[batchIDKey][]types.BatchAttestationFragment)
	var order []batchIDKey // keys in first-seen order (map iteration is unordered)

	for _, baf := range batchAttestationFragments {
		key := batchIDKeyOf(baf)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], baf)
	}

	attestations := make([][]types.BatchAttestationFragment, 0, len(order))
	for _, key := range order {
		attestations = append(attestations, groups[key])
	}

	return attestations
}

func requestsToControlEvents(requests [][]byte) ([]state.ControlEvent, error) {
	events := make([]state.ControlEvent, 0, len(requests))
	for i := range requests {
		ce := state.ControlEvent{}
		if err := ce.FromBytes(requests[i]); err != nil {
			return nil, err
		}
		events = append(events, ce)
	}

	return events, nil
}
