/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/committedindex"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// pruneState returns a state of four parties and one shard, whose assemblers 1 to 3 reported decision
// reported, and whose durable watermark is still durable. Process raises the watermark to reported.
func pruneState(reported, durable arma_types.DecisionNum) *state.State {
	return &state.State{
		N:         4,
		Threshold: 2,
		Quorum:    3,
		Shards:    []state.ShardTerm{{Shard: 1, Term: 1}},
		AssemblerHeights: []state.AssemblerHeight{
			{Party: 1, DecisionNum: reported},
			{Party: 2, DecisionNum: reported},
			{Party: 3, DecisionNum: reported},
		},
		DurableWatermark: durable,
	}
}

// commitSeqs indexes, through IndexCommitted, one batch of <shard 1, primary 1> per decision: decisions[i]
// commits seqs[i]. The decisions are in increasing order.
func commitSeqs(t *testing.T, c *consensus.Consenter, seqs []arma_types.BatchSequence, decisions []arma_types.DecisionNum) {
	require.Len(t, decisions, len(seqs))
	for i := range seqs {
		hdr := headerWithBatches(t, decisions[i], state.NewAvailableBatch(1, 1, seqs[i], []byte{byte(seqs[i])}))
		hdr.State = &state.State{}
		c.IndexCommitted(hdr)
	}
}

func TestConsenterPrunePointFollowsWatermark(t *testing.T) {
	c := createConsenter(testutil.CreateLogger(t, 0))
	commitSeqs(t, c, []arma_types.BatchSequence{0, 1, 2, 3}, []arma_types.DecisionNum{1, 2, 3, 4})

	next, _, _ := c.SimulateStateTransition(pruneState(2, 0), 0, nil)
	require.Equal(t, arma_types.DecisionNum(2), next.DurableWatermark)
	require.Equal(t, arma_types.BatchSequence(2), next.PrunePoint(1, 1))

	prev := next.Clone()
	prev.AssemblerHeights = pruneState(4, 0).AssemblerHeights
	next, _, _ = c.SimulateStateTransition(prev, 0, nil)
	require.Equal(t, arma_types.BatchSequence(4), next.PrunePoint(1, 1))

	next, _, _ = c.SimulateStateTransition(next, 0, nil)
	require.Equal(t, arma_types.BatchSequence(4), next.PrunePoint(1, 1))
}

func TestConsenterPrunePointStopsAtGap(t *testing.T) {
	c := createConsenter(testutil.CreateLogger(t, 0))
	commitSeqs(t, c, []arma_types.BatchSequence{0, 1, 3}, []arma_types.DecisionNum{1, 2, 3})

	next, _, _ := c.SimulateStateTransition(pruneState(4, 0), 0, nil)
	require.Equal(t, arma_types.BatchSequence(2), next.PrunePoint(1, 1))
}

// TestConsenterPrunePointCappedByThisDecision verifies that the prune point does not pass a seq that the
// decision being simulated commits: a second digest of seq 1 reaches threshold in it, so seq 1 is kept.
func TestConsenterPrunePointCappedByThisDecision(t *testing.T) {
	c := createConsenter(testutil.CreateLogger(t, 0))
	commitSeqs(t, c, []arma_types.BatchSequence{0, 1}, []arma_types.DecisionNum{1, 2})

	var events [][]byte
	for _, signer := range []arma_types.PartyID{2, 3} {
		baf := arma_types.NewSimpleBatchAttestationFragment(1, 1, 1, []byte{0xee}, signer, 0, 0, []byte{1})
		baf.SetSignature([]byte{1})
		events = append(events, (&state.ControlEvent{BAF: baf}).Bytes())
	}

	next, bas, _ := c.SimulateStateTransition(pruneState(2, 0), 0, events)
	require.Len(t, bas, 1)
	require.Equal(t, arma_types.BatchSequence(1), next.PrunePoint(1, 1))
}

func TestConsenterDropsBAFsBelowPrunePoint(t *testing.T) {
	baf := func(seq arma_types.BatchSequence, digest byte) arma_types.BatchAttestationFragment {
		baf := arma_types.NewSimpleBatchAttestationFragment(1, 1, seq, []byte{digest}, 2, 0, 0, []byte{1})
		baf.SetSignature([]byte{1})
		return baf
	}

	t.Run("incoming", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		prev := pruneState(0, 0)
		prev.SetPrunePoint(1, 1, 4)

		next, _, _ := c.SimulateStateTransition(prev, 0, [][]byte{(&state.ControlEvent{BAF: baf(2, 0xee)}).Bytes(), (&state.ControlEvent{BAF: baf(5, 0xee)}).Bytes()})
		require.Len(t, next.Pending, 1)
		require.Equal(t, arma_types.BatchSequence(5), next.Pending[0].Seq())
	})

	t.Run("pending", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		commitSeqs(t, c, []arma_types.BatchSequence{0, 1, 2, 3}, []arma_types.DecisionNum{1, 2, 3, 4})
		prev := pruneState(4, 0)
		prev.Pending = []arma_types.BatchAttestationFragment{baf(3, 0xee), baf(4, 0xee)}

		next, _, _ := c.SimulateStateTransition(prev, 0, nil)
		require.Equal(t, arma_types.BatchSequence(4), next.PrunePoint(1, 1))
		require.Len(t, next.Pending, 1)
		require.Equal(t, arma_types.BatchSequence(4), next.Pending[0].Seq())
	})
}

func TestConsenterIndexCommitted(t *testing.T) {
	batch := state.NewAvailableBatch(1, 1, 4, []byte{1})

	t.Run("config decision", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		hdr := headerWithBatches(t, 7, batch)
		configReq := protoutil.MarshalOrPanic(tx.CreateStructuredConfigEnvelope([]byte{3}))
		configBlock, err := consensus.CreateConfigCommonBlock(2, nil, 0, 7, 2, 1, configReq)
		require.NoError(t, err)
		hdr.AvailableCommonBlocks = append(hdr.AvailableCommonBlocks, configBlock)
		hdr.DecisionNumOfLastConfigBlock = 7
		hdr.State = &state.State{}

		c.IndexCommitted(hdr)

		require.Equal(t, 1, c.Committed.Size())
		require.Equal(t, []committedindex.LedgerKey{{Shard: 1, Primary: 1}}, c.Committed.Ledgers())
		require.Equal(t, arma_types.DecisionNum(7), c.Committed.ReplayFrom())
	})

	t.Run("genesis", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		genesis := tx.CreateConfigBlock(0, []byte{3})
		genesis.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = utils.GenesisBlockMetadataBytes()

		c.IndexCommitted(&state.Header{Num: 0, AvailableCommonBlocks: []*common.Block{genesis}, State: &state.State{}})

		require.Zero(t, c.Committed.Size())
		require.Equal(t, arma_types.DecisionNum(1), c.Committed.ReplayFrom())
	})

	t.Run("unverified signatures metadata is not read", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		hdr := headerWithBatches(t, 7, batch)
		hdr.AvailableCommonBlocks[0].Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = []byte{0xff, 0xff, 0xff}
		hdr.State = &state.State{}

		require.NotPanics(t, func() { c.IndexCommitted(hdr) })
		require.Equal(t, 1, c.Committed.Size())
	})

	t.Run("malformed orderer metadata panics", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		hdr := headerWithBatches(t, 7, batch)
		hdr.AvailableCommonBlocks[0].Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = []byte{0xff, 0xff, 0xff}
		hdr.State = &state.State{}

		require.Panics(t, func() { c.IndexCommitted(hdr) })
	})

	t.Run("skipped decision panics", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		commitSeqs(t, c, []arma_types.BatchSequence{0}, []arma_types.DecisionNum{1})

		hdr := headerWithBatches(t, 3, batch)
		hdr.State = &state.State{}
		require.Panics(t, func() { c.IndexCommitted(hdr) })
	})

	t.Run("drops batches below the decision's prune points", func(t *testing.T) {
		c := createConsenter(testutil.CreateLogger(t, 0))
		commitSeqs(t, c, []arma_types.BatchSequence{0, 1, 2, 3}, []arma_types.DecisionNum{1, 2, 3, 4})

		hdr := headerWithBatches(t, 5)
		hdr.State = &state.State{}
		hdr.State.SetPrunePoint(1, 1, 2)
		c.IndexCommitted(hdr)

		require.Equal(t, 2, c.Committed.Size())
		require.Equal(t, arma_types.DecisionNum(3), c.Committed.ReplayFrom())
	})
}

func TestConsenterReplayFrom(t *testing.T) {
	c := createConsenter(testutil.CreateLogger(t, 0))
	commitSeqs(t, c, []arma_types.BatchSequence{0, 1, 2, 3}, []arma_types.DecisionNum{1, 2, 3, 4})

	next, _, _ := c.SimulateStateTransition(pruneState(2, 0), 0, nil)
	require.Equal(t, arma_types.DecisionNum(1), next.ReplayFrom)

	// Committing the next decision drops the batches below its prune points, and the following decision
	// replays from the lowest decision still held.
	hdr := headerWithBatches(t, 5)
	hdr.State = next
	c.IndexCommitted(hdr)

	next, _, _ = c.SimulateStateTransition(next, 0, nil)
	require.Equal(t, arma_types.DecisionNum(3), next.ReplayFrom)
}

// headerWithBatches returns the header of decision num whose available blocks hold the given batches.
func headerWithBatches(t *testing.T, num arma_types.DecisionNum, batches ...arma_types.BatchID) *state.Header {
	hdr := &state.Header{Num: num, DecisionNumOfLastConfigBlock: 0}
	if num == 0 {
		hdr.DecisionNumOfLastConfigBlock = 1 // not a config decision
	}
	for i, batch := range batches {
		block, err := consensus.CreateDataCommonBlock(uint64(i), nil, batch, 1, num, len(batches), i, 0)
		require.NoError(t, err)
		hdr.AvailableCommonBlocks = append(hdr.AvailableCommonBlocks, block)
	}
	return hdr
}
