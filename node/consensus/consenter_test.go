/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestConsenter(t *testing.T) {
	s := &state.State{
		N:         4,
		Shards:    []state.ShardTerm{{Shard: 1, Term: 1}},
		Threshold: 2,
		Pending:   []arma_types.BatchAttestationFragment{},
		Quorum:    4,
	}

	logger := testutil.CreateLogger(t, 0)
	consenter := createConsenter(logger)

	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	ba := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(2), 0, 0, nil)
	ba.SetSignature([]uint8{1})
	events := [][]byte{(&state.ControlEvent{BAF: ba}).Bytes()}

	// Test with an event that should be filtered out
	db.ExistsReturns(true)
	newState, batchAttestations, _ := consenter.SimulateStateTransition(s, 0, events)
	assert.Empty(t, batchAttestations)
	assert.Empty(t, newState.Pending)

	consenter.Index(headerWithBatches(t, 1))
	assert.Zero(t, db.PutCallCount())

	// Test a valid event below threshold
	db.ExistsReturns(false)
	newState, batchAttestations, _ = consenter.SimulateStateTransition(s, 0, events)
	assert.Empty(t, batchAttestations)
	assert.Len(t, newState.Pending, 1)

	consenter.Index(headerWithBatches(t, 2))
	assert.Zero(t, db.PutCallCount())

	// Test valid events meeting the threshold
	ba2 := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(3), 0, 0, nil)
	ba2.SetSignature([]byte{1})
	events = append(events, (&state.ControlEvent{BAF: ba2}).Bytes())

	newState, batchAttestations, _ = consenter.SimulateStateTransition(s, 0, events)
	assert.Len(t, batchAttestations[0], 2)
	assert.Empty(t, newState.Pending)

	consenter.Index(headerWithBatches(t, 3, batchAttestations[0][0]))
	assert.Equal(t, db.PutCallCount(), 1)

	// Test the complaint is not stored in the DB
	c := state.Complaint{
		ShardTerm: state.ShardTerm{Shard: 1, Term: 1},
		Signer:    3,
		Signature: []byte{2},
	}

	events = [][]byte{(&state.ControlEvent{Complaint: &c}).Bytes()}

	_, batchAttestations, _ = consenter.SimulateStateTransition(s, 0, events)
	assert.Empty(t, batchAttestations)
	consenter.Index(headerWithBatches(t, 4))
	assert.Equal(t, db.PutCallCount(), 1)

	// Test ConfigRequest is returned by SimulateStateTransition

	configEnvelope := &common.ConfigEnvelope{
		Config: &common.Config{
			Sequence: 1,
		},
		LastUpdate: nil,
	}
	configBytes, err := proto.Marshal(configEnvelope)
	assert.NoError(t, err)
	cr := &state.ConfigRequest{
		Envelope: tx.CreateStructuredConfigUpdateEnvelope(configBytes),
	}
	events = [][]byte{(&state.ControlEvent{ConfigRequest: cr}).Bytes()}

	_, _, configRequests := consenter.SimulateStateTransition(s, 0, events)
	assert.Equal(t, cr.Envelope.Payload, configRequests[0].Envelope.Payload)
	assert.Equal(t, cr.Envelope.Signature, configRequests[0].Envelope.Signature)
}

// TestConsenterAggregateFragmentsHonest exercises the full SimulateStateTransition path (which
// calls aggregateFragments) on honest, threshold-reaching input: distinct batches each attested
// by a threshold of signers, with no equivocation. It pins the grouping/ordering contract that
// downstream relies on: fragments are grouped by batch identity (one group per batch), every
// fragment in a group shares that batch's digest, and groups appear in the order their batch's
// key is first seen in Pending.
func TestConsenterAggregateFragmentsHonest(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	// newBAF builds a serialized BAF control event for the given batch <shard, primary, seq, digest> and signer.
	newBAF := func(shard arma_types.ShardID, primary arma_types.PartyID, seq arma_types.BatchSequence, digest []byte, signer arma_types.PartyID) []byte {
		baf := arma_types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, signer, 0, 0, nil)
		baf.SetSignature([]byte{1})
		return (&state.ControlEvent{BAF: baf}).Bytes()
	}

	newState := func() *state.State {
		return &state.State{
			N:         7,
			Shards:    []state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
			Threshold: 2,
			Pending:   []arma_types.BatchAttestationFragment{},
			Quorum:    5,
		}
	}

	db := &mocks.FakeBatchAttestationDB{}
	db.ExistsReturns(false)
	consenter := createConsenter(logger)
	consenter.DB = db

	// Two distinct honest batches, each attested by two signers (>= threshold). Batch A appears
	// first in the event stream; the two batches interleave to prove grouping is by identity,
	// not adjacency, and that ordering follows first-seen key.
	digestA := []byte{1}
	digestB := []byte{2}
	events := [][]byte{
		newBAF(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), digestA, arma_types.PartyID(2)),
		newBAF(arma_types.ShardID(2), arma_types.PartyID(3), arma_types.BatchSequence(1), digestB, arma_types.PartyID(4)),
		newBAF(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), digestA, arma_types.PartyID(5)),
		newBAF(arma_types.ShardID(2), arma_types.PartyID(3), arma_types.BatchSequence(1), digestB, arma_types.PartyID(6)),
	}

	_, batchAttestations, _ := consenter.SimulateStateTransition(newState(), 0, events)

	// One group per batch, in first-seen order: batch A (shard 1) then batch B (shard 2).
	assert.Len(t, batchAttestations, 2)

	assert.Equal(t, arma_types.ShardID(1), batchAttestations[0][0].Shard())
	assert.Equal(t, arma_types.ShardID(2), batchAttestations[1][0].Shard())

	// Each group holds both fragments of its batch, all sharing that batch's digest.
	for _, group := range batchAttestations {
		assert.Len(t, group, 2)
		d := group[0].Digest()
		for _, baf := range group {
			assert.Equal(t, d, baf.Digest(), "every fragment in a group must share the batch digest")
			assert.Equal(t, group[0].Shard(), baf.Shard())
			assert.Equal(t, group[0].Primary(), baf.Primary())
			assert.Equal(t, group[0].Seq(), baf.Seq())
		}
	}
	assert.Equal(t, digestA, batchAttestations[0][0].Digest())
	assert.Equal(t, digestB, batchAttestations[1][0].Digest())
}

// TestConsenterEquivocatingDigests exercises the full SimulateStateTransition path (which calls
// aggregateFragments) when a primary equivocates: emits multiple digests for the same
// <seq, shard, primary>. It verifies that (1) a below-threshold digest never becomes a block, and
// (2) if two digests both reach threshold, each becomes its own single-digest block.
func TestConsenterEquivocatingDigests(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	// newBAF builds a serialized BAF control event for <shard 1, seq 1> from the given primary and signer.
	newBAF := func(primary arma_types.PartyID, digest []byte, signer arma_types.PartyID) []byte {
		baf := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), primary, arma_types.BatchSequence(1), digest, signer, 0, 0, nil)
		baf.SetSignature([]byte{1})
		return (&state.ControlEvent{BAF: baf}).Bytes()
	}

	newState := func() *state.State {
		return &state.State{
			N:         7,
			Shards:    []state.ShardTerm{{Shard: 1, Term: 1}},
			Threshold: 2,
			Pending:   []arma_types.BatchAttestationFragment{},
			Quorum:    5,
		}
	}

	newConsenter := func() *consensus.Consenter {
		consenter := createConsenter(logger)
		db := &mocks.FakeBatchAttestationDB{}
		db.ExistsReturns(false)
		consenter.DB = db
		return consenter
	}

	t.Run("under-attested digest does not become a block", func(t *testing.T) {
		underAttested := []byte{9}
		threshold := []byte{3}
		// The under-attested digest is placed first, so before the fix it would have become ba[0].
		events := [][]byte{
			newBAF(arma_types.PartyID(1), underAttested, arma_types.PartyID(4)), // single signer -> below threshold
			newBAF(arma_types.PartyID(1), threshold, arma_types.PartyID(2)),
			newBAF(arma_types.PartyID(1), threshold, arma_types.PartyID(3)), // 2 signers -> reaches threshold
		}

		_, batchAttestations, _ := newConsenter().SimulateStateTransition(newState(), 0, events)

		// Exactly one block, backed only by the threshold digest.
		assert.Len(t, batchAttestations, 1)
		assert.Len(t, batchAttestations[0], 2)
		for _, baf := range batchAttestations[0] {
			assert.Equal(t, threshold, baf.Digest())
		}
		// ba[0] is what consensus uses for the block's DataHash; it must be the threshold digest.
		assert.Equal(t, threshold, batchAttestations[0][0].Digest())
	})

	t.Run("two digests reaching threshold produce two single-digest blocks", func(t *testing.T) {
		digestA := []byte{3}
		digestB := []byte{7}
		events := [][]byte{
			newBAF(arma_types.PartyID(1), digestA, arma_types.PartyID(2)),
			newBAF(arma_types.PartyID(1), digestA, arma_types.PartyID(3)),
			newBAF(arma_types.PartyID(1), digestB, arma_types.PartyID(4)),
			newBAF(arma_types.PartyID(1), digestB, arma_types.PartyID(5)),
		}

		_, batchAttestations, _ := newConsenter().SimulateStateTransition(newState(), 0, events)

		// Both digests crossed threshold -> two separate blocks, each grouping only its own digest.
		assert.Len(t, batchAttestations, 2)
		seenDigests := map[string]int{}
		for _, group := range batchAttestations {
			assert.Len(t, group, 2)
			d := string(group[0].Digest())
			for _, baf := range group {
				assert.Equal(t, d, string(baf.Digest()), "each block group must contain a single digest")
			}
			seenDigests[d]++
		}
		assert.Equal(t, 1, seenDigests[string(digestA)])
		assert.Equal(t, 1, seenDigests[string(digestB)])
	})

	t.Run("same digest from two different primaries produce two blocks with distinct BatchIDs", func(t *testing.T) {
		// A legitimate (non-equivocation) scenario: across a primary rotation both primaries
		// batch the same pending requests, producing an identical digest for the same shard/seq.
		// These are two distinct BatchIDs (different Primary), so each reaches threshold on its
		// own <seq, shard, primary> tuple and becomes its own block. This pins the primary
		// dimension of the aggregation key: keying on digest alone would wrongly merge them.
		digest := []byte{3}
		events := [][]byte{
			newBAF(arma_types.PartyID(1), digest, arma_types.PartyID(2)),
			newBAF(arma_types.PartyID(1), digest, arma_types.PartyID(3)),
			newBAF(arma_types.PartyID(6), digest, arma_types.PartyID(4)),
			newBAF(arma_types.PartyID(6), digest, arma_types.PartyID(5)),
		}

		_, batchAttestations, _ := newConsenter().SimulateStateTransition(newState(), 0, events)

		// Same digest, different primary -> two distinct BatchIDs -> two blocks.
		assert.Len(t, batchAttestations, 2)
		seenPrimaries := map[arma_types.PartyID]int{}
		for _, group := range batchAttestations {
			assert.Len(t, group, 2)
			p := group[0].Primary()
			for _, baf := range group {
				assert.Equal(t, digest, baf.Digest(), "both blocks carry the same digest")
				assert.Equal(t, p, baf.Primary(), "each block groups a single primary")
			}
			seenPrimaries[p]++
		}
		assert.Equal(t, 1, seenPrimaries[arma_types.PartyID(1)])
		assert.Equal(t, 1, seenPrimaries[arma_types.PartyID(6)])
	})
}

// TestConsenterIndex verifies that Index records the batches of a decision's data blocks under the
// decision's number, and skips the config block that ends a config decision, which carries no batch.
func TestConsenterIndex(t *testing.T) {
	consenter := createConsenter(testutil.CreateLogger(t, 0))
	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	batches := []arma_types.BatchID{
		state.NewAvailableBatch(1, 0, 4, []byte{1}),
		state.NewAvailableBatch(2, 1, 9, []byte{2}),
	}
	hdr := headerWithBatches(t, 7, batches...)
	configReq := protoutil.MarshalOrPanic(tx.CreateStructuredConfigEnvelope([]byte{3}))
	configBlock, err := consensus.CreateConfigCommonBlock(2, nil, 0, 7, 3, 2, configReq)
	require.NoError(t, err)
	hdr.AvailableCommonBlocks = append(hdr.AvailableCommonBlocks, configBlock)
	hdr.DecisionNumOfLastConfigBlock = 7

	consenter.Index(hdr)

	require.Equal(t, 1, db.PutCallCount())
	ids, decision := db.PutArgsForCall(0)
	assert.Equal(t, arma_types.DecisionNum(7), decision)
	require.Len(t, ids, len(batches))
	for i, id := range ids {
		assert.True(t, arma_types.BatchIDEqual(batches[i], id), "batch %d: got %s", i, arma_types.BatchIDToString(id))
	}
}

// TestConsenterIndexDecisionWithoutConfig verifies that Index records every available block of a decision
// that carries no config block, including its last one.
func TestConsenterIndexDecisionWithoutConfig(t *testing.T) {
	consenter := createConsenter(testutil.CreateLogger(t, 0))
	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	batches := []arma_types.BatchID{
		state.NewAvailableBatch(1, 0, 4, []byte{1}),
		state.NewAvailableBatch(2, 1, 9, []byte{2}),
	}
	hdr := headerWithBatches(t, 7, batches...)
	hdr.DecisionNumOfLastConfigBlock = 3

	consenter.Index(hdr)

	require.Equal(t, 1, db.PutCallCount())
	ids, _ := db.PutArgsForCall(0)
	require.Len(t, ids, len(batches))
	for i, id := range ids {
		assert.True(t, arma_types.BatchIDEqual(batches[i], id), "batch %d: got %s", i, arma_types.BatchIDToString(id))
	}
}

// TestConsenterIndexGenesis verifies that Index records nothing for the genesis decision, whose only
// available block is the genesis config block.
func TestConsenterIndexGenesis(t *testing.T) {
	consenter := createConsenter(testutil.CreateLogger(t, 0))
	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	genesis := tx.CreateConfigBlock(0, []byte{3})
	genesis.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = utils.GenesisBlockMetadataBytes()

	consenter.Index(&state.Header{Num: 0, DecisionNumOfLastConfigBlock: 0, AvailableCommonBlocks: []*common.Block{genesis}})

	assert.Zero(t, db.PutCallCount())
}

// TestConsenterIndexUsesOnlyVerifiedFields verifies that Index takes the batch ID and the data/config
// distinction from the ORDERER metadata and the header, which proposal verification pins, and does not
// depend on the SIGNATURES metadata or the data of an available block.
func TestConsenterIndexUsesOnlyVerifiedFields(t *testing.T) {
	batch := state.NewAvailableBatch(1, 0, 4, []byte{1})

	for _, tst := range []struct {
		name   string
		mutate func(*common.Block)
	}{
		{
			name: "unparsable signatures metadata",
			mutate: func(b *common.Block) {
				b.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = []byte{0xff, 0xff, 0xff}
			},
		},
		{
			name: "data that looks like a config block",
			mutate: func(b *common.Block) {
				b.Data = &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(tx.CreateStructuredConfigEnvelope([]byte{3}))}}
				require.True(t, protoutil.IsConfigBlock(b))
			},
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			consenter := createConsenter(testutil.CreateLogger(t, 0))
			db := &mocks.FakeBatchAttestationDB{}
			consenter.DB = db

			hdr := headerWithBatches(t, 7, batch)
			tst.mutate(hdr.AvailableCommonBlocks[0])

			require.NotPanics(t, func() { consenter.Index(hdr) })

			require.Equal(t, 1, db.PutCallCount())
			ids, decision := db.PutArgsForCall(0)
			assert.Equal(t, arma_types.DecisionNum(7), decision)
			require.Len(t, ids, 1)
			assert.True(t, arma_types.BatchIDEqual(batch, ids[0]), "got %s", arma_types.BatchIDToString(ids[0]))
		})
	}
}

// TestConsenterIndexPanicsOnMalformedOrdererMetadata verifies that Index fail-stops on an available block
// whose ORDERER metadata cannot be parsed, since it cannot tell which batch the block holds.
func TestConsenterIndexPanicsOnMalformedOrdererMetadata(t *testing.T) {
	consenter := createConsenter(testutil.CreateLogger(t, 0))

	hdr := headerWithBatches(t, 7, state.NewAvailableBatch(1, 0, 4, []byte{1}))
	hdr.AvailableCommonBlocks[0].Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = []byte{0xff, 0xff, 0xff}

	assert.Panics(t, func() { consenter.Index(hdr) })
}

// headerWithBatches returns the header of decision num whose available blocks hold the given batches.
func headerWithBatches(t *testing.T, num arma_types.DecisionNum, batches ...arma_types.BatchID) *state.Header {
	hdr := &state.Header{Num: num}
	for i, batch := range batches {
		block, err := consensus.CreateDataCommonBlock(uint64(i), nil, batch, 1, num, len(batches), i, 0)
		require.NoError(t, err)
		hdr.AvailableCommonBlocks = append(hdr.AvailableCommonBlocks, block)
	}
	return hdr
}

func createConsenter(logger *flogging.FabricLogger) *consensus.Consenter {
	consenter := &consensus.Consenter{
		Logger: logger,
		DB:     &mocks.FakeBatchAttestationDB{},
	}

	return consenter
}
