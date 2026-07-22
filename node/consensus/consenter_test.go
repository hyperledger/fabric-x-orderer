/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/assert"
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

	consenter.Index([][]byte{})
	assert.Zero(t, db.PutCallCount())

	// Test a valid event below threshold
	db.ExistsReturns(false)
	newState, batchAttestations, _ = consenter.SimulateStateTransition(s, 0, events)
	assert.Empty(t, batchAttestations)
	assert.Len(t, newState.Pending, 1)

	consenter.Index([][]byte{})
	assert.Zero(t, db.PutCallCount())

	// Test valid events meeting the threshold
	ba2 := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(3), 0, 0, nil)
	ba2.SetSignature([]byte{1})
	events = append(events, (&state.ControlEvent{BAF: ba2}).Bytes())

	newState, batchAttestations, _ = consenter.SimulateStateTransition(s, 0, events)
	assert.Len(t, batchAttestations[0], 2)
	assert.Empty(t, newState.Pending)

	consenter.Index([][]byte{batchAttestations[0][0].Digest()})
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
	consenter.Index([][]byte{})
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

func createConsenter(logger *flogging.FabricLogger) *consensus.Consenter {
	consenter := &consensus.Consenter{
		Logger: logger,
		DB:     &mocks.FakeBatchAttestationDB{},
	}

	return consenter
}
