/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"math"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
)

var (
	initialState = consensus_state.State{
		N:          4,
		Threshold:  2,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
	}

	complaint = consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 1,
			Term:  1,
		},
		Signer:    3,
		Signature: []byte{4},
	}
)

func TestStateSerializeDeserialize(t *testing.T) {
	s := consensus_state.State{
		N:          4,
		Threshold:  2,
		Quorum:     3,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
		AppContext: make([]byte, 64),
	}

	bytes := s.Serialize()

	s2 := consensus_state.State{}

	s2.Deserialize(bytes, nil)

	assert.Equal(t, s, s2)
}

func TestComplaintSerialization(t *testing.T) {
	c := consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    3,
		Signature: []byte{4},
		Reason:    "abc",
	}

	var c2 consensus_state.Complaint

	err := c2.FromBytes(c.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, c, c2)

	// check with no reason
	c.Reason = ""
	err = c2.FromBytes(c.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, c, c2)

	// check with long reason
	longReason := make([]byte, 2*math.MaxUint16)
	c.Reason = string(longReason)
	err = c2.FromBytes(c.Bytes())
	assert.NoError(t, err)

	shorterReason := make([]byte, math.MaxUint16)
	assert.Equal(t, string(shorterReason), c2.Reason)

	assert.Equal(t, c.Signer, c2.Signer)
	assert.Equal(t, c.ShardTerm, c2.ShardTerm)
	assert.Equal(t, c.Signature, c2.Signature)
}

func TestControlEventSerialization(t *testing.T) {
	// Serialization and deserialization of ControlEvent with Complaint
	ce := consensus_state.ControlEvent{nil, &complaint}

	var ce2 consensus_state.ControlEvent

	err := ce2.FromBytes(ce.Bytes(), nil)
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), nil, 0, [][]uint8{}, 0)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}

	bafd := consensus_state.BAFDeserialize{}

	ce2.Complaint = nil
	err = ce2.FromBytes(ce.Bytes(), bafd.Deserialize)
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)
}

func TestCollectAndDeduplicateEvents(t *testing.T) {
	state := initialState
	ce := consensus_state.ControlEvent{nil, &complaint}
	ce2 := consensus_state.ControlEvent{nil, &complaint}
	logger := testutil.CreateLogger(t, 0)

	// Add a valid Complaint and ensure no duplicates are accepted in the same round
	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce, ce2)

	expectedState := consensus_state.State{
		N:          4,
		Threshold:  2,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
		Complaints: []consensus_state.Complaint{complaint},
	}

	assert.Equal(t, state, expectedState)

	// Handle duplicate Complaint
	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle Complaint with invalid shard
	c := consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 2,
			Term:  1,
		},
		Signer:    2,
		Signature: []byte{4},
	}

	ce = consensus_state.ControlEvent{nil, &c}
	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle Complaint with invalid term
	c = consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    2,
		Signature: []byte{4},
	}

	ce = consensus_state.ControlEvent{nil, &c}
	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Update state with a valid BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), nil, 0, [][]uint8{}, 0)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}
	expectedState.Pending = append(expectedState.Pending, baf)

	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle duplicate BAF
	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle BAF with invalid Shard
	baf2 := types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(3), nil, 0, [][]uint8{}, 0)
	baf2.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf2}

	consensus_state.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)
}

func TestPrimaryRotateDueToComplaints(t *testing.T) {
	state := consensus_state.State{
		N:          4,
		Threshold:  2,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
		ShardCount: 2,
		Complaints: []consensus_state.Complaint{
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3},
		},
	}

	logger := testutil.CreateLogger(t, 0)

	consensus_state.PrimaryRotateDueToComplaints(&state, logger)

	// Check that the term for shard 1 has been incremented
	expectedShards := []consensus_state.ShardTerm{{Shard: 1, Term: 2}, {Shard: 2, Term: 1}}
	assert.Equal(t, expectedShards, state.Shards)

	assert.Empty(t, state.Complaints)
}

func TestCleanupOldComplaints(t *testing.T) {
	state := consensus_state.State{
		Shards: []consensus_state.ShardTerm{{Shard: 1, Term: 2}},
		Complaints: []consensus_state.Complaint{
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2}, // Old complaint
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 2}, Signer: 3}, // Valid complaint
		},
	}

	logger := testutil.CreateLogger(t, 0)

	consensus_state.CleanupOldComplaints(&state, logger)

	expectedComplaints := []consensus_state.Complaint{
		{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 2}, Signer: 3},
	}
	assert.Equal(t, expectedComplaints, state.Complaints)
}

func TestCleanupOldAttestations(t *testing.T) {
	state := initialState

	// Test condition where the threshold is not met
	baf1 := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2}, types.PartyID(2), nil, 1, [][]byte{{1, 2}, {3, 4}}, 0)
	baf1.SetSignature([]byte{1})
	logger := testutil.CreateLogger(t, 0)

	state.Pending = append(state.Pending, baf1)
	consensus_state.CleanupOldAttestations(&state, logger)

	assert.Len(t, state.Pending, 1)

	// Test condition where the threshold is met
	baf2 := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3, 4}, types.PartyID(3), nil, 1, [][]byte{{3, 4}}, 0)
	baf2.SetSignature([]byte{1})

	state.Pending = append(state.Pending, baf2)
	consensus_state.CleanupOldAttestations(&state, logger)

	assert.Len(t, state.Pending, 1)
	assert.Equal(t, baf1, state.Pending[0])
}
