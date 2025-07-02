/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core_test

import (
	"math"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
)

var (
	initialState = core.State{
		N:          4,
		Threshold:  2,
		Shards:     []core.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
	}

	complaint = core.Complaint{
		ShardTerm: core.ShardTerm{
			Shard: 1,
			Term:  1,
		},
		Signer:    3,
		Signature: []byte{4},
	}

	baf = types.NewSimpleBatchAttestationFragment(
		types.ShardID(1),
		types.PartyID(1),
		types.BatchSequence(1),
		[]byte{3},
		types.PartyID(2),
		[]uint8{4},
		0,
		[][]uint8{},
	)
)

func TestStateSerializeDeserialize(t *testing.T) {
	s := core.State{
		N:          4,
		Threshold:  2,
		Quorum:     3,
		Shards:     []core.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
		AppContext: make([]byte, 64),
	}

	bytes := s.Serialize()

	s2 := core.State{}

	s2.Deserialize(bytes, nil)

	assert.Equal(t, s, s2)
}

func TestComplaintSerialization(t *testing.T) {
	c := core.Complaint{
		ShardTerm: core.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    3,
		Signature: []byte{4},
		Reason:    "abc",
	}

	var c2 core.Complaint

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
	ce := core.ControlEvent{nil, &complaint}

	var ce2 core.ControlEvent

	err := ce2.FromBytes(ce.Bytes(), nil)
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with BAF
	ce = core.ControlEvent{baf, nil}

	bafd := state.BAFDeserializer{}

	ce2.Complaint = nil
	err = ce2.FromBytes(ce.Bytes(), bafd.Deserialize)
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)
}

func TestCollectAndDeduplicateEvents(t *testing.T) {
	state := initialState
	ce := core.ControlEvent{nil, &complaint}
	ce2 := core.ControlEvent{nil, &complaint}
	logger := testutil.CreateLogger(t, 0)

	// Add a valid Complaint and ensure no duplicates are accepted in the same round
	core.CollectAndDeduplicateEvents(&state, logger, ce, ce2)

	expectedState := core.State{
		N:          4,
		Threshold:  2,
		Shards:     []core.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
		Complaints: []core.Complaint{complaint},
	}

	assert.Equal(t, state, expectedState)

	// Handle duplicate Complaint
	core.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle Complaint with invalid shard
	c := core.Complaint{
		ShardTerm: core.ShardTerm{
			Shard: 2,
			Term:  1,
		},
		Signer:    2,
		Signature: []byte{4},
	}

	ce = core.ControlEvent{nil, &c}
	core.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle Complaint with invalid term
	c = core.Complaint{
		ShardTerm: core.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    2,
		Signature: []byte{4},
	}

	ce = core.ControlEvent{nil, &c}
	core.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Update state with a valid BAF
	ce = core.ControlEvent{baf, nil}
	expectedState.Pending = append(expectedState.Pending, baf)

	core.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle duplicate BAF
	core.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle BAF with invalid Shard
	baf2 := types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(3), []uint8{4}, 0, [][]uint8{})
	ce = core.ControlEvent{baf2, nil}

	core.CollectAndDeduplicateEvents(&state, logger, ce)
	assert.Equal(t, state, expectedState)
}

func TestPrimaryRotateDueToComplaints(t *testing.T) {
	state := core.State{
		N:          4,
		Threshold:  2,
		Shards:     []core.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
		ShardCount: 2,
		Complaints: []core.Complaint{
			{ShardTerm: core.ShardTerm{Shard: 1, Term: 1}, Signer: 2},
			{ShardTerm: core.ShardTerm{Shard: 1, Term: 1}, Signer: 3},
		},
	}

	logger := testutil.CreateLogger(t, 0)

	core.PrimaryRotateDueToComplaints(&state, logger)

	// Check that the term for shard 1 has been incremented
	expectedShards := []core.ShardTerm{{Shard: 1, Term: 2}, {Shard: 2, Term: 1}}
	assert.Equal(t, expectedShards, state.Shards)

	assert.Empty(t, state.Complaints)
}

func TestCleanupOldComplaints(t *testing.T) {
	state := core.State{
		Shards: []core.ShardTerm{{Shard: 1, Term: 2}},
		Complaints: []core.Complaint{
			{ShardTerm: core.ShardTerm{Shard: 1, Term: 1}, Signer: 2}, // Old complaint
			{ShardTerm: core.ShardTerm{Shard: 1, Term: 2}, Signer: 3}, // Valid complaint
		},
	}

	logger := testutil.CreateLogger(t, 0)

	core.CleanupOldComplaints(&state, logger)

	expectedComplaints := []core.Complaint{
		{ShardTerm: core.ShardTerm{Shard: 1, Term: 2}, Signer: 3},
	}
	assert.Equal(t, expectedComplaints, state.Complaints)
}

func TestCleanupOldAttestations(t *testing.T) {
	state := initialState

	// Test condition where the threshold is not met
	baf1 := types.NewSimpleBatchAttestationFragment(
		types.ShardID(1),
		types.PartyID(1),
		types.BatchSequence(1),
		[]byte{1, 2},
		types.PartyID(2),
		[]byte{},
		1,
		[][]byte{{1, 2}, {3, 4}},
	)
	logger := testutil.CreateLogger(t, 0)

	state.Pending = append(state.Pending, baf1)
	core.CleanupOldAttestations(&state, logger)

	assert.Len(t, state.Pending, 1)

	// Test condition where the threshold is met
	baf2 := types.NewSimpleBatchAttestationFragment(
		types.ShardID(1),
		types.PartyID(1),
		types.BatchSequence(1),
		[]byte{3, 4},
		types.PartyID(3),
		[]byte{},
		1,
		[][]byte{{3, 4}},
	)

	state.Pending = append(state.Pending, baf2)
	core.CleanupOldAttestations(&state, logger)

	assert.Len(t, state.Pending, 1)
	assert.Equal(t, baf1, state.Pending[0])
}
