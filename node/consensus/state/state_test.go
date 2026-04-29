/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"math"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/assert"
)

var (
	initialState = consensus_state.State{
		N:         4,
		Threshold: 2,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
	}

	complaint = consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 1,
			Term:  1,
		},
		Signer:    3,
		Signature: []byte{4},
		ConfigSeq: 10,
	}
)

func TestStateSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name  string
		state consensus_state.State
	}{
		{
			name: "full state with all fields",
			state: consensus_state.State{
				N:          4,
				Threshold:  2,
				Quorum:     3,
				Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				AppContext: make([]byte, 64),
			},
		},
		{
			name: "state with empty app context",
			state: consensus_state.State{
				N:          4,
				Threshold:  2,
				Quorum:     3,
				Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				AppContext: []byte{}, // empty slice, never nil after proto serialization
			},
		},
		{
			name: "state with multiple shards",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards: []consensus_state.ShardTerm{
					{Shard: 1, Term: 1},
					{Shard: 2, Term: 3},
					{Shard: 3, Term: 5},
				},
				AppContext: []byte{1, 2, 3},
			},
		},
		{
			name: "state with complaints",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				Complaints: []consensus_state.Complaint{
					{
						ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1},
						Signer:    2,
						Signature: []byte{1, 2, 3, 4},
						Reason:    "test complaint",
						ConfigSeq: 10,
					},
				},
				AppContext: []byte{5, 6, 7},
			},
		},
		{
			name: "minimal state with zero values",
			state: consensus_state.State{
				N:          0,
				Threshold:  0,
				Quorum:     0,
				Shards:     nil,
				Pending:    nil,
				Complaints: nil,
				AppContext: []byte{},
			},
		},
		{
			name: "state with large app context",
			state: consensus_state.State{
				N:          10,
				Threshold:  7,
				Quorum:     8,
				Shards:     []consensus_state.ShardTerm{{Shard: 5, Term: 100}},
				AppContext: make([]byte, 1024), // 1KB
			},
		},
		{
			name: "state with high term numbers",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards: []consensus_state.ShardTerm{
					{Shard: 1, Term: 999999},
					{Shard: 2, Term: 1000000},
				},
				AppContext: []byte{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			bytes := tt.state.Serialize()
			assert.NotEmpty(t, bytes, "serialized bytes should not be empty")

			// Deserialize
			var deserialized consensus_state.State
			err := deserialized.Deserialize(bytes, nil)
			assert.NoError(t, err, "deserialization should not fail")

			// Compare
			assert.Equal(t, tt.state, deserialized, "deserialized state should match original")
			assert.Equal(t, tt.state.String(), deserialized.String(), "string representations should match")
		})
	}
}

func TestStateString(t *testing.T) {
	s := consensus_state.State{
		N:          4,
		Threshold:  2,
		Quorum:     3,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 3}},
		AppContext: make([]byte, 64),
		Complaints: []consensus_state.Complaint{
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2},
		},
	}

	// Test that String() returns a non-empty string
	str := s.String()
	assert.NotEmpty(t, str)

	// Test that the string contains key information
	assert.Contains(t, str, "N: 4")
	assert.Contains(t, str, "Threshold: 2")
	assert.Contains(t, str, "Quorum: 3")
	assert.Contains(t, str, "ShardCount: 2")
	assert.Contains(t, str, "Pending: none")
	assert.Contains(t, str, "Complaints: 1")
	assert.Contains(t, str, "Complaint: Signer: 2")

	// Test with more than 5 BAFs
	s2 := consensus_state.State{
		N:          4,
		Threshold:  2,
		Quorum:     3,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		AppContext: make([]byte, 64),
		Pending: []types.BatchAttestationFragment{
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1}, types.PartyID(2), 0, 0),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(2), []byte{2}, types.PartyID(2), 0, 0),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(3), []byte{3}, types.PartyID(2), 0, 0),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(4), []byte{4}, types.PartyID(2), 0, 0),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(5), []byte{5}, types.PartyID(2), 0, 0),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(6), []byte{6}, types.PartyID(2), 0, 0),
		},

		Complaints: []consensus_state.Complaint{
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 1},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 4},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 5},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 6},
		},
	}

	str2 := s2.String()
	assert.NotEmpty(t, str2)
	assert.Contains(t, str2, "Pending: 6")
	assert.Contains(t, str2, "... and 1 more")
	assert.Contains(t, str2, "Complaints: 6")
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
		ConfigSeq: 20,
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
	ce := consensus_state.ControlEvent{Complaint: &complaint}

	var ce2 consensus_state.ControlEvent

	err := ce2.FromBytes(ce.Bytes(), nil)
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}

	bafd := consensus_state.BAFDeserialize{}

	ce2.Complaint = nil
	err = ce2.FromBytes(ce.Bytes(), bafd.Deserialize)
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with ConfigRequest
	cr := &consensus_state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload:   []byte("config-payload"),
			Signature: []byte("config-signature"),
		},
	}
	ce = consensus_state.ControlEvent{ConfigRequest: cr}

	var ce3 consensus_state.ControlEvent
	err = ce3.FromBytes(ce.Bytes(), bafd.Deserialize)
	assert.NoError(t, err)
	assert.NotNil(t, ce3.ConfigRequest)
	assert.Equal(t, cr.Envelope.Payload, ce3.ConfigRequest.Envelope.Payload)
	assert.Equal(t, cr.Envelope.Signature, ce3.ConfigRequest.Envelope.Signature)
}

func TestCollectAndDeduplicateEvents(t *testing.T) {
	state := initialState
	ce := consensus_state.ControlEvent{Complaint: &complaint}
	ce2 := consensus_state.ControlEvent{Complaint: &complaint}
	logger := testutil.CreateLogger(t, 0)

	// Add a valid Complaint and ensure no duplicates are accepted in the same round
	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce, ce2)

	expectedState := consensus_state.State{
		N:          4,
		Threshold:  2,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		Complaints: []consensus_state.Complaint{complaint},
	}

	assert.Equal(t, state, expectedState)

	// Handle duplicate Complaint
	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
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

	ce = consensus_state.ControlEvent{Complaint: &c}
	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
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

	ce = consensus_state.ControlEvent{Complaint: &c}
	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
	assert.Equal(t, state, expectedState)

	// Update state with a valid BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}
	expectedState.Pending = append(expectedState.Pending, baf)

	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle duplicate BAF
	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle BAF with invalid Shard
	baf2 := types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(3), 0, 0)
	baf2.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf2}

	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
	assert.Equal(t, state, expectedState)
}

func TestFilterPendingEventsWithDiffConfigSeq(t *testing.T) {
	state := consensus_state.State{
		N:         4,
		Threshold: 2,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
		Complaints: []consensus_state.Complaint{
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3},
		},
	}

	logger := testutil.CreateLogger(t, 0)

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 0, logger)

	assert.Len(t, state.Complaints, 2)

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 1, logger)

	assert.Len(t, state.Complaints, 0)

	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0))

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 0, logger)

	assert.Len(t, state.Pending, 1)

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 1, logger)

	assert.Len(t, state.Pending, 0)

	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 1, 0))
	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 2, 0))
	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 3, 0))

	state.Complaints = append(state.Complaints, consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2, ConfigSeq: 1})
	state.Complaints = append(state.Complaints, consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2, ConfigSeq: 2})
	state.Complaints = append(state.Complaints, consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2, ConfigSeq: 3})

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 2, logger)
	assert.Len(t, state.Pending, 1)
	assert.Equal(t, types.ConfigSequence(2), state.Pending[0].ConfigSequence())
	assert.Len(t, state.Complaints, 1)
	assert.Equal(t, types.ConfigSequence(2), state.Complaints[0].ConfigSeq)
}

func TestPrimaryRotateDueToComplaints(t *testing.T) {
	state := consensus_state.State{
		N:         4,
		Threshold: 2,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
		Complaints: []consensus_state.Complaint{
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2},
			{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3},
		},
	}

	logger := testutil.CreateLogger(t, 0)

	consensus_state.PrimaryRotateDueToComplaints(&state, 0, logger)

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

	consensus_state.CleanupOldComplaints(&state, 0, logger)

	expectedComplaints := []consensus_state.Complaint{
		{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 2}, Signer: 3},
	}
	assert.Equal(t, expectedComplaints, state.Complaints)
}
