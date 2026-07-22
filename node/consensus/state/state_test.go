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
				Threshold:  4,
				Quorum:     7,
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
		{
			name: "state with single pending BAF",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				Pending: []types.BatchAttestationFragment{
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil)
						baf.SetSignature([]byte{}) // Set empty signature to match deserialization behavior
						return baf
					}(),
				},
				AppContext: []byte{10, 20, 30},
			},
		},
		{
			name: "state with multiple pending BAFs",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 2}},
				Pending: []types.BatchAttestationFragment{
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(2), types.BatchSequence(2), []byte{4, 5, 6}, types.PartyID(3), 1, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(3), types.BatchSequence(3), []byte{7, 8, 9}, types.PartyID(1), 2, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
				},
				AppContext: []byte{100, 200},
			},
		},
		{
			name: "state with pending BAFs and complaints",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				Pending: []types.BatchAttestationFragment{
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(5), []byte{11, 22, 33}, types.PartyID(2), 5, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
				},
				Complaints: []consensus_state.Complaint{
					{
						ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1},
						Signer:    3,
						Signature: []byte{99, 88, 77},
						Reason:    "primary timeout",
						ConfigSeq: 5,
					},
				},
				AppContext: []byte{50, 60, 70},
			},
		},
		{
			name: "state with pending BAFs with signatures",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				Pending: []types.BatchAttestationFragment{
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(10), []byte{44, 55, 66}, types.PartyID(2), 3, 0, nil)
						baf.SetSignature([]byte{111, 222, 233})
						return baf
					}(),
				},
				AppContext: []byte{},
			},
		},
		{
			name: "state with many pending BAFs",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
				Pending: []types.BatchAttestationFragment{
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1}, types.PartyID(2), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(2), types.BatchSequence(2), []byte{2}, types.PartyID(3), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(3), types.BatchSequence(3), []byte{3}, types.PartyID(1), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(4), []byte{4}, types.PartyID(2), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(2), types.BatchSequence(5), []byte{5}, types.PartyID(3), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
					func() types.BatchAttestationFragment {
						baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(3), types.BatchSequence(6), []byte{6}, types.PartyID(1), 0, 0, nil)
						baf.SetSignature([]byte{})
						return baf
					}(),
				},
				AppContext: []byte{255},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			bytes := tt.state.Serialize()

			// For minimal state with all zero values, protobuf produces an empty byte slice
			// which is valid behavior (all fields have default values)
			if tt.name != "minimal state with zero values" {
				assert.NotEmpty(t, bytes, "serialized bytes should not be empty")
			}

			// Deserialize
			var deserialized consensus_state.State
			err := deserialized.Deserialize(bytes)
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
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1}, types.PartyID(2), 0, 0, nil),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(2), []byte{2}, types.PartyID(2), 0, 0, nil),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(3), []byte{3}, types.PartyID(2), 0, 0, nil),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(4), []byte{4}, types.PartyID(2), 0, 0, nil),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(5), []byte{5}, types.PartyID(2), 0, 0, nil),
			types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(6), []byte{6}, types.PartyID(2), 0, 0, nil),
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

	err := ce2.FromBytes(ce.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0, nil)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}

	ce2.Complaint = nil
	err = ce2.FromBytes(ce.Bytes())
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
	err = ce3.FromBytes(ce.Bytes())
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
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0, nil)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}
	expectedState.Pending = append(expectedState.Pending, baf)

	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle duplicate BAF
	consensus_state.CollectAndDeduplicateEvents(&state, 0, logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle BAF with invalid Shard
	baf2 := types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(3), 0, 0, nil)
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

	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0, nil))

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 0, logger)

	assert.Len(t, state.Pending, 1)

	consensus_state.FilterPendingEventsWithDiffConfigSeq(&state, 1, logger)

	assert.Len(t, state.Pending, 0)

	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 1, 0, nil))
	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 2, 0, nil))
	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 3, 0, nil))

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

func TestDetectEquivocation(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	t.Run("no equivocation - single digest per batch attestation", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term should not change - no equivocation detected
		assert.Equal(t, uint64(1), state.Shards[0].Term)
	})

	t.Run("equivocation detected - multiple digests for same seq/shard/primary", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Primary 1 sends different digests for the same sequence in shard 1
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{4, 5, 6}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term should be incremented due to equivocation
		assert.Equal(t, uint64(2), state.Shards[0].Term)
	})

	t.Run("equivocation with multiple signers on different digests", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 5}},
			Pending: []types.BatchAttestationFragment{
				// Primary 1 (term 5 % 4 = 1) sends three different digests
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(10), []byte{1, 1, 1}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(10), []byte{2, 2, 2}, types.PartyID(3), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(10), []byte{3, 3, 3}, types.PartyID(4), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term should be incremented
		assert.Equal(t, uint64(6), state.Shards[0].Term)
	})

	t.Run("no equivocation - different sequences", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Different sequences - not equivocation
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(2), []byte{4, 5, 6}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term should not change
		assert.Equal(t, uint64(1), state.Shards[0].Term)
	})

	t.Run("no equivocation - different shards", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Different shards - not equivocation
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), []byte{4, 5, 6}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Terms should not change
		assert.Equal(t, uint64(1), state.Shards[0].Term)
		assert.Equal(t, uint64(1), state.Shards[1].Term)
	})

	t.Run("no equivocation - different primaries", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Different primaries - not equivocation
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(2), types.BatchSequence(1), []byte{4, 5, 6}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term should not change
		assert.Equal(t, uint64(1), state.Shards[0].Term)
	})

	t.Run("equivocation in multiple shards", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Equivocation in shard 1
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{4, 5, 6}, types.PartyID(3), 0, 0, nil),
				// Equivocation in shard 2
				types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(5), []byte{7, 8, 9}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(5), []byte{10, 11, 12}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Both terms should be incremented
		assert.Equal(t, uint64(2), state.Shards[0].Term)
		assert.Equal(t, uint64(2), state.Shards[1].Term)
	})

	t.Run("empty pending - no equivocation", func(t *testing.T) {
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending:   []types.BatchAttestationFragment{},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term should not change
		assert.Equal(t, uint64(1), state.Shards[0].Term)
	})

	t.Run("no rotation when signer equals primary - self-signed BAFs ignored", func(t *testing.T) {
		// A non-primary node can try to force a term rotation by sending two BAFs where
		// Primary() == Signer() == self with different digests.  Because the primary would
		// never re-broadcast its own batch as a secondary voter, such BAFs are filtered out
		// and must not trigger equivocation detection.
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Both BAFs claim Primary==Signer==2: self-signed, ignored for equivocation.
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(2), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(2), types.BatchSequence(1), []byte{4, 5, 6}, types.PartyID(2), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Term must not change: self-signed BAFs cannot be used as equivocation evidence.
		assert.Equal(t, uint64(1), state.Shards[0].Term)
	})

	t.Run("rotation still triggered when secondaries sign conflicting digests from the same primary", func(t *testing.T) {
		// Genuine equivocation: two different secondaries received different batches from
		// the same primary and both signed their BAF (Signer != Primary).
		state := consensus_state.State{
			N:         4,
			Threshold: 2,
			Quorum:    3,
			Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
			Pending: []types.BatchAttestationFragment{
				// Signer 2 attests digest {1,2,3} for primary 1.
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{1, 2, 3}, types.PartyID(2), 0, 0, nil),
				// Signer 3 attests a different digest {4,5,6} for the same primary 1 / seq 1.
				types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{4, 5, 6}, types.PartyID(3), 0, 0, nil),
			},
		}

		consensus_state.DetectEquivocation(&state, 0, logger)

		// Equivocation is detected: term must be incremented.
		assert.Equal(t, uint64(2), state.Shards[0].Term)
	})
}

// TestExtractBatchAttestationsFromPending_EquivocationDigestLeak demonstrates the bug where,
// when a primary equivocates, a below-threshold (under-attested) digest's fragment is extracted
// alongside the digest that actually reached the signature threshold.
//
// ExtractBatchAttestationsFromPending decides to extract a <seq, shard, primary> group when at
// least one of its digests reaches threshold, but then copies every pending BAF for that key
// regardless of digest. Downstream, aggregateFragments groups these by <seq, shard> and consensus
// collapses each group to ba[0] (see node/consensus/consensus.go). Because the under-attested
// fragment is placed first in Pending here, it would become ba[0] and the committed block's
// DataHash would come from a digest that never reached a threshold of signatures.
//
// Desired behaviour (this test asserts it, so it FAILS before the fix): only fragments belonging
// to threshold-reaching digests are extracted.
func TestExtractBatchAttestationsFromPending_EquivocationDigestLeak(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	underAttestedDigest := []byte{9, 9, 9}
	thresholdDigest := []byte{1, 2, 3}
	primary := types.PartyID(1)
	shardID := types.ShardID(1)
	seq := types.BatchSequence(1)

	state := consensus_state.State{
		N:         4,
		Threshold: 2,
		Quorum:    3,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		Pending: []types.BatchAttestationFragment{
			// Equivocating digest with a single signer (below the threshold of 2).
			// Placed first so it would become ba[0] downstream.
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, underAttestedDigest, types.PartyID(4), 0, 1, nil),
			// Legitimate digest reaching threshold (2 distinct signers).
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, thresholdDigest, types.PartyID(2), 0, 1, nil),
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, thresholdDigest, types.PartyID(3), 0, 1, nil),
		},
	}

	consensus_state.DetectEquivocation(&state, 0, logger)
	// Equivocation is detected: term must be incremented.
	assert.Equal(t, uint64(2), state.Shards[0].Term)

	extracted := consensus_state.ExtractBatchAttestationsFromPending(&state, logger)

	// Only the threshold-reaching digest should be extracted; the under-attested digest must not leak.
	for _, baf := range extracted {
		assert.Equal(t, thresholdDigest, baf.Digest(),
			"extracted a fragment for under-attested digest %x; only threshold-reaching digests should be extracted", baf.Digest())
	}
	assert.Len(t, extracted, 2, "expected exactly the two threshold-digest fragments to be extracted")

	thresholdDigest2 := []byte{4, 5, 6}
	state2 := consensus_state.State{
		N:         4,
		Threshold: 2,
		Quorum:    3,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		Pending: []types.BatchAttestationFragment{
			// Legitimate digest reaching threshold (2 distinct signers).
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, thresholdDigest, types.PartyID(1), 0, 1, nil),
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, thresholdDigest, types.PartyID(4), 0, 1, nil),
			// Another legitimate digest reaching threshold (2 distinct signers).
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, thresholdDigest2, types.PartyID(2), 0, 1, nil),
			types.NewSimpleBatchAttestationFragment(shardID, primary, seq, thresholdDigest2, types.PartyID(3), 0, 1, nil),
		},
	}

	consensus_state.DetectEquivocation(&state2, 0, logger)
	// Equivocation is detected: term must be incremented.
	assert.Equal(t, uint64(2), state.Shards[0].Term)

	extracted2 := consensus_state.ExtractBatchAttestationsFromPending(&state2, logger)
	assert.Len(t, extracted2, 4, "expected all fragments to be extracted")
}
