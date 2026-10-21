/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"math"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
		{
			name: "state with assembler heights and prune points",
			state: consensus_state.State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 2}},
				AssemblerHeights: []consensus_state.AssemblerHeight{
					{Party: 1, DecisionNum: 7},
					{Party: 3, DecisionNum: 12},
				},
				PrunePoints: []consensus_state.PrunePoint{
					{Shard: 1, Primary: 2, Seq: 40},
					{Shard: 2, Primary: 1, Seq: 5},
				},
				DurableWatermark: 7,
				AppContext:       []byte{1},
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
	assert.Contains(t, str, "AssemblerHeights: 0")
	assert.Contains(t, str, "PrunePoints: 0")
	assert.Contains(t, str, "DurableWatermark: 0")
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

func TestCollectAndDeduplicateEvents(t *testing.T) {
	state := initialState
	ce := consensus_state.ControlEvent{Complaint: &complaint}
	ce2 := consensus_state.ControlEvent{Complaint: &complaint}
	logger := testutil.CreateLogger(t, 0)

	// Add a valid Complaint and ensure no duplicates are accepted in the same round
	state.CollectAndDeduplicateEvents(logger, ce, ce2)

	expectedState := consensus_state.State{
		N:          4,
		Threshold:  2,
		Shards:     []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		Complaints: []consensus_state.Complaint{complaint},
	}

	assert.Equal(t, state, expectedState)

	// Handle duplicate Complaint
	state.CollectAndDeduplicateEvents(logger, ce)
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
	state.CollectAndDeduplicateEvents(logger, ce)
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
	state.CollectAndDeduplicateEvents(logger, ce)
	assert.Equal(t, state, expectedState)

	// Update state with a valid BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0, nil)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}
	expectedState.Pending = append(expectedState.Pending, baf)

	state.CollectAndDeduplicateEvents(logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle duplicate BAF
	state.CollectAndDeduplicateEvents(logger, ce)
	assert.Equal(t, state, expectedState)

	// Handle BAF with invalid Shard
	baf2 := types.NewSimpleBatchAttestationFragment(types.ShardID(2), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(3), 0, 0, nil)
	baf2.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf2}

	state.CollectAndDeduplicateEvents(logger, ce)
	assert.Equal(t, state, expectedState)
}

// TestCollectAndDeduplicateEvents_SameSignerDifferentDigests pins the dedup unit at
// <shard, primary, seq, signer, digest>. A single secondary that reports two different digests
// for the same <shard, primary, seq> — the on-wire evidence that the primary equivocated (each
// BAF carries the primary's verified signature over its digest) — must NOT be treated as a
// duplicate: both BAFs must be retained so DetectEquivocation can see the conflict and rotate.
// An exact duplicate (same signer AND same digest) is still dropped.
//
// Before the fix, dedup keyed on <shard, primary, seq, signer> alone, so the signer's second
// digest was silently discarded and the equivocation went undetected. This test fails then.
func TestCollectAndDeduplicateEvents_SameSignerDifferentDigests(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	shard := types.ShardID(1)
	primary := types.PartyID(1)
	seq := types.BatchSequence(1)
	signer := types.PartyID(2)

	newBAF := func(digest []byte) consensus_state.ControlEvent {
		baf := types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, signer, 0, 0, nil)
		baf.SetSignature([]byte{1})
		return consensus_state.ControlEvent{BAF: baf}
	}

	state := consensus_state.State{
		N:         4,
		Threshold: 2,
		Quorum:    3,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
	}

	// Round 1: the signer reports digest X.
	state.CollectAndDeduplicateEvents(logger, newBAF([]byte{1, 2, 3}))
	assert.Len(t, state.Pending, 1)

	// Round 2: the same signer reports a re-send of X (must be dropped as a true duplicate: same
	// <shard, primary, seq, signer, digest> already pending) and a different digest Y (must be
	// retained: it is equivocation evidence, not a duplicate).
	state.CollectAndDeduplicateEvents(logger, newBAF([]byte{1, 2, 3}), newBAF([]byte{4, 5, 6}))

	assert.Len(t, state.Pending, 2)
	assert.Equal(t, []byte{1, 2, 3}, state.Pending[0].Digest())
	assert.Equal(t, []byte{4, 5, 6}, state.Pending[1].Digest())

	// The retained conflict is now visible to equivocation detection, which rotates the primary.
	state.DetectEquivocation(logger)
	assert.Equal(t, uint64(2), state.Shards[0].Term)
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

	state.FilterPendingEventsWithDiffConfigSeq(0, logger)

	assert.Len(t, state.Complaints, 2)

	state.FilterPendingEventsWithDiffConfigSeq(1, logger)

	assert.Len(t, state.Complaints, 0)

	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0, nil))

	state.FilterPendingEventsWithDiffConfigSeq(0, logger)

	assert.Len(t, state.Pending, 1)

	state.FilterPendingEventsWithDiffConfigSeq(1, logger)

	assert.Len(t, state.Pending, 0)

	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 1, 0, nil))
	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 2, 0, nil))
	state.Pending = append(state.Pending, types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 3, 0, nil))

	state.Complaints = append(state.Complaints, consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2, ConfigSeq: 1})
	state.Complaints = append(state.Complaints, consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2, ConfigSeq: 2})
	state.Complaints = append(state.Complaints, consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 2, ConfigSeq: 3})

	state.FilterPendingEventsWithDiffConfigSeq(2, logger)
	assert.Len(t, state.Pending, 1)
	assert.Equal(t, types.ConfigSequence(2), state.Pending[0].ConfigSequence())
	assert.Len(t, state.Complaints, 1)
	assert.Equal(t, types.ConfigSequence(2), state.Complaints[0].ConfigSeq)
}

// TestProcessSurfacesStaleConfigBAFsBehind verifies that Process diverts an incoming BAF that is behind
// the current config sequence into StaleConfigBAFs (rather than dropping it
// or placing it in Pending), leaves it out of BA extraction, drops BAFs ahead of the current config
// sequence, and clears the array the next round.
func TestProcessSurfacesStaleConfigBAFsBehind(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	st := consensus_state.State{
		N:         4,
		Threshold: 2,
		Shards:    []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
	}
	const cur = types.ConfigSequence(2)

	currentBAF := bafCE(1, 1, 10, []byte{1}, 2, cur, 0, nil)  // exact match -> Pending
	staleOne := bafCE(1, 1, 11, []byte{2}, 2, cur-1, 0, nil)  // one behind -> StaleConfigBAFs
	twoBehind := bafCE(1, 1, 12, []byte{3}, 2, cur-2, 0, nil) // two behind -> StaleConfigBAFs
	ahead := bafCE(1, 1, 13, []byte{4}, 2, cur+1, 0, nil)     // ahead -> dropped

	next, extracted, _ := st.Process(logger, cur, currentBAF, staleOne, twoBehind, ahead)

	// exact-match BAF collected into Pending
	require.Len(t, next.Pending, 1)
	assert.Equal(t, cur, next.Pending[0].ConfigSequence())

	// every behind BAF surfaced separately, and never extracted as a BA
	require.Len(t, next.StaleConfigBAFs, 2)
	surfaced := map[types.ConfigSequence]struct{}{}
	for _, baf := range next.StaleConfigBAFs {
		surfaced[baf.ConfigSequence()] = struct{}{}
	}
	assert.Contains(t, surfaced, cur-1)
	assert.Contains(t, surfaced, cur-2)
	assert.Empty(t, extracted)

	// the ahead BAF is dropped entirely (not surfaced, not pending)
	assert.NotContains(t, surfaced, cur+1)

	// one decision only: a subsequent Process without the stale CEs clears StaleConfigBAFs,
	// while Pending is unaffected.
	next2, _, _ := next.Process(logger, cur)
	assert.Empty(t, next2.StaleConfigBAFs)
	assert.Len(t, next2.Pending, 1)
}

// TestStateSerializeDeserializeStaleConfigBAFs verifies StaleConfigBAFs round-trips through
// Serialize/Deserialize, which is what carries it (deterministically) to batchers and into the
// VerifyProposal state byte-compare.
func TestStateSerializeDeserializeStaleConfigBAFs(t *testing.T) {
	baf := types.NewSimpleBatchAttestationFragment(1, 1, 5, []byte{7}, 2, 3, 0, nil)
	baf.SetSignature([]byte{9})
	st := consensus_state.State{
		N:               4,
		Threshold:       2,
		Quorum:          3,
		Shards:          []consensus_state.ShardTerm{{Shard: 1, Term: 1}},
		StaleConfigBAFs: []types.BatchAttestationFragment{baf},
	}

	var got consensus_state.State
	require.NoError(t, got.Deserialize(st.Serialize()))

	require.Len(t, got.StaleConfigBAFs, 1)
	assert.Equal(t, types.ConfigSequence(3), got.StaleConfigBAFs[0].ConfigSequence())
	assert.Equal(t, types.BatchSequence(5), got.StaleConfigBAFs[0].Seq())
	assert.Empty(t, got.Pending)
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

	state.PrimaryRotateDueToComplaints(logger)

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

	state.CleanupOldComplaints(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

		state.DetectEquivocation(logger)

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

	state.DetectEquivocation(logger)
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

	state2.DetectEquivocation(logger)
	// Equivocation is detected: term must be incremented.
	assert.Equal(t, uint64(2), state2.Shards[0].Term)

	extracted2 := consensus_state.ExtractBatchAttestationsFromPending(&state2, logger)
	assert.Len(t, extracted2, 4, "expected all fragments to be extracted")
}

func reportCE(
	party types.PartyID, decisionNum types.DecisionNum, configSeq types.ConfigSequence,
) consensus_state.ControlEvent {
	return consensus_state.ControlEvent{AssemblerReport: &consensus_state.AssemblerDecisionReport{
		Party:       party,
		DecisionNum: decisionNum,
		ConfigSeq:   configSeq,
		Signature:   []byte{9},
	}}
}

func TestCollectAssemblerReports(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	tests := []struct {
		name     string
		initial  []consensus_state.AssemblerHeight
		ces      []consensus_state.ControlEvent
		expected []consensus_state.AssemblerHeight
	}{
		{
			name:     "first report of a party is recorded",
			ces:      []consensus_state.ControlEvent{reportCE(2, 10, 0)},
			expected: []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 10}},
		},
		{
			name:     "higher report replaces the recorded height",
			initial:  []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 10}},
			ces:      []consensus_state.ControlEvent{reportCE(2, 15, 0)},
			expected: []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 15}},
		},
		{
			name:     "lower report is ignored",
			initial:  []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 10}},
			ces:      []consensus_state.ControlEvent{reportCE(2, 5, 0)},
			expected: []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 10}},
		},
		{
			name:     "equal report is ignored",
			initial:  []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 10}},
			ces:      []consensus_state.ControlEvent{reportCE(2, 10, 0)},
			expected: []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 10}},
		},
		{
			name:     "several reports of one party in a round reduce to the max",
			ces:      []consensus_state.ControlEvent{reportCE(1, 4, 0), reportCE(1, 9, 0), reportCE(1, 6, 0)},
			expected: []consensus_state.AssemblerHeight{{Party: 1, DecisionNum: 9}},
		},
		{
			name:    "heights are kept sorted by party",
			initial: []consensus_state.AssemblerHeight{{Party: 2, DecisionNum: 1}},
			ces:     []consensus_state.ControlEvent{reportCE(4, 3, 0), reportCE(1, 7, 0), reportCE(3, 2, 0)},
			expected: []consensus_state.AssemblerHeight{
				{Party: 1, DecisionNum: 7},
				{Party: 2, DecisionNum: 1},
				{Party: 3, DecisionNum: 2},
				{Party: 4, DecisionNum: 3},
			},
		},
		{
			name:     "non-report events are ignored",
			ces:      []consensus_state.ControlEvent{bafCE(1, 1, 1, []byte{1}, 2, 0, 0, nil), {Complaint: &complaint}},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initialState.Clone()
			s.AssemblerHeights = tt.initial
			s.CollectAssemblerReports(logger, tt.ces...)
			assert.Equal(t, tt.expected, s.AssemblerHeights)
		})
	}
}

// TestProcessRecordsAssemblerReports verifies that Process records reports of the current config
// sequence, drops reports of any other config sequence, and carries published prune points over to the
// next state (UpdatePrunePoints never computes a new one yet, so it must not clear existing ones).
func TestProcessRecordsAssemblerReports(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	const cur = types.ConfigSequence(3)

	st := initialState.Clone()
	st.PrunePoints = []consensus_state.PrunePoint{{Shard: 1, Primary: 2, Seq: 8}}

	next, _, _ := st.Process(logger, cur, reportCE(1, 20, cur), reportCE(2, 30, cur-1), reportCE(3, 40, cur+1))

	assert.Equal(t, []consensus_state.AssemblerHeight{{Party: 1, DecisionNum: 20}}, next.AssemblerHeights)
	assert.Equal(t, []consensus_state.PrunePoint{{Shard: 1, Primary: 2, Seq: 8}}, next.PrunePoints)
	assert.Empty(t, st.AssemblerHeights, "Process must not modify the receiver")
}

func TestUpdateDurableWatermark(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	heights := func(decisionNums ...types.DecisionNum) []consensus_state.AssemblerHeight {
		hs := make([]consensus_state.AssemblerHeight, len(decisionNums))
		for i, d := range decisionNums {
			hs[i] = consensus_state.AssemblerHeight{Party: types.PartyID(i + 1), DecisionNum: d}
		}
		return hs
	}

	tests := []struct {
		name     string
		n        uint16
		heights  []consensus_state.AssemblerHeight
		previous types.DecisionNum
		expected types.DecisionNum
	}{
		{
			name:     "n=4: no reports keep the watermark",
			n:        4,
			previous: 5,
			expected: 5,
		},
		{
			name:     "n=4: two reports are below 2f+1=3 and keep the watermark",
			n:        4,
			heights:  heights(100, 90),
			previous: 5,
			expected: 5,
		},
		{
			name:     "n=4: three reports set the watermark to the third highest",
			n:        4,
			heights:  heights(100, 90, 80),
			expected: 80,
		},
		{
			name:     "n=4: four reports set the watermark to the third highest",
			n:        4,
			heights:  heights(50, 100, 80, 90),
			expected: 80,
		},
		{
			name:     "n=4: a lower third highest never lowers the watermark",
			n:        4,
			heights:  heights(100, 90, 80),
			previous: 85,
			expected: 85,
		},
		{
			name:     "n=5: 2f+1=3 reports suffice, although the quorum is 4",
			n:        5,
			heights:  heights(30, 20, 10),
			expected: 10,
		},
		{
			name:     "n=7: 2f+1=5 reports set the watermark to the fifth highest",
			n:        7,
			heights:  heights(70, 60, 50, 40, 30, 20, 10),
			expected: 30,
		},
		{
			name:     "n=0: no parties keep the watermark",
			n:        0,
			heights:  heights(9),
			previous: 5,
			expected: 5,
		},
		{
			name:     "n=1: the only report sets the watermark",
			n:        1,
			heights:  heights(9),
			expected: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := consensus_state.State{N: tt.n, AssemblerHeights: tt.heights, DurableWatermark: tt.previous}
			_, s.Threshold, s.Quorum = utils.ComputeFTQ(tt.n)
			s.UpdateDurableWatermark(logger)
			assert.Equal(t, tt.expected, s.DurableWatermark)
		})
	}
}

// TestProcessAdvancesDurableWatermark verifies that Process derives the watermark from the reports it
// records in the same decision, keeps the latest report of a party, and never lowers the watermark.
func TestProcessAdvancesDurableWatermark(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	st := initialState.Clone() // n=4, so 2f+1=3

	st, _, _ = st.Process(logger, 0, reportCE(1, 10, 0), reportCE(2, 20, 0))
	assert.Zero(t, st.DurableWatermark, "two reports are not enough")

	st, _, _ = st.Process(logger, 0, reportCE(3, 30, 0))
	assert.Equal(t, types.DecisionNum(10), st.DurableWatermark)

	st, _, _ = st.Process(logger, 0, reportCE(1, 40, 0), reportCE(1, 15, 0))
	assert.Equal(t, types.DecisionNum(20), st.DurableWatermark, "the latest (highest) report of party 1 counts")

	// A reconfiguration removes party 3; with party 4's low report the third highest height drops to 5,
	// but the watermark does not.
	st.RetainAssemblerHeights([]types.PartyID{1, 2, 4})
	st, _, _ = st.Process(logger, 0, reportCE(4, 5, 0))
	assert.Equal(t, types.DecisionNum(20), st.DurableWatermark)
}

func TestStateCloneAssemblerHeightsAndPrunePoints(t *testing.T) {
	st := initialState.Clone()
	st.AssemblerHeights = []consensus_state.AssemblerHeight{{Party: 1, DecisionNum: 5}}
	st.PrunePoints = []consensus_state.PrunePoint{{Shard: 1, Primary: 1, Seq: 3}}

	clone := st.Clone()
	clone.AssemblerHeights[0].DecisionNum = 6
	clone.PrunePoints[0].Seq = 4

	assert.Equal(t, types.DecisionNum(5), st.AssemblerHeights[0].DecisionNum)
	assert.Equal(t, types.BatchSequence(3), st.PrunePoints[0].Seq)
}

func TestStatePrunePoint(t *testing.T) {
	st := consensus_state.State{
		PrunePoints: []consensus_state.PrunePoint{
			{Shard: 1, Primary: 1, Seq: 10},
			{Shard: 1, Primary: 3, Seq: 30},
			{Shard: 2, Primary: 1, Seq: 50},
		},
	}

	assert.Equal(t, types.BatchSequence(10), st.PrunePoint(1, 1))
	assert.Equal(t, types.BatchSequence(30), st.PrunePoint(1, 3))
	assert.Equal(t, types.BatchSequence(50), st.PrunePoint(2, 1))
	assert.Zero(t, st.PrunePoint(1, 2), "absent primary")
	assert.Zero(t, st.PrunePoint(3, 1), "absent shard")
	assert.Zero(t, (&consensus_state.State{}).PrunePoint(1, 1), "no prune points")
}

func TestStateRetainAssemblerHeights(t *testing.T) {
	st := initialState.Clone()
	st.AssemblerHeights = []consensus_state.AssemblerHeight{
		{Party: 1, DecisionNum: 5},
		{Party: 2, DecisionNum: 6},
		{Party: 3, DecisionNum: 7},
	}
	st.PrunePoints = []consensus_state.PrunePoint{{Shard: 1, Primary: 2, Seq: 3}}

	st.RetainAssemblerHeights([]types.PartyID{3, 1, 4})

	expected := []consensus_state.AssemblerHeight{{Party: 1, DecisionNum: 5}, {Party: 3, DecisionNum: 7}}
	assert.Equal(t, expected, st.AssemblerHeights)
	assert.Equal(t, []consensus_state.PrunePoint{{Shard: 1, Primary: 2, Seq: 3}}, st.PrunePoints, "prune points are kept")
}

func TestStateDeserializeRejectsOutOfRangePruningFields(t *testing.T) {
	tests := []struct {
		name  string
		state *stateprotos.State
	}{
		{
			name:  "assembler height party",
			state: &stateprotos.State{AssemblerHeights: []*stateprotos.AssemblerHeight{{Party: math.MaxUint16 + 1}}},
		},
		{
			name:  "prune point shard",
			state: &stateprotos.State{PrunePoints: []*stateprotos.PrunePoint{{Shard: math.MaxUint16 + 1, Primary: 1}}},
		},
		{
			name:  "prune point primary",
			state: &stateprotos.State{PrunePoints: []*stateprotos.PrunePoint{{Shard: 1, Primary: math.MaxUint16 + 1}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := proto.Marshal(tt.state)
			require.NoError(t, err)
			var s consensus_state.State
			require.ErrorContains(t, s.Deserialize(raw), "exceeds uint16 maximum")
		})
	}
}
