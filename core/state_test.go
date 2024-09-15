package core_test

import (
	"testing"

	"arma/common/types"
	"arma/core"
	"arma/node/consensus/state"
	"arma/testutil"

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
	}

	var c2 core.Complaint

	err := c2.FromBytes(c.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, c, c2)
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
	logger := testutil.CreateLogger(t, 0)

	// Update state with a valid Complaint
	core.CollectAndDeduplicateEvents(&state, logger, ce)

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
		Quorum:     2,
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
