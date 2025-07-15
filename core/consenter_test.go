/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core_test

import (
	"testing"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/core/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
)

func TestConsenter(t *testing.T) {
	s := &core.State{
		ShardCount: 1,
		N:          4,
		Shards:     []core.ShardTerm{{Shard: 1, Term: 1}},
		Threshold:  2,
		Pending:    []arma_types.BatchAttestationFragment{},
		Quorum:     4,
	}

	logger := testutil.CreateLogger(t, 0)
	consenter := createConsenter(s, logger)

	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	ba := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(2), []uint8{1}, 0, [][]uint8{})
	events := [][]byte{(&core.ControlEvent{BAF: ba}).Bytes()}

	// Test with an event that should be filtered out
	db.ExistsReturns(true)
	newState, batchAttestations := consenter.SimulateStateTransition(s, events)
	assert.Empty(t, batchAttestations)
	assert.Empty(t, newState.Pending)

	consenter.Commit(events)
	assert.Equal(t, consenter.State, newState)
	assert.Zero(t, db.PutCallCount())

	// Test a valid event below threshold
	db.ExistsReturns(false)
	newState, batchAttestations = consenter.SimulateStateTransition(s, events)
	assert.Empty(t, batchAttestations)
	assert.Len(t, newState.Pending, 1)

	consenter.Commit(events)
	assert.Equal(t, consenter.State, newState)
	assert.Zero(t, db.PutCallCount())

	// Test valid events meeting the threshold
	ba2 := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(3), []uint8{1}, 0, [][]uint8{})
	events = append(events, (&core.ControlEvent{BAF: ba2}).Bytes())

	newState, batchAttestations = consenter.SimulateStateTransition(s, events)
	assert.Len(t, batchAttestations[0], 2)
	assert.Empty(t, newState.Pending)

	consenter.Commit(events)
	assert.Equal(t, consenter.State, newState)
	assert.Equal(t, db.PutCallCount(), 1)

	// Test the complaint is not stored in the DB
	c := core.Complaint{
		ShardTerm: core.ShardTerm{Shard: 1, Term: 1},
		Signer:    3,
		Signature: []byte{2},
	}

	events = [][]byte{(&core.ControlEvent{Complaint: &c}).Bytes()}

	consenter.Commit(events)
	assert.Equal(t, db.PutCallCount(), 1)
	assert.Len(t, consenter.State.Complaints, 1)
}

func createConsenter(state *core.State, logger arma_types.Logger) *core.Consenter {
	consenter := &core.Consenter{
		Logger:          logger,
		DB:              &mocks.FakeBatchAttestationDB{},
		BAFDeserializer: &BAFSimpleDeserializer{},
		State:           state,
	}

	return consenter
}
