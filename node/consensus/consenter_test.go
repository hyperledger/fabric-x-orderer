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
	"github.com/stretchr/testify/assert"
)

func TestConsenter(t *testing.T) {
	s := &state.State{
		ShardCount: 1,
		N:          4,
		Shards:     []state.ShardTerm{{Shard: 1, Term: 1}},
		Threshold:  2,
		Pending:    []arma_types.BatchAttestationFragment{},
		Quorum:     4,
	}

	logger := testutil.CreateLogger(t, 0)
	consenter := createConsenter(logger)

	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	ba := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(2), 0, 0)
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
	ba2 := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(3), 0, 0)
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
	cr := &state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload:   []byte("config-payload"),
			Signature: []byte("config-signature"),
		},
	}
	events = [][]byte{(&state.ControlEvent{ConfigRequest: cr}).Bytes()}

	_, _, configRequests := consenter.SimulateStateTransition(s, 0, events)
	assert.Equal(t, cr.Envelope.Payload, configRequests[0].Envelope.Payload)
	assert.Equal(t, cr.Envelope.Signature, configRequests[0].Envelope.Signature)
}

func createConsenter(logger *flogging.FabricLogger) *consensus.Consenter {
	consenter := &consensus.Consenter{
		Logger:          logger,
		DB:              &mocks.FakeBatchAttestationDB{},
		BAFDeserializer: &state.BAFDeserialize{},
	}

	return consenter
}
