/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	consenter := createConsenter(s, logger)

	db := &mocks.FakeBatchAttestationDB{}
	consenter.DB = db

	ba := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(2), 0)
	ba.SetSignature([]uint8{1})
	events := [][]byte{(&state.ControlEvent{BAF: ba}).Bytes()}

	// Test with an event that should be filtered out
	db.ExistsReturns(true)
	newState, batchAttestations, _ := consenter.SimulateStateTransition(s, events)
	assert.Empty(t, batchAttestations)
	assert.Empty(t, newState.Pending)

	consenter.Commit(events)
	assert.Equal(t, consenter.State, newState)
	assert.Zero(t, db.PutCallCount())

	// Test a valid event below threshold
	db.ExistsReturns(false)
	newState, batchAttestations, _ = consenter.SimulateStateTransition(s, events)
	assert.Empty(t, batchAttestations)
	assert.Len(t, newState.Pending, 1)

	consenter.Commit(events)
	assert.Equal(t, consenter.State, newState)
	assert.Zero(t, db.PutCallCount())

	// Test valid events meeting the threshold
	ba2 := arma_types.NewSimpleBatchAttestationFragment(arma_types.ShardID(1), arma_types.PartyID(1), arma_types.BatchSequence(1), []byte{3}, arma_types.PartyID(3), 0)
	ba2.SetSignature([]byte{1})
	events = append(events, (&state.ControlEvent{BAF: ba2}).Bytes())

	newState, batchAttestations, _ = consenter.SimulateStateTransition(s, events)
	assert.Len(t, batchAttestations[0], 2)
	assert.Empty(t, newState.Pending)

	consenter.Commit(events)
	assert.Equal(t, consenter.State, newState)
	assert.Equal(t, db.PutCallCount(), 1)

	// Test the complaint is not stored in the DB
	c := state.Complaint{
		ShardTerm: state.ShardTerm{Shard: 1, Term: 1},
		Signer:    3,
		Signature: []byte{2},
	}

	events = [][]byte{(&state.ControlEvent{Complaint: &c}).Bytes()}

	consenter.Commit(events)
	assert.Equal(t, db.PutCallCount(), 1)
	assert.Len(t, consenter.State.Complaints, 1)

	// Test ConfigRequest is returned by SimulateStateTransition
	cr := &state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload:   []byte("config-payload"),
			Signature: []byte("config-signature"),
		},
	}
	events = [][]byte{(&state.ControlEvent{ConfigRequest: cr}).Bytes()}

	_, _, configRequests := consenter.SimulateStateTransition(s, events)
	assert.Equal(t, cr.Envelope.Payload, configRequests[0].Envelope.Payload)
	assert.Equal(t, cr.Envelope.Signature, configRequests[0].Envelope.Signature)
}

func createConsenter(s *state.State, logger arma_types.Logger) *consensus.Consenter {
	consenter := &consensus.Consenter{
		Logger:          logger,
		DB:              &mocks.FakeBatchAttestationDB{},
		BAFDeserializer: &state.BAFDeserialize{},
		State:           s,
	}

	return consenter
}

func TestSubmitConfigConsensusNode(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	// prepare setup
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, 1, genesisBlock)

	// update consensus config update proposer to return a dummy config request with header = common.HeaderType_CONFIG
	payloadBytes := []byte{1}
	configRequestEnvelope := tx.CreateStructuredConfigEnvelope(payloadBytes)
	configRequest := &protos.Request{
		Payload:   configRequestEnvelope.Payload,
		Signature: configRequestEnvelope.Signature,
	}
	mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	mockConfigUpdateProposer.ProposeConfigUpdateReturns(configRequest, nil)
	setup.consensusNodes[0].ConfigUpdateProposer = mockConfigUpdateProposer

	// update consensus router config
	routerCert, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, routerCert)

	consensusNode := setup.consensusNodes[0]
	consensusNode.Config.Router = config.RouterInfo{
		PartyID:    1,
		Endpoint:   "127.0.0.1:5030",
		TLSCACerts: nil,
		TLSCert:    routerCert.Cert,
	}

	// update setup configs such that consenterNodeConfig.router will be the above routerInfo (for consistency)
	setup.configs[0].Router = consensusNode.Config.Router

	// Submit request (decision 1)
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)

	// Submit config request (decision 2)
	_, err = createAndSubmitConfigRequest(setup.consensusNodes[0], routerCert.TLSCert, payloadBytes)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)

	// check decision appended to the consensus ledger is a config decision, i.e. it has a config block as last block in AvailableCommonBlocks
	proposal, _, err := state.BytesToDecision(b.Data.Data[0])
	require.NotNil(t, proposal)
	require.NoError(t, err)

	header := &state.Header{}
	err = header.Deserialize(proposal.Header)
	require.NoError(t, err)

	block := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
	env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[0])
	require.NotNil(t, env)
	require.NoError(t, err)
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	require.NoError(t, err)
	require.NotNil(t, payload)
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	require.NoError(t, err)
	require.NotNil(t, chdr)
	require.Equal(t, chdr.Type, int32(common.HeaderType_CONFIG))

	setup.consensusNodes[0].Stop()
}
