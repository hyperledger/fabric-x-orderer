/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	node_consensus "github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestAssembleProposalAndVerifyPrunePoints verifies that a leader and a validator that indexed the same
// committed batches compute the same prune points and replay start, so the validator accepts the leader's
// proposal, while a validator whose index lacks a decision computes another state and rejects it.
func TestAssembleProposalAndVerifyPrunePoints(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	bundle.ConfigtxValidatorReturns(configtxValidator)
	config := &nodeconfig.ConsenterNodeConfig{Bundle: bundle, RequestMaxBytes: 1000}

	// SmartBFT proposes only when it has requests, so the proposal carries one signed complaint.
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer := crypto.ECDSASigner(*sk)
	verifier := crypto.ECDSAVerifier{arma_types.NodeIdentity{Role: arma_types.RoleBatcher, PartyID: 1, ShardID: 1}: signer.PublicKey}
	complaint := &state.Complaint{ShardTerm: state.ShardTerm{Shard: 1, Term: 1}, Signer: 1}
	complaint.Signature, err = signer.Sign(complaint.ToBeSigned())
	require.NoError(t, err)
	requests := [][]byte{(&state.ControlEvent{Complaint: complaint}).Bytes()}

	newNode := func(decisions []arma_types.DecisionNum) *node_consensus.Consensus {
		consenter := &node_consensus.Consenter{DB: &mocks.FakeBatchAttestationDB{}, Logger: logger}
		commitSeqs(t, consenter, []arma_types.BatchSequence{0, 1, 2}[:len(decisions)], decisions)
		initialState := pruneState(3, 0)
		initialState.AppContext = protoutil.MarshalOrPanic(&common.BlockHeader{Number: 3})
		return &node_consensus.Consensus{
			Arma:            consenter,
			State:           initialState,
			Logger:          logger,
			Config:          config,
			SigVerifier:     verifier,
			RequestVerifier: node_consensus.CreateConsensusRulesVerifier(config),
		}
	}

	leader := newNode([]arma_types.DecisionNum{1, 2, 3})
	md, err := proto.Marshal(&smartbftprotos.ViewMetadata{LatestSequence: 4})
	require.NoError(t, err)
	proposal := leader.AssembleProposal(md, requests)

	hdr := &state.Header{}
	require.NoError(t, hdr.Deserialize(proposal.Header))
	require.Equal(t, arma_types.BatchSequence(3), hdr.State.PrunePoint(1, 1))
	require.Equal(t, arma_types.DecisionNum(1), hdr.State.ReplayFrom)

	_, err = newNode([]arma_types.DecisionNum{1, 2, 3}).VerifyProposal(proposal)
	require.NoError(t, err)

	_, err = newNode([]arma_types.DecisionNum{1, 2}).VerifyProposal(proposal)
	require.ErrorContains(t, err, "proposed state")
}
