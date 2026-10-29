/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery_test

import (
	"sync/atomic"
	"testing"
	"time"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Serve the consensus deliver service over TLS, over a decision ledger holding one decision, let
//     the policy of the configuration admit only one signing identity, and count the requests it
//     refuses.
//  2. Replicate decisions with a replicator that signs with that identity, and expect the decision.
//  3. Replicate decisions with a replicator that signs with another identity, and expect the policy to
//     refuse its request, with no decision replicated.
func TestConsensusDecisionReplicatorSignsTheRequestsItPullsWith(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	admittedSigner, admittedCert := signutil.NewSelfSignedSigner(t, "org")
	foreignSigner, _ := signutil.NewSelfSignedSigner(t, "org")

	bundle := &mocks.FakeConfigResources{}
	configutil.AdmitSigners(bundle, admittedCert)
	refusals := countRefusals(t, bundle)

	endpoint := serveOneDecisionOverTLS(t, "arma", bundle, ca)

	clientKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	tlsCACerts := []config.RawBytes{ca.CertBytes()}
	logger := testutil.CreateLogger(t, 1)

	t.Run("signed with the admitted identity", func(t *testing.T) {
		replicator := delivery.NewConsensusDecisionReplicator(
			"arma", tlsCACerts, clientKeyPair.Key, clientKeyPair.Cert, endpoint, admittedSigner, logger, delivery.NextSeekInfo(0),
		)
		t.Cleanup(replicator.Stop)

		select {
		case header := <-replicator.ReplicateDecision():
			require.Equal(t, types.DecisionNum(1), header.Num)
		case <-time.After(10 * time.Second):
			t.Fatal("no decision was replicated")
		}
	})

	t.Run("signed with another identity", func(t *testing.T) {
		replicator := delivery.NewConsensusDecisionReplicator(
			"arma", tlsCACerts, clientKeyPair.Key, clientKeyPair.Cert, endpoint, foreignSigner, logger, delivery.NextSeekInfo(0),
		)
		t.Cleanup(replicator.Stop)

		headers := replicator.ReplicateDecision()
		require.Eventually(t, func() bool { return refusals.Load() > 0 }, 10*time.Second, 100*time.Millisecond)
		require.Empty(t, headers)
	})
}

// Scenario:
//  1. Serve the consensus deliver service over TLS, over a decision ledger holding decisions 1 to 5.
//  2. Replicate decisions from decision 1 and consume decisions 1 and 2, leaving the rest unconsumed.
//  3. Restart the deliver service, append decisions 6 to 8, and consume the rest.
//  4. Expect every decision from 3 to 8 exactly once and in order.
func TestConsensusDecisionReplicatorResumesAfterTheLastDecisionOnReconnect(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	signer, cert := signutil.NewSelfSignedSigner(t, "org")
	bundle := &mocks.FakeConfigResources{}
	configutil.AdmitSigners(bundle, cert)

	server := serveDecisionsOverTLS(t, "arma", bundle, ca)
	server.appendDecisions(t, 6)

	clientKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	replicator := delivery.NewConsensusDecisionReplicator(
		"arma", []config.RawBytes{ca.CertBytes()}, clientKeyPair.Key, clientKeyPair.Cert, server.endpoint, signer,
		testutil.CreateLogger(t, 1), delivery.NextSeekInfo(1),
	)
	t.Cleanup(replicator.Stop)
	headers := replicator.ReplicateDecision()

	require.Equal(t, types.DecisionNum(1), nextHeader(t, headers).Num)
	require.Equal(t, types.DecisionNum(2), nextHeader(t, headers).Num)

	server.restart(t)
	server.appendDecisions(t, 3)

	for expected := types.DecisionNum(3); expected <= 8; expected++ {
		require.Equal(t, expected, nextHeader(t, headers).Num)
	}
	require.Never(t, func() bool { return len(headers) > 0 }, time.Second, 100*time.Millisecond)
}

func nextHeader(t *testing.T, headers <-chan *state.Header) *state.Header {
	select {
	case header := <-headers:
		return header
	case <-time.After(10 * time.Second):
		t.Fatal("no decision was replicated")
		return nil
	}
}

// countRefusals counts the requests the policy of the bundle refuses.
func countRefusals(t *testing.T, bundle *mocks.FakeConfigResources) *atomic.Int32 {
	policy, ok := bundle.PolicyManager().GetPolicy(policies.ChannelOrdererReaders)
	require.True(t, ok)
	evaluator, ok := policy.(*policyMocks.FakePolicyEvaluator)
	require.True(t, ok)

	refusals := &atomic.Int32{}
	admit := evaluator.EvaluateSignedDataStub
	evaluator.EvaluateSignedDataStub = func(signedData []*protoutil.SignedData) error {
		err := admit(signedData)
		if err != nil {
			refusals.Add(1)
		}
		return err
	}

	return refusals
}

// serveOneDecisionOverTLS serves the consensus deliver service over TLS, over a decision ledger holding
// one decision, and returns the endpoint it listens on.
func serveOneDecisionOverTLS(t *testing.T, channelID string, bundle *mocks.FakeConfigResources, ca tlsgen.CA) string {
	consensusLedger, err := node_ledger.NewConsensusLedger(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(consensusLedger.Close)

	// A header of all zero values serializes to no bytes, which a replicator cannot read back.
	header := &state.Header{Num: types.DecisionNum(1)}
	consensusLedger.Append(state.CreateBlockToAppendFromDecision(0, smartbft_types.Proposal{Header: header.Serialize()}, nil, nil, 0))

	deliverService, err := delivery.NewDeliverService(channelID, consensusLedger, bundle)
	require.NoError(t, err)

	serverKeyPair, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	server, err := comm.NewGRPCServer(testutil.AllocateLocalhostAddress(t), comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: serverKeyPair.Cert,
			Key:         serverKeyPair.Key,
		},
	})
	require.NoError(t, err)

	orderer.RegisterAtomicBroadcastServer(server.Server(), deliverService)

	go func() {
		_ = server.Start()
	}()
	t.Cleanup(server.Stop)

	return server.Address()
}
