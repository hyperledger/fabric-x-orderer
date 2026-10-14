/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery_test

import (
	"context"
	"testing"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tlsutil"
	"github.com/stretchr/testify/require"
)

const localhost = "127.0.0.1"

// Scenario:
//  1. Generate the signing identity of the router, the consenter, the assembler and a batcher of one
//     party, and let the policy of the configuration admit only those four.
//  2. Serve the consensus deliver service over a decision ledger holding one decision, with the
//     access control a consenter builds from that configuration.
//  3. Pull with a request signed by each of those four nodes, and expect the decision to be
//     delivered to every one of them.
//  4. Pull with a request signed by an identity the policy does not admit, and expect it to be
//     refused as forbidden, with no decision delivered.
func TestConsensusDeliverServiceServesOnlyAdmittedSigners(t *testing.T) {
	channelID := "arma"

	routerSigner, routerCert := signutil.NewSelfSignedSigner(t, "org")
	batcherSigner, batcherCert := signutil.NewSelfSignedSigner(t, "org")
	consenterSigner, consenterCert := signutil.NewSelfSignedSigner(t, "org")
	assemblerSigner, assemblerCert := signutil.NewSelfSignedSigner(t, "org")
	foreignSigner, _ := signutil.NewSelfSignedSigner(t, "org")

	bundle := &mocks.FakeConfigResources{}
	configutil.AdmitSigners(bundle, routerCert, batcherCert, consenterCert, assemblerCert)

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	endpoint := serveConsensusDeliverService(t, channelID, bundle, ca)
	channelName := delivery.DecisionChannelName(channelID)

	for _, node := range []struct {
		name   string
		signer identity.SignerSerializer
	}{
		{name: "the router", signer: routerSigner},
		{name: "the batcher", signer: batcherSigner},
		{name: "the consenter", signer: consenterSigner},
		{name: "the assembler", signer: assemblerSigner},
	} {
		t.Run(node.name, func(t *testing.T) {
			response, err := pullDecision(t, endpoint, channelName, node.signer, tlsutil.NewClient(t, ca))
			require.NoError(t, err)
			require.NotNil(t, response.GetBlock())
		})
	}

	t.Run("an identity the policy does not admit", func(t *testing.T) {
		response, err := pullDecision(t, endpoint, channelName, foreignSigner, tlsutil.NewClient(t, ca))
		require.NoError(t, err)
		require.Equal(t, common.Status_FORBIDDEN, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})
}

// Scenario:
//  1. Generate the signing identity of the consenter of one party, and let the policy of the
//     configuration admit it.
//  2. Serve the consensus deliver service over a decision ledger holding one decision, with the
//     access control a consenter builds from that configuration.
//  3. Pull with a request the consenter signed that carries no TLS certificate hash, and expect the
//     request to be refused with no decision delivered.
//  4. Pull with a request the consenter signed that carries the TLS certificate hash of another
//     client, and expect the request to be refused with no decision delivered.
func TestConsensusDeliverServiceBindsRequestToClientCertificate(t *testing.T) {
	channelID := "arma"

	consenterSigner, consenterCert := signutil.NewSelfSignedSigner(t, "org")

	bundle := &mocks.FakeConfigResources{}
	configutil.AdmitSigners(bundle, consenterCert)

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	endpoint := serveConsensusDeliverService(t, channelID, bundle, ca)
	channelName := delivery.DecisionChannelName(channelID)

	t.Run("no TLS certificate hash", func(t *testing.T) {
		client := tlsutil.NewClient(t, ca)
		client.TLSCertHash = nil

		response, err := pullDecision(t, endpoint, channelName, consenterSigner, client)
		require.NoError(t, err)
		require.Equal(t, common.Status_BAD_REQUEST, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})

	t.Run("the TLS certificate hash of another client", func(t *testing.T) {
		client := tlsutil.NewClient(t, ca)
		client.TLSCertHash = tlsutil.NewClient(t, ca).TLSCertHash

		response, err := pullDecision(t, endpoint, channelName, consenterSigner, client)
		require.NoError(t, err)
		require.Equal(t, common.Status_BAD_REQUEST, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})
}

// serveConsensusDeliverService serves a consensus deliver service over a ledger holding a single
// decision, and returns the endpoint it listens on.
func serveConsensusDeliverService(t *testing.T, channelID string, bundle channelconfig.Resources, ca tlsgen.CA) string {
	logger := testutil.CreateLogger(t, 1)

	consensusLedger, err := node_ledger.NewConsensusLedger(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(consensusLedger.Close)

	header := &state.Header{Num: types.DecisionNum(0)}
	consensusLedger.Append(state.CreateBlockToAppendFromDecision(0, smartbft_types.Proposal{Header: header.Serialize()}, nil, nil, 0))

	deliverService, err := delivery.NewDeliverService(channelID, consensusLedger, bundle)
	require.NoError(t, err)

	serverKeyPair, err := ca.NewServerCertKeyPair(localhost)
	require.NoError(t, err)

	server, err := tlsutil.NewMutualTLSServer(testutil.AllocateLocalhostAddress(t), ca, serverKeyPair)
	require.NoError(t, err)

	orderer.RegisterAtomicBroadcastServer(server.Server(), deliverService)

	go func() {
		if err := server.Start(); err != nil {
			logger.Errorf("Deliver service stopped serving: %s", err)
		}
	}()
	t.Cleanup(server.Stop)

	return server.Address()
}

// pullDecision pulls decisions over the mutual TLS connection of client, and returns the first response.
func pullDecision(t *testing.T, endpoint string, channelName string, signer identity.SignerSerializer, client *tlsutil.Client) (*orderer.DeliverResponse, error) {
	connection, err := client.Dial(endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { connection.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := orderer.NewAtomicBroadcastClient(connection).Deliver(ctx)
	require.NoError(t, err)

	request, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		channelName,
		signer,
		delivery.SingleSpecifiedSeekInfo(0),
		int32(0),
		uint64(0),
		client.TLSCertHash,
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(request))

	return stream.Recv()
}
