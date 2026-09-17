/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery_test

import (
	"context"
	"testing"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const localhost = "127.0.0.1"

// Scenario:
//  1. Generate the signing identity of the router, the consenter, the assembler and the batchers of
//     two shards of one party, and place their certificates in a shared configuration that names no
//     other node.
//  2. Serve the consensus deliver service over a decision ledger holding one decision, with the
//     access control a consenter builds from that shared configuration.
//  3. Pull with a request signed by each of those five nodes, and expect the decision to be
//     delivered to every one of them.
//  4. Pull with a request signed by a node the shared configuration does not name, and expect the
//     request to be refused with no decision delivered.
//  5. Pull with a request that carries the certificate of the consenter but is signed by another
//     key, and expect the request to be refused with no decision delivered.
func TestConsensusDeliverServiceServesOnlyConfiguredNodes(t *testing.T) {
	channelID := "arma"

	routerSigner, routerCert := signutil.NewSelfSignedSigner(t, "org")
	firstShardSigner, firstShardCert := signutil.NewSelfSignedSigner(t, "org")
	secondShardSigner, secondShardCert := signutil.NewSelfSignedSigner(t, "org")
	consenterSigner, consenterCert := signutil.NewSelfSignedSigner(t, "org")
	assemblerSigner, assemblerCert := signutil.NewSelfSignedSigner(t, "org")
	foreignSigner, _ := signutil.NewSelfSignedSigner(t, "org")

	bundle := configutil.NewBundleOfSharedConfig(t, []*ordererpb.PartyConfig{
		{
			PartyID:      1,
			RouterConfig: &ordererpb.RouterNodeConfig{SignCert: routerCert},
			BatchersConfig: []*ordererpb.BatcherNodeConfig{
				{ShardID: 1, SignCert: firstShardCert},
				{ShardID: 2, SignCert: secondShardCert},
			},
			ConsenterConfig: &ordererpb.ConsenterNodeConfig{SignCert: consenterCert},
			AssemblerConfig: &ordererpb.AssemblerNodeConfig{SignCert: assemblerCert},
		},
	})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	endpoint := serveConsensusDeliverService(t, channelID, bundle, ca)
	channelName := delivery.DecisionChannelName(channelID)

	for _, node := range []struct {
		name   string
		signer identity.SignerSerializer
	}{
		{name: "the router", signer: routerSigner},
		{name: "the batcher of the first shard", signer: firstShardSigner},
		{name: "the batcher of the second shard", signer: secondShardSigner},
		{name: "the consenter", signer: consenterSigner},
		{name: "the assembler", signer: assemblerSigner},
	} {
		t.Run(node.name, func(t *testing.T) {
			response, err := pullDecision(t, endpoint, channelName, node.signer, newTLSClient(t, ca))
			require.NoError(t, err)
			require.NotNil(t, response.GetBlock())
		})
	}

	t.Run("a node the shared configuration does not name", func(t *testing.T) {
		response, err := pullDecision(t, endpoint, channelName, foreignSigner, newTLSClient(t, ca))
		require.NoError(t, err)
		require.Equal(t, common.Status_FORBIDDEN, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})

	t.Run("the certificate of the consenter, signed by another key", func(t *testing.T) {
		response, err := pullDecision(t, endpoint, channelName, &mismatchedSigner{cert: consenterCert, key: foreignSigner}, newTLSClient(t, ca))
		require.NoError(t, err)
		require.Equal(t, common.Status_FORBIDDEN, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})
}

// Scenario:
//  1. Generate the signing identity of the consenter of one party, and place its certificate in a
//     shared configuration.
//  2. Serve the consensus deliver service over a decision ledger holding one decision, with the
//     access control a consenter builds from that shared configuration.
//  3. Pull with a request the consenter signed that carries no TLS certificate hash, and expect the
//     request to be refused with no decision delivered.
//  4. Pull with a request the consenter signed that carries the TLS certificate hash of another
//     client, and expect the request to be refused with no decision delivered.
func TestConsensusDeliverServiceBindsRequestToClientCertificate(t *testing.T) {
	channelID := "arma"

	consenterSigner, consenterCert := signutil.NewSelfSignedSigner(t, "org")

	bundle := configutil.NewBundleOfSharedConfig(t, []*ordererpb.PartyConfig{
		{
			PartyID:         1,
			ConsenterConfig: &ordererpb.ConsenterNodeConfig{SignCert: consenterCert},
		},
	})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	endpoint := serveConsensusDeliverService(t, channelID, bundle, ca)
	channelName := delivery.DecisionChannelName(channelID)

	t.Run("no TLS certificate hash", func(t *testing.T) {
		client := newTLSClient(t, ca)
		client.tlsCertHash = nil

		response, err := pullDecision(t, endpoint, channelName, consenterSigner, client)
		require.NoError(t, err)
		require.Equal(t, common.Status_BAD_REQUEST, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})

	t.Run("the TLS certificate hash of another client", func(t *testing.T) {
		client := newTLSClient(t, ca)
		client.tlsCertHash = newTLSClient(t, ca).tlsCertHash

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

	server, err := newGRPCServer(testutil.AllocateLocalhostAddress(t), ca, serverKeyPair)
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
func pullDecision(t *testing.T, endpoint string, channelName string, signer identity.SignerSerializer, client *tlsClient) (*orderer.DeliverResponse, error) {
	connection, err := client.dial(endpoint)
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
		seekOneDecision(0),
		int32(0),
		uint64(0),
		client.tlsCertHash,
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(request))

	return stream.Recv()
}

// tlsClient holds the material a test client needs for a mutual TLS connection and its request binding.
type tlsClient struct {
	ca          tlsgen.CA
	keyPair     *tlsgen.CertKeyPair
	tlsCertHash []byte
}

func newTLSClient(t *testing.T, ca tlsgen.CA) *tlsClient {
	keyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	tlsCertHash, err := protoutil.HashTLSCertificate(keyPair.Cert)
	require.NoError(t, err)

	return &tlsClient{ca: ca, keyPair: keyPair, tlsCertHash: tlsCertHash}
}

func (c *tlsClient) dial(endpoint string) (*grpc.ClientConn, error) {
	clientConfig := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			Key:               c.keyPair.Key,
			Certificate:       c.keyPair.Cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     [][]byte{c.ca.CertBytes()},
		},
		DialTimeout: 10 * time.Second,
	}

	return clientConfig.Dial(endpoint)
}

func newGRPCServer(addr string, ca tlsgen.CA, kp *tlsgen.CertKeyPair) (*comm.GRPCServer, error) {
	return comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     [][]byte{ca.CertBytes()},
			Key:               kp.Key,
			Certificate:       kp.Cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     [][]byte{ca.CertBytes()},
		},
	})
}

func seekOneDecision(decisionNum uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: decisionNum}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: decisionNum}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

// mismatchedSigner presents the certificate of one node and signs with the key of another.
type mismatchedSigner struct {
	cert []byte
	key  *signutil.TestSigner
}

func (s *mismatchedSigner) Sign(message []byte) ([]byte, error) {
	return s.key.Sign(message)
}

func (s *mismatchedSigner) Serialize() ([]byte, error) {
	return protoutil.MarshalOrPanic(&msp.SerializedIdentity{Mspid: "org", IdBytes: s.cert}), nil
}
