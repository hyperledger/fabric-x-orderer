/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Scenario:
//  1. Generate the signing identity of the assembler of one party, and of the batcher of each of
//     two shards in another party, and place their certificates in a shared configuration that
//     names no other node.
//  2. Serve the batcher deliver service of shard 1 over a batch ledger holding one batch, with the
//     access control a batcher builds from that shared configuration.
//  3. Pull with a request signed by the assembler, and expect the batch to be delivered.
//  4. Pull with a request signed by the batcher of the same shard, and expect the batch to be
//     delivered.
//  5. Pull with a request signed by the batcher of the other shard, and expect the stream to end
//     with no batch delivered.
//  6. Pull with a request signed by a node the shared configuration does not name, and expect the
//     stream to end with no batch delivered.
//  7. Pull with a request that carries the certificate of the assembler but is signed by another
//     key, and expect the stream to end with no batch delivered.
func TestBatcherDeliverServiceServesOnlyConfiguredNodes(t *testing.T) {
	shardID := types.ShardID(1)
	partyID := types.PartyID(1)
	channelID := "arma"

	assemblerSigner, assemblerCert := signutil.NewSelfSignedSigner(t, "org")
	sameShardSigner, sameShardCert := signutil.NewSelfSignedSigner(t, "org")
	otherShardSigner, otherShardCert := signutil.NewSelfSignedSigner(t, "org")
	foreignSigner, _ := signutil.NewSelfSignedSigner(t, "org")

	bundle := configutil.NewBundleOfSharedConfig(t, []*ordererpb.PartyConfig{
		{
			PartyID:         uint32(partyID),
			AssemblerConfig: &ordererpb.AssemblerNodeConfig{SignCert: assemblerCert},
		},
		{
			PartyID: uint32(partyID) + 1,
			BatchersConfig: []*ordererpb.BatcherNodeConfig{
				{ShardID: uint32(shardID), SignCert: sameShardCert},
				{ShardID: uint32(shardID) + 1, SignCert: otherShardCert},
			},
		},
	})

	accessControl, err := deliver.NewBatcherDeliverVerifier(bundle, shardID)
	require.NoError(t, err)

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	endpoint := serveBatcherDeliverService(t, shardID, partyID, channelID, accessControl, ca)
	channelName := node_ledger.ShardPartyChannelIDToChannelName(shardID, partyID, channelID)

	t.Run("the assembler the shared configuration names", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, assemblerSigner, newTLSClient(t, ca))
		require.NoError(t, err)
		require.NotNil(t, response.GetBlock())
	})

	t.Run("the batcher of the same shard in another party", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, sameShardSigner, newTLSClient(t, ca))
		require.NoError(t, err)
		require.NotNil(t, response.GetBlock())
	})

	t.Run("a batcher of another shard", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, otherShardSigner, newTLSClient(t, ca))
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, response.GetBlock())
	})

	t.Run("a node the shared configuration does not name", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, foreignSigner, newTLSClient(t, ca))
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, response.GetBlock())
	})

	t.Run("the certificate of the assembler, signed by another key", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, &mismatchedSigner{cert: assemblerCert, key: foreignSigner}, newTLSClient(t, ca))
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, response.GetBlock())
	})
}

// Scenario:
//  1. Generate the signing identity of the assembler of one party, and place its certificate in a
//     shared configuration.
//  2. Serve the batcher deliver service of shard 1 over a batch ledger holding one batch, with the
//     access control a batcher builds from that shared configuration.
//  3. Pull with a request the assembler signed that carries no TLS certificate hash, and expect the
//     stream to end with no batch delivered.
//  4. Pull with a request the assembler signed that carries the TLS certificate hash of another
//     client, and expect the stream to end with no batch delivered.
func TestBatcherDeliverServiceBindsRequestToClientCertificate(t *testing.T) {
	shardID := types.ShardID(1)
	partyID := types.PartyID(1)
	channelID := "arma"

	assemblerSigner, assemblerCert := signutil.NewSelfSignedSigner(t, "org")

	bundle := configutil.NewBundleOfSharedConfig(t, []*ordererpb.PartyConfig{
		{
			PartyID:         uint32(partyID),
			AssemblerConfig: &ordererpb.AssemblerNodeConfig{SignCert: assemblerCert},
		},
	})

	accessControl, err := deliver.NewBatcherDeliverVerifier(bundle, shardID)
	require.NoError(t, err)

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	endpoint := serveBatcherDeliverService(t, shardID, partyID, channelID, accessControl, ca)
	channelName := node_ledger.ShardPartyChannelIDToChannelName(shardID, partyID, channelID)

	t.Run("no TLS certificate hash", func(t *testing.T) {
		client := newTLSClient(t, ca)
		client.tlsCertHash = nil

		response, err := pullBatch(t, endpoint, channelName, assemblerSigner, client)
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, response.GetBlock())
	})

	t.Run("the TLS certificate hash of another client", func(t *testing.T) {
		client := newTLSClient(t, ca)
		client.tlsCertHash = newTLSClient(t, ca).tlsCertHash

		response, err := pullBatch(t, endpoint, channelName, assemblerSigner, client)
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, response.GetBlock())
	})
}

// serveBatcherDeliverService serves a batcher deliver service over a ledger holding a single batch,
// and returns the endpoint it listens on.
func serveBatcherDeliverService(t *testing.T, shardID types.ShardID, partyID types.PartyID, channelID string, accessControl *deliver.NodeVerifier, ca tlsgen.CA) string {
	logger := testutil.CreateLogger(t, int(partyID))

	ledgerMetrics := &node_ledger.BatchLedgerMetrics{}
	ledgerMetrics.NewBatchLedgerMetrics(&disabled.Provider{}, "1", "1")
	ledgerArray, err := node_ledger.NewBatchLedgerArray(shardID, partyID, []types.PartyID{partyID}, channelID, t.TempDir(), logger, ledgerMetrics)
	require.NoError(t, err)
	t.Cleanup(ledgerArray.Close)

	requests := types.BatchedRequests{[]byte("tx")}
	ledgerArray.Append(partyID, types.BatchSequence(0), types.ConfigSequence(0), requests, requests.Digest(), nil)

	serverKeyPair, err := ca.NewServerCertKeyPair(localhost)
	require.NoError(t, err)

	server, err := newGRPCServer(testutil.AllocateLocalhostAddress(t), ca, serverKeyPair)
	require.NoError(t, err)

	orderer.RegisterAtomicBroadcastServer(server.Server(), &batcher.BatcherDeliverService{
		LedgerArray:   ledgerArray,
		AccessControl: accessControl,
		Logger:        logger,
	})

	go func() {
		if err := server.Start(); err != nil {
			logger.Errorf("Deliver service stopped serving: %s", err)
		}
	}()
	t.Cleanup(server.Stop)

	return server.Address()
}

// pullBatch pulls batches over the mutual TLS connection of client, and returns the first response.
func pullBatch(t *testing.T, endpoint string, channelName string, signer batcher.Signer, client *tlsClient) (*orderer.DeliverResponse, error) {
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
		nextSeekInfoOfTest(0),
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

func nextSeekInfoOfTest(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
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
