/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Scenario:
//  1. Generate the signing identity of the assembler of one party and of a batcher of the shard in
//     another party, and let the policy of the configuration admit only those two.
//  2. Serve the batcher deliver service of shard 1 over a batch ledger holding one batch, with the
//     access control a batcher builds from that configuration.
//  3. Pull with a request signed by the assembler, and expect the batch to be delivered.
//  4. Pull with a request signed by the batcher, and expect the batch to be delivered.
//  5. Pull with a request signed by an identity the policy does not admit, and expect it to be refused
//     as forbidden, with no batch delivered.
func TestBatcherDeliverServiceServesOnlyAdmittedSigners(t *testing.T) {
	shardID := types.ShardID(1)
	partyID := types.PartyID(1)
	channelID := "arma"

	assemblerSigner, assemblerCert := signutil.NewSelfSignedSigner(t, "org")
	batcherSigner, batcherCert := signutil.NewSelfSignedSigner(t, "org")
	foreignSigner, _ := signutil.NewSelfSignedSigner(t, "org")

	bundle := &mocks.FakeConfigResources{}
	configutil.AdmitSigners(bundle, assemblerCert, batcherCert)

	accessControl, err := deliver.NewAccessControl(bundle, policies.ChannelOrdererReaders)
	require.NoError(t, err)

	endpoint := serveBatcherDeliverService(t, shardID, partyID, channelID, accessControl)
	channelName := node_ledger.ShardPartyChannelIDToChannelName(shardID, partyID, channelID)

	t.Run("the assembler the policy admits", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, assemblerSigner)
		require.NoError(t, err)
		require.NotNil(t, response.GetBlock())
	})

	t.Run("the batcher the policy admits", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, batcherSigner)
		require.NoError(t, err)
		require.NotNil(t, response.GetBlock())
	})

	t.Run("an identity the policy does not admit", func(t *testing.T) {
		response, err := pullBatch(t, endpoint, channelName, foreignSigner)
		require.NoError(t, err)
		require.Equal(t, common.Status_FORBIDDEN, response.GetStatus())
		require.Nil(t, response.GetBlock())
	})
}

// serveBatcherDeliverService serves a batcher deliver service over a ledger holding a single batch,
// and returns the endpoint it listens on.
func serveBatcherDeliverService(t *testing.T, shardID types.ShardID, partyID types.PartyID, channelID string, accessControl *deliver.AccessControl) string {
	logger := testutil.CreateLogger(t, int(partyID))

	ledgerMetrics := &node_ledger.BatchLedgerMetrics{}
	ledgerMetrics.NewBatchLedgerMetrics(&disabled.Provider{}, "1", "1")
	ledgerArray, err := node_ledger.NewBatchLedgerArray(shardID, partyID, []types.PartyID{partyID}, channelID, t.TempDir(), logger, ledgerMetrics)
	require.NoError(t, err)
	t.Cleanup(ledgerArray.Close)

	requests := types.BatchedRequests{[]byte("tx")}
	ledgerArray.Append(partyID, types.BatchSequence(0), types.ConfigSequence(0), requests, requests.Digest(), nil)

	server, err := comm.NewGRPCServer(testutil.AllocateLocalhostAddress(t), comm.ServerConfig{})
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

// pullBatch asks the deliver service at endpoint for the batches of a channel, and returns the first
// response of the stream.
func pullBatch(t *testing.T, endpoint string, channelName string, signer batcher.Signer) (*orderer.DeliverResponse, error) {
	connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(request))

	return stream.Recv()
}

func nextSeekInfoOfTest(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}
