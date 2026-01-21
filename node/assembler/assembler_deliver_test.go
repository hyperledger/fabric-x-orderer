/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
)

// scenario:
// 1) start an assembler, stub-batcher, stub-consenter
// 2) send batches and decisions, to store 3 additional blocks in the ledger
// 3) call SoftStop on assembler
// 4) connect to assembler's delivery service and pull blocks from the ledger
func TestAssembler_SoftStopAndPull(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)

	batchersStubShard0, batcherInfosShard0, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(0), ca)
	defer cleanup()

	_, batcherInfosShard1, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(1), ca)
	defer cleanup()

	shards := []config.ShardInfo{
		{ShardId: types.ShardID(0), Batchers: batcherInfosShard0},
		{ShardId: types.ShardID(1), Batchers: batcherInfosShard1},
	}

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Stop()

	assembler, assemblerEndpoint := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	batchCount := 3
	for i := 0; i < batchCount; i++ {
		// send batch and decision from party 1 on shard 0
		batch := types.NewSimpleBatch(0, 2, types.BatchSequence(i), types.BatchedRequests{[]byte{1}}, 0)
		batchersStubShard0[0].SetNextBatch(batch)
		oba := obaCreator.Append(batch, types.DecisionNum(i+1), 0, 1)
		consenterStub.SetNextDecision(oba)
		<-consenterStub.decisionSentCh

		require.Eventually(t, func() bool {
			return assembler.GetTxCount() == uint64(i)+2
		}, 10*time.Second, 100*time.Millisecond)
	}

	assembler.SoftStop()
	// wait for SoftStop to finish and pull from assembler

	err = createDeliveryClientAndPull(t, assemblerEndpoint, [][]byte{ca.CertBytes()}, ca, batchCount)
	require.NoError(t, err)
}

// createDeliveryClientAndPull connects to the delivery service in the assembler and tries to pull a number of blocks.
func createDeliveryClientAndPull(t *testing.T, assemblerEndpoint string, assemblerRootCAs [][]byte, ca tlsgen.CA, numBlocksToPull int) error {
	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               ckp.Key,
			Certificate:       ckp.Cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     assemblerRootCAs,
		},
		DialTimeout: time.Second * 5,
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		nil,
		delivery.NextSeekInfo(1),
		int32(0),
		uint64(0),
		nil,
	)
	require.NoError(t, err)

	gRPCAssemblerClientConn, err := cc.Dial(assemblerEndpoint)
	require.NoError(t, err)
	defer gRPCAssemblerClientConn.Close()

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver stream
	stream, err := abc.Deliver(context.TODO())
	require.NoError(t, err)
	defer stream.CloseSend()

	// send request envelope
	err = stream.Send(requestEnvelope)
	require.NoError(t, err)

	// pull blocks
	for i := 0; i < numBlocksToPull; i++ {
		resp, err := stream.Recv()
		require.NoError(t, err)
		block := resp.GetBlock()
		require.NotNil(t, block)
		require.NotNil(t, block.Data)
		require.Greater(t, len(block.Data.Data), 0)
	}
	return nil
}
