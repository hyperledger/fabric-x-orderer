/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"context"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

	assembler, assemblerEndpoint := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second, false, nil)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	batchCount := 3
	for i := 0; i < batchCount; i++ {
		// send batch and decision from party 1 on shard 0
		batch := types.NewSimpleBatch(0, 2, types.BatchSequence(i), types.BatchedRequests{[]byte{1}}, 0, nil)
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

	err = createDeliveryClientAndPull(t, assemblerEndpoint, [][]byte{ca.CertBytes()}, ca, batchCount, "TLS", nil)
	require.NoError(t, err)
}

func TestAssemblerDeliver_WithMTLS_success(t *testing.T) {
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

	clientRootCAs := [][]byte{ca.CertBytes()}

	assembler, assemblerEndpoint := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second, true, clientRootCAs)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	batchCount := 3
	for i := 0; i < batchCount; i++ {
		// send batch and decision from party 1 on shard 0
		batch := types.NewSimpleBatch(0, 2, types.BatchSequence(i), types.BatchedRequests{[]byte{1}}, 0, nil)
		batchersStubShard0[0].SetNextBatch(batch)
		oba := obaCreator.Append(batch, types.DecisionNum(i+1), 0, 1)
		consenterStub.SetNextDecision(oba)
		<-consenterStub.decisionSentCh

		require.Eventually(t, func() bool {
			return assembler.GetTxCount() == uint64(i)+2
		}, 10*time.Second, 100*time.Millisecond)
	}

	err = createDeliveryClientAndPull(t, assemblerEndpoint, [][]byte{ca.CertBytes()}, ca, batchCount, "mTLS", nil)
	require.NoError(t, err)
}

func TestAssemblerDeliver_WithMTLS_NoCertHash(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)

	_, batcherInfosShard0, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(0), ca)
	defer cleanup()

	_, batcherInfosShard1, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(1), ca)
	defer cleanup()

	shards := []config.ShardInfo{
		{ShardId: types.ShardID(0), Batchers: batcherInfosShard0},
		{ShardId: types.ShardID(1), Batchers: batcherInfosShard1},
	}

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Stop()

	clientRootCAs := [][]byte{ca.CertBytes()}

	assembler, assemblerEndpoint := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second, true, clientRootCAs)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	ckp, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	cc := createAssemblerClientConn(t, assemblerEndpoint, [][]byte{ca.CertBytes()}, ckp, "mTLS")
	defer cc.Close()
	abc := ab.NewAtomicBroadcastClient(cc)

	// create a deliver stream
	stream, err := abc.Deliver(context.TODO())
	require.NoError(t, err)
	defer stream.CloseSend()

	require.NoError(t, err)

	// prepare request envelope with no cert hash
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

	// send request envelope
	err = stream.Send(requestEnvelope)
	require.NoError(t, err)

	// expect to recv response with error due to missing cert hash
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, resp.GetStatus(), common.Status_BAD_REQUEST)
}

func TestAssemblerDeliver_WrongChannelID(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)

	_, batcherInfosShard0, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(0), ca)
	defer cleanup()

	_, batcherInfosShard1, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(1), ca)
	defer cleanup()

	shards := []config.ShardInfo{
		{ShardId: types.ShardID(0), Batchers: batcherInfosShard0},
		{ShardId: types.ShardID(1), Batchers: batcherInfosShard1},
	}

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Stop()

	clientRootCAs := [][]byte{ca.CertBytes()}

	assembler, assemblerEndpoint := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second, true, clientRootCAs)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	ckp, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	cc := createAssemblerClientConn(t, assemblerEndpoint, [][]byte{ca.CertBytes()}, ckp, "mTLS")
	defer cc.Close()
	abc := ab.NewAtomicBroadcastClient(cc)

	// create a deliver stream
	stream, err := abc.Deliver(context.TODO())
	require.NoError(t, err)
	defer stream.CloseSend()

	// prepare request envelope with wrong channel ID
	block, _ := pem.Decode(ckp.Cert)
	require.True(t, block != nil && block.Type == "CERTIFICATE")
	tlsCertHash := util.ComputeSHA256(block.Bytes)

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"wrongchannel",
		nil,
		delivery.NextSeekInfo(1),
		int32(0),
		uint64(0),
		tlsCertHash,
	)
	require.NoError(t, err)

	// send request envelope
	err = stream.Send(requestEnvelope)
	require.NoError(t, err)

	// expect to recv response with NOT_FOUND status
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, resp.GetStatus(), common.Status_NOT_FOUND)
}

func createAssemblerClientConn(t *testing.T, assemblerEndpoint string, assemblerRootCAs [][]byte, ckp *tlsgen.CertKeyPair, tls string) *grpc.ClientConn {
	cc := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               ckp.Key,
			Certificate:       ckp.Cert,
			RequireClientCert: tls == "mTLS",
			UseTLS:            tls != "none",
			ServerRootCAs:     assemblerRootCAs,
		},
		DialTimeout: time.Second * 10,
	}

	gRPCAssemblerClientConn, err := cc.Dial(assemblerEndpoint)
	require.NoError(t, err)
	return gRPCAssemblerClientConn
}

// createDeliveryClientAndPull connects to the delivery service in the assembler and tries to pull a number of blocks.
// If signer is non-nil, the seek request is signed with it (required when the assembler enforces a real
// ChannelReaders policy); pass nil when the assembler uses a permissive/mock policy.
func createDeliveryClientAndPull(t *testing.T, assemblerEndpoint string, assemblerRootCAs [][]byte, ca tlsgen.CA, numBlocksToPull int, tls string, signer identity.SignerSerializer) error {
	ckp, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	gRPCAssemblerClientConn := createAssemblerClientConn(t, assemblerEndpoint, assemblerRootCAs, ckp, tls)
	defer gRPCAssemblerClientConn.Close()

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver stream
	stream, err := abc.Deliver(context.TODO())
	require.NoError(t, err)
	defer stream.CloseSend()

	var tlsCertHash []byte
	if tls == "mTLS" {
		block, _ := pem.Decode(ckp.Cert)
		require.True(t, block != nil && block.Type == "CERTIFICATE")
		tlsCertHash = util.ComputeSHA256(block.Bytes)
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		signer,
		delivery.NextSeekInfo(1),
		int32(0),
		uint64(0),
		tlsCertHash,
	)
	require.NoError(t, err)

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

// Scenario:
//  1. Generate the crypto material and config block of a two party network, and start an
//     assembler that authorizes deliver requests against the MSPs that config block carries.
//  2. Commit one block, so the assembler has something to deliver.
//  3. Pull with the identity of a user of a party of the channel, and expect the block.
//  4. Pull with the same key material under an MSP identifier that is not in the channel, and
//     expect the request to be forbidden.
//  5. Pull with an unsigned request, and expect the request to be forbidden.
func TestAssemblerDeliver_ChannelReadersPolicy(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 2
	partyID := types.PartyID(1)

	dir, bundle := generateChannelConfig(t, numParties)

	batchersStub, batcherInfos, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(0), ca)
	defer cleanup()

	shards := []config.ShardInfo{{ShardId: types.ShardID(0), Batchers: batcherInfos}}

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Stop()

	assembler, assemblerEndpoint := newAssemblerTestWithBundle(t, partyID, ca, shards,
		consenterStub.consenterInfo, time.Second, false, nil, bundle)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()
	batch := types.NewSimpleBatch(0, 2, types.BatchSequence(0), types.BatchedRequests{[]byte{1}}, 0, nil)
	batchersStub[0].SetNextBatch(batch)
	consenterStub.SetNextDecision(obaCreator.Append(batch, types.DecisionNum(1), 0, 1))
	<-consenterStub.decisionSentCh
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 10*time.Second, 100*time.Millisecond)

	assemblerRootCAs := [][]byte{ca.CertBytes()}

	t.Run("MemberOfTheChannel", func(t *testing.T) {
		signer := signutil.CreateTestSigner(t, "org1", dir)

		require.NoError(t, createDeliveryClientAndPull(t, assemblerEndpoint, assemblerRootCAs, ca, 1, "TLS", signer))
	})

	t.Run("MSPNotInTheChannel", func(t *testing.T) {
		signer := testSignerWithMSPID(t, dir, "org1", "orgNotInTheChannel")

		resp := deliverFirstResponse(t, assemblerEndpoint, assemblerRootCAs, ca, signer)
		require.Equal(t, common.Status_FORBIDDEN, resp.GetStatus())
	})

	t.Run("Unsigned", func(t *testing.T) {
		resp := deliverFirstResponse(t, assemblerEndpoint, assemblerRootCAs, ca, nil)
		require.Equal(t, common.Status_FORBIDDEN, resp.GetStatus())
	})
}

// generateChannelConfig generates the crypto material and config block of a numParties network,
// and returns the directory holding them along with a bundle over that config block. The bundle
// carries the MSP of every party, so policy evaluation against it accepts exactly the identities
// the generated material issues.
func generateChannelConfig(t *testing.T, numParties int) (string, channelconfig.Resources) {
	t.Helper()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	nodesInfo := testutil.CreateNetwork(t, configPath, numParties, 1, "TLS", "TLS")
	require.NotNil(t, nodesInfo)
	// None of the generated nodes is started here, so release the ports reserved for them.
	for _, info := range nodesInfo {
		if info != nil {
			info.Close()
		}
	}
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	blockBytes, err := os.ReadFile(filepath.Join(dir, "bootstrap", "bootstrap.block"))
	require.NoError(t, err)
	configBlock, err := protoutil.UnmarshalBlock(blockBytes)
	require.NoError(t, err)

	return dir, bundleFromBlock(t, configBlock)
}

// testSignerWithMSPID returns a signer holding the key and certificate of the client user of
// mspID, presenting them under claimedMSPID. Passing a claimedMSPID that the channel does not
// know produces an identity no channel policy can be satisfied by.
func testSignerWithMSPID(t *testing.T, dir string, mspID string, claimedMSPID string) identity.SignerSerializer {
	t.Helper()

	clientName := fmt.Sprintf("client@%s", mspID)
	userMSPDir := filepath.Join(dir, "crypto", "ordererOrganizations", mspID, "users", clientName, "msp")
	signer, err := signutil.NewTestSigner(
		filepath.Join(userMSPDir, "keystore", "priv_sk"),
		filepath.Join(userMSPDir, "signcerts", clientName+"-cert.pem"),
		claimedMSPID,
	)
	require.NoError(t, err)
	return signer
}

// deliverFirstResponse sends a single seek request signed by the given signer, and returns the
// first response the assembler sends back.
func deliverFirstResponse(t *testing.T, assemblerEndpoint string, assemblerRootCAs [][]byte, ca tlsgen.CA, signer identity.SignerSerializer) *ab.DeliverResponse {
	t.Helper()

	ckp, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	cc := createAssemblerClientConn(t, assemblerEndpoint, assemblerRootCAs, ckp, "TLS")
	defer cc.Close()

	stream, err := ab.NewAtomicBroadcastClient(cc).Deliver(context.TODO())
	require.NoError(t, err)
	defer stream.CloseSend()

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO, "arma", signer, delivery.NextSeekInfo(1), int32(0), uint64(0), nil,
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(requestEnvelope))

	resp, err := stream.Recv()
	require.NoError(t, err)
	return resp
}
