/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery_test

import (
	"encoding/asn1"
	"testing"
	"time"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	ledgerMocks "github.com/hyperledger/fabric-x-orderer/node/ledger/mocks"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
)

const basPerDecision = 2

// Scenario:
//  1. Serve the consensus deliver service over TLS, over a decision ledger holding decisions 1 to 4, each
//     with 2 BAs; the assembler ledger stays at the end of decision 0.
//  2. Replicate BAs, consume the first 3 (decision 1 and the first BA of decision 2), leaving the rest
//     unconsumed and not in the assembler ledger.
//  3. Restart the deliver service, append decisions 5 and 6, and consume the rest.
//  4. Expect every BA from the second BA of decision 2 to the last BA of decision 6 exactly once and in order.
func TestConsensusBAReplicatorResumesAfterTheLastBAOnReconnect(t *testing.T) {
	server, replicator := replicateBAs(t, 4, &state.OrderingInformation{DecisionNum: 0, BatchIndex: 0, BatchCount: 1})
	bas := replicator.Replicate()

	requireNextBA(t, bas, 1, 0)
	requireNextBA(t, bas, 1, 1)
	requireNextBA(t, bas, 2, 0)

	server.restart(t)
	server.appendBADecisions(t, 2)

	requireNextBA(t, bas, 2, 1)
	for decision := types.DecisionNum(3); decision <= 6; decision++ {
		for index := range basPerDecision {
			requireNextBA(t, bas, decision, index)
		}
	}
	require.Never(t, func() bool { return len(bas) > 0 }, time.Second, 100*time.Millisecond)
}

// Scenario:
//  1. Serve the consensus deliver service over TLS, over a decision ledger holding decisions 1 to 2, each
//     with 2 BAs; the assembler ledger holds the first BA of decision 1.
//  2. Replicate BAs, and expect the second BA of decision 1 first.
//  3. Restart the deliver service before the BA is consumed, append decision 3, and consume the rest.
//  4. Expect every BA from the second BA of decision 1 to the last BA of decision 3 exactly once and in order.
func TestConsensusBAReplicatorResumesWithinADecisionOnReconnect(t *testing.T) {
	server, replicator := replicateBAs(t, 2, &state.OrderingInformation{DecisionNum: 1, BatchIndex: 0, BatchCount: basPerDecision})
	bas := replicator.Replicate()

	require.Eventually(t, func() bool { return len(bas) == 3 }, 10*time.Second, 10*time.Millisecond)
	server.restart(t)
	server.appendBADecisions(t, 1)

	requireNextBA(t, bas, 1, 1)
	for decision := types.DecisionNum(2); decision <= 3; decision++ {
		for index := range basPerDecision {
			requireNextBA(t, bas, decision, index)
		}
	}
	require.Never(t, func() bool { return len(bas) > 0 }, time.Second, 100*time.Millisecond)
}

// replicateBAs serves decisions 1 to count, each with 2 BAs, and returns a BA replicator whose assembler
// ledger stays at lastInLedger.
func replicateBAs(t *testing.T, count int, lastInLedger *state.OrderingInformation) (*decisionServer, *delivery.ConsensusBAReplicator) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	signer, cert := signutil.NewSelfSignedSigner(t, "org")
	bundle := &mocks.FakeConfigResources{}
	configutil.AdmitSigners(bundle, cert)

	server := serveDecisionsOverTLS(t, "arma", bundle, ca)
	server.appendDecisions(t, 1)
	server.appendBADecisions(t, count)

	assemblerLedger := &ledgerMocks.FakeAssemblerLedgerReaderWriter{}
	assemblerLedger.LastOrderingInfoReturns(lastInLedger, nil)

	clientKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)
	replicator := delivery.NewConsensusBAReplicator(
		"arma", []config.RawBytes{ca.CertBytes()}, clientKeyPair.Key, clientKeyPair.Cert, server.endpoint, assemblerLedger,
		signer, testutil.CreateLogger(t, 1),
	)
	t.Cleanup(replicator.Stop)

	return server, replicator
}

func requireNextBA(t *testing.T, bas <-chan *state.AvailableBatchOrdered, decision types.DecisionNum, index int) {
	select {
	case ba := <-bas:
		require.Equal(t, decision, ba.OrderingInformation.DecisionNum)
		require.Equal(t, index, ba.OrderingInformation.BatchIndex)
	case <-time.After(10 * time.Second):
		t.Fatalf("BA %d of decision %d was not replicated", index, decision)
	}
}

// appendBADecisions appends count decisions to the ledger, block i holding decision i, each with 2 BAs.
func (ds *decisionServer) appendBADecisions(t *testing.T, count int) {
	for range count {
		height := ds.ledger.Height()
		prev, err := ds.ledger.RetrieveBlockByNumber(height - 1)
		require.NoError(t, err)

		num := types.DecisionNum(height)
		header := &state.Header{Num: num}
		for index := range basPerDecision {
			header.AvailableCommonBlocks = append(header.AvailableCommonBlocks, availableCommonBlock(t, num, index))
		}

		block := state.CreateBlockToAppendFromDecision(
			height, smartbft_types.Proposal{Header: header.Serialize()}, compoundSignatures(t), protoutil.BlockHeaderHash(prev.Header), 0,
		)
		ds.ledger.Append(block)
	}
}

func availableCommonBlock(t *testing.T, decision types.DecisionNum, index int) *common.Block {
	digest := []byte{byte(decision), byte(index)}
	metadata, err := node_ledger.AssemblerBlockMetadataToBytes(
		state.NewAvailableBatch(1, 1, types.BatchSequence(int(decision)*basPerDecision+index), digest),
		&state.OrderingInformation{DecisionNum: decision, BatchIndex: index, BatchCount: basPerDecision},
		0,
	)
	require.NoError(t, err)

	block := &common.Block{Header: &common.BlockHeader{Number: uint64(decision)*basPerDecision + uint64(index), DataHash: digest}}
	protoutil.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = metadata
	return block
}

// compoundSignatures returns the signature of one party over the proposal and over each of the BAs.
func compoundSignatures(t *testing.T) []smartbft_types.Signature {
	values := make([][]byte, basPerDecision+1)
	msgs := make([][]byte, basPerDecision+1)
	for i := range values {
		values[i] = []byte{byte(i)}
		msgs[i] = (&protoutil.MessageToSign{
			IdentifierHeader: protoutil.MarshalOrPanic(protoutil.NewIdentifierHeaderOrPanic(1)),
		}).ASN1MarshalOrPanic()
	}

	valueBytes, err := asn1.Marshal(values)
	require.NoError(t, err)
	msgBytes, err := asn1.Marshal(msgs)
	require.NoError(t, err)

	return []smartbft_types.Signature{{ID: 1, Value: valueBytes, Msg: msgBytes}}
}
