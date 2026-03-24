/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger_test

import (
	"encoding/asn1"
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/node/ledger/mocks"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
)

// createProposal creates a proposal with the given block number
func createProposal(blockNum uint64) smartbft_types.Proposal {
	header := &state.Header{Num: types.DecisionNum(blockNum)}
	return smartbft_types.Proposal{
		Header: header.Serialize(),
	}
}

// assertBlocksExist verifies that all specified blocks can be retrieved from the ledger
func assertBlocksExist(t *testing.T, ledger *ledger.ConsensusLedger, blockNums ...uint64) {
	t.Helper()
	for _, num := range blockNums {
		_, err := ledger.RetrieveBlockByNumber(num)
		require.NoError(t, err, "block %d should exist", num)
	}
}

// createSignatures creates test signatures with proper ASN1 marshaling
func createSignatures(signerIDs []uint64) []smartbft_types.Signature {
	var signatures []smartbft_types.Signature
	for _, id := range signerIDs {
		identifierHeader := state.NewIdentifierHeaderOrPanic(types.PartyID(id))
		msg := &state.MessageToSign{
			IdentifierHeader: protoutil.MarshalOrPanic(identifierHeader),
		}
		msgs := make([][]byte, 0)
		msgs = append(msgs, msg.Marshal())
		msgsRaw, err := asn1.Marshal(msgs)
		if err != nil {
			panic(err)
		}
		signatures = append(signatures, smartbft_types.Signature{
			ID:    id,
			Msg:   msgsRaw,
			Value: []byte("signature-value"),
		})
	}
	return signatures
}

func TestConsensusLedger(t *testing.T) {
	t.Run("empty ledger initialization", func(t *testing.T) {
		dir := t.TempDir()
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)
		require.NotNil(t, l)
		require.Zero(t, l.Height())

		_, err = l.RetrieveBlockByNumber(0)
		require.Error(t, err, "should error when retrieving from empty ledger")
	})

	t.Run("append and retrieve blocks", func(t *testing.T) {
		dir := t.TempDir()
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)

		// Test appending blocks sequentially
		appendTests := []struct {
			blockNum       uint64
			expectedHeight uint64
		}{
			{blockNum: 0, expectedHeight: 1},
			{blockNum: 1, expectedHeight: 2},
			{blockNum: 2, expectedHeight: 3},
		}

		var prevHash []byte
		for _, tt := range appendTests {
			proposal := createProposal(tt.blockNum)
			block := state.CreateBlockToAppendFromDecision(tt.blockNum, proposal, nil, prevHash, tt.blockNum)
			prevHash = protoutil.BlockHeaderHash(block.Header)
			l.Append(block)
			require.Equal(t, tt.expectedHeight, l.Height())

			_, err = l.RetrieveBlockByNumber(tt.blockNum)
			require.NoError(t, err, "should retrieve block %d", tt.blockNum)
		}

		// Verify all blocks are accessible
		assertBlocksExist(t, l, 0, 1, 2)
	})

	t.Run("listener notification on append", func(t *testing.T) {
		dir := t.TempDir()
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)

		// Append first block without listener
		proposal := createProposal(0)
		block := state.CreateBlockToAppendFromDecision(0, proposal, nil, nil, 0)
		l.Append(block)
		require.Equal(t, uint64(1), l.Height())

		// Register listener and append second block
		listener := &mocks.FakeAppendListener{}
		l.RegisterAppendListener(listener)

		proposal = createProposal(1)
		prevHash := protoutil.BlockHeaderHash(block.Header)
		block1 := state.CreateBlockToAppendFromDecision(1, proposal, nil, prevHash, 1)
		l.Append(block1)
		require.Equal(t, uint64(2), l.Height())
		require.Equal(t, 1, listener.OnAppendCallCount(), "listener should be notified once")
	})

	t.Run("persistence after close and reopen", func(t *testing.T) {
		dir := t.TempDir()

		// Create ledger and append blocks
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)

		listener := &mocks.FakeAppendListener{}
		l.RegisterAppendListener(listener)

		proposal := createProposal(0)
		block := state.CreateBlockToAppendFromDecision(0, proposal, nil, nil, 0)
		l.Append(block)
		proposal = createProposal(1)
		prevHash := protoutil.BlockHeaderHash(block.Header)
		block1 := state.CreateBlockToAppendFromDecision(1, proposal, nil, prevHash, 1)
		l.Append(block1)

		require.Equal(t, uint64(2), l.Height())
		require.Equal(t, 2, listener.OnAppendCallCount())

		// Close and reopen ledger
		l.Close()

		l, err = ledger.NewConsensusLedger(dir)
		require.NoError(t, err)
		require.NotNil(t, l)
		require.Equal(t, uint64(2), l.Height(), "height should persist after reopen")

		// Verify blocks are still accessible
		assertBlocksExist(t, l, 0, 1)

		// Old listener should not be notified after reopen
		require.Equal(t, 2, listener.OnAppendCallCount(), "old listener count should not change")

		// Register new listener and append another block
		newListener := &mocks.FakeAppendListener{}
		l.RegisterAppendListener(newListener)

		proposal = createProposal(2)
		prevHash = protoutil.BlockHeaderHash(block1.Header)
		block2 := state.CreateBlockToAppendFromDecision(2, proposal, nil, prevHash, 2)
		l.Append(block2)
		require.Equal(t, uint64(3), l.Height())
		require.Equal(t, 1, newListener.OnAppendCallCount(), "new listener should be notified")

		// Verify all blocks are accessible
		assertBlocksExist(t, l, 0, 1, 2)
	})

	t.Run("duplicate close is safe", func(t *testing.T) {
		dir := t.TempDir()
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)

		l.Close()
		require.NotPanics(t, func() { l.Close() }, "closing twice should not panic")
	})

	t.Run("panic on duplicate block number", func(t *testing.T) {
		dir := t.TempDir()
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)

		var prevHash []byte
		// Append blocks 0, 1, 2
		for i := uint64(0); i < 3; i++ {
			proposal := createProposal(i)
			block := state.CreateBlockToAppendFromDecision(i, proposal, nil, prevHash, i)
			prevHash = protoutil.BlockHeaderHash(block.Header)
			l.Append(block)
		}
		require.Equal(t, uint64(3), l.Height())

		// Attempt to append block 2 again (should panic)
		duplicateProposal := createProposal(2)
		block := state.CreateBlockToAppendFromDecision(2, duplicateProposal, nil, prevHash, 2)
		require.PanicsWithError(t, "block number should have been 3 but was 2", func() {
			l.Append(block)
		})

		// Verify ledger state is unchanged after panic
		require.Equal(t, uint64(3), l.Height(), "height should remain unchanged after panic")
		assertBlocksExist(t, l, 0, 1, 2)
	})

	t.Run("append with signatures", func(t *testing.T) {
		dir := t.TempDir()
		l, err := ledger.NewConsensusLedger(dir)
		require.NoError(t, err)

		// Create proposal for block 0
		proposal := createProposal(0)

		// Create signatures from multiple signers
		signerIDs := []uint64{1, 2, 3}
		signatures := createSignatures(signerIDs)

		// Append block with signatures
		block := state.CreateBlockToAppendFromDecision(0, proposal, signatures, nil, 0)
		l.Append(block)
		require.Equal(t, uint64(1), l.Height())

		// Retrieve and verify the block
		block, err = l.RetrieveBlockByNumber(0)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.Equal(t, uint64(0), block.Header.Number)

		// Verify signatures are stored in block metadata
		require.NotNil(t, block.Metadata)
		require.NotEmpty(t, block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES],
			"signature metadata should not be empty when signatures are provided")

		// Append another block with different number of signatures
		proposal2 := createProposal(1)
		signatures2 := createSignatures([]uint64{1, 2, 3, 4})
		block2 := state.CreateBlockToAppendFromDecision(1, proposal2, signatures2, protoutil.BlockHeaderHash(block.Header), 1)
		l.Append(block2)
		require.Equal(t, uint64(2), l.Height())

		block2, err = l.RetrieveBlockByNumber(1)
		require.NoError(t, err)
		require.NotEmpty(t, block2.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES],
			"second block should also have signature metadata")

		// Verify both blocks are accessible
		assertBlocksExist(t, l, 0, 1)
	})
}
