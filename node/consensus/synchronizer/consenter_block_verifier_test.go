/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/block"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsenterBlockVerifierCreator_CreateBlockVerifier(t *testing.T) {
	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
	require.NoError(t, err)
	logger := flogging.MustGetLogger("test")
	creator := &ConsenterBlockVerifierCreator{}

	testCases := []struct {
		name                string
		configBlock         *common.Block
		lastBlock           *common.Block
		expectedErrContains string
	}{
		{
			name:                "nil config block",
			configBlock:         nil,
			lastBlock:           &common.Block{Header: &common.BlockHeader{Number: 1}},
			expectedErrContains: "config block is nil",
		},
		{
			name:                "nil config block header",
			configBlock:         &common.Block{Header: nil},
			lastBlock:           &common.Block{Header: &common.BlockHeader{Number: 1}},
			expectedErrContains: "config block header is nil",
		},
		{
			name:                "nil last block",
			configBlock:         block.BlockWithGroups(&common.ConfigGroup{}, "test-channel", 0),
			lastBlock:           nil,
			expectedErrContains: "last block is nil",
		},
		{
			name:                "nil last block header",
			configBlock:         block.BlockWithGroups(&common.ConfigGroup{}, "test-channel", 0),
			lastBlock:           &common.Block{Header: nil},
			expectedErrContains: "last verified block header is nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			verifier, err := creator.CreateBlockVerifier(tc.configBlock, tc.lastBlock, cryptoProvider, logger)
			assert.Nil(t, verifier)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)
		})
	}
}

func TestConsenterBlockVerifier_UpdateBlockHeader_NilBlock(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &ConsenterBlockVerifier{
		channelID: "test-channel",
		lastBlockHeader: &common.BlockHeader{
			Number: 5,
		},
		logger: logger,
	}

	verifier.UpdateBlockHeader(nil)

	assert.Equal(t, uint64(5), verifier.lastBlockHeader.Number)
}

func TestConsenterBlockVerifier_VerifyBlock(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	lastBlockHeader := &common.BlockHeader{
		Number:       5,
		PreviousHash: []byte("some-prev-hash"),
		DataHash:     []byte("some-data-hash"),
	}

	correctPreviousHash := protoutil.BlockHeaderHash(lastBlockHeader)

	testCases := []struct {
		name string
		// block is used directly when set.
		// When useDefaultBlock is true, a default well-formed block
		// (number=6, correct previousHash, wrong dataHash) is built and
		// blockData is attached to it.
		block               *common.Block
		useDefaultBlock     bool
		blockData           *common.BlockData
		sigVerifierFunc     SigVerifierFunc
		expectedErrContains string
	}{
		// ── verifyHeader error cases ──────────────────────────────────────────
		{
			// nil block → "block must be different from nil"
			name:                "nil block",
			block:               nil,
			expectedErrContains: "block must be different from nil",
		},
		{
			// nil block header → "header must be different from nil"
			name: "nil block header",
			block: &common.Block{
				Header:   nil,
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			expectedErrContains: "header must be different from nil",
		},
		{
			// wrong block number → "expected block number is [6] but actual … is [99]"
			name: "wrong block number",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 99, PreviousHash: correctPreviousHash},
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			expectedErrContains: "expected block number is [6] but actual block number inside block is [99]",
		},
		{
			// wrong previous hash → "Header.PreviousHash … is different from Hash(block.Header)"
			name: "wrong previous hash",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 6, PreviousHash: []byte("wrong-prev-hash")},
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			expectedErrContains: "Header.PreviousHash of block [6] is different from Hash(block.Header) of previous block",
		},
		// ── verifyMetadata error cases ────────────────────────────────────────
		{
			// nil metadata → "does not have metadata or contains too few entries"
			name: "nil metadata",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 6, PreviousHash: correctPreviousHash},
				Metadata: nil,
			},
			expectedErrContains: "does not have metadata or contains too few entries",
		},
		{
			// too few metadata entries → same error
			name: "too few metadata entries",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 6, PreviousHash: correctPreviousHash},
				Metadata: &common.BlockMetadata{Metadata: [][]byte{}},
			},
			expectedErrContains: "does not have metadata or contains too few entries",
		},
		// ── post-metadata error cases ─────────────────────────────────────────
		{
			name:                "nil data",
			useDefaultBlock:     true,
			blockData:           nil,
			expectedErrContains: "does not have data",
		},
		{
			name:                "empty data",
			useDefaultBlock:     true,
			blockData:           &common.BlockData{Data: [][]byte{}},
			expectedErrContains: "data length is zero",
		},
		{
			name:            "data hash mismatch",
			useDefaultBlock: true,
			blockData:       &common.BlockData{Data: [][]byte{[]byte("some-transaction-data")}},
			sigVerifierFunc: func(block *common.Block, verifyData bool) error {
				return nil
			},
			expectedErrContains: "Header.DataHash is different from Hash(block.Data)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			verifier := &ConsenterBlockVerifier{
				channelID:       "test-channel",
				lastBlockHeader: lastBlockHeader,
				logger:          logger,
				sigVerifierFunc: tc.sigVerifierFunc,
			}

			b := tc.block
			if tc.useDefaultBlock {
				b = &common.Block{
					Header: &common.BlockHeader{
						Number:       6,
						PreviousHash: correctPreviousHash,
						DataHash:     []byte("wrong-hash"),
					},
					Metadata: &common.BlockMetadata{
						Metadata: make([][]byte, len(common.BlockMetadataIndex_name)),
					},
					Data: tc.blockData,
				}
			}

			err := verifier.VerifyBlock(b)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)
		})
	}
}

func TestConsenterBlockVerifier_VerifyBlockAttestation(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	lastBlockHeader := &common.BlockHeader{
		Number:       5,
		PreviousHash: []byte("some-prev-hash"),
		DataHash:     []byte("some-data-hash"),
	}
	correctPreviousHash := protoutil.BlockHeaderHash(lastBlockHeader)

	// verifyHeader and verifyMetadata error paths are already covered by
	// TestConsenterBlockVerifier_VerifyBlock which calls the same private methods.
	// This table covers only what is unique to VerifyBlockAttestation: the
	// sig-verifier call and the success path.
	testCases := []struct {
		name            string
		block           *common.Block
		sigVerifierFunc SigVerifierFunc
		expectedErr     string // non-empty → expect error containing this string
	}{
		{
			// sig verifier returns error
			name: "sig verifier error",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 6, PreviousHash: correctPreviousHash},
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			sigVerifierFunc: func(_ *common.Block, _ bool) error {
				return errors.New("sig verification failed")
			},
			expectedErr: "failed to verify signatures of block attestation",
		},
		{
			// happy path: sig verifier succeeds
			name: "success",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 6, PreviousHash: correctPreviousHash},
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			sigVerifierFunc: func(_ *common.Block, _ bool) error { return nil },
			expectedErr:     "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			verifier := &ConsenterBlockVerifier{
				channelID:       "test-channel",
				lastBlockHeader: lastBlockHeader,
				logger:          logger,
				sigVerifierFunc: tc.sigVerifierFunc,
			}

			err := verifier.VerifyBlockAttestation(tc.block)
			if tc.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

func TestConsenterBlockVerifier_VerifyHeader_WithProperBlocks(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	// Create a sequence of blocks with proper hashing
	block1 := protoutil.NewBlock(1, []byte("genesis-hash"))
	block1.Header.DataHash = []byte("data-hash-1")

	verifier := &ConsenterBlockVerifier{
		channelID:       "test-channel",
		lastBlockHeader: block1.Header,
		logger:          logger,
	}

	// Create block 2 with correct previous hash
	block2 := protoutil.NewBlock(2, protoutil.BlockHeaderHash(block1.Header))
	block2.Header.DataHash = []byte("data-hash-2")

	err := verifier.verifyHeader(block2)
	assert.NoError(t, err)
}

func TestConsenterBlockVerifier_VerifyHeader_BlockSequence(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	// Test a sequence of blocks
	blocks := make([]*common.Block, 5)
	blocks[0] = protoutil.NewBlock(0, nil) // Genesis
	blocks[0].Header.DataHash = []byte("genesis-data")

	for i := 1; i < 5; i++ {
		blocks[i] = protoutil.NewBlock(uint64(i), protoutil.BlockHeaderHash(blocks[i-1].Header))
		blocks[i].Header.DataHash = []byte{byte(i)}
	}

	verifier := &ConsenterBlockVerifier{
		channelID:       "test-channel",
		lastBlockHeader: blocks[0].Header,
		logger:          logger,
	}

	// Verify each block in sequence
	for i := 1; i < 5; i++ {
		err := verifier.verifyHeader(blocks[i])
		assert.NoError(t, err, "Block %d should verify successfully", i)
		verifier.lastBlockHeader = blocks[i].Header
	}
}

func TestConsenterBlockVerifier_Clone_PreservesState(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	originalHeader := &common.BlockHeader{
		Number:       42,
		PreviousHash: []byte("prev-hash"),
		DataHash:     []byte("data-hash"),
	}

	original := &ConsenterBlockVerifier{
		channelID:       "test-channel",
		lastBlockHeader: originalHeader,
		logger:          logger,
	}

	cloned := original.Clone().(*ConsenterBlockVerifier)

	// Verify the clone has the same state
	assert.Equal(t, original.channelID, cloned.channelID)
	assert.Equal(t, original.lastBlockHeader.Number, cloned.lastBlockHeader.Number)
	assert.Equal(t, original.lastBlockHeader.PreviousHash, cloned.lastBlockHeader.PreviousHash)

	// Modify the clone and verify original is unchanged
	cloned.lastBlockHeader = &common.BlockHeader{Number: 100}
	assert.Equal(t, uint64(42), original.lastBlockHeader.Number, "Original should not be affected by clone modification")
}

func TestConsenterBlockVerifier_UpdateBlockHeader_UpdatesState(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &ConsenterBlockVerifier{
		channelID: "test-channel",
		lastBlockHeader: &common.BlockHeader{
			Number: 5,
		},
		logger: logger,
	}

	// Create a sequence of blocks
	for i := 6; i <= 10; i++ {
		newBlock := &common.Block{
			Header: &common.BlockHeader{
				Number:       uint64(i),
				PreviousHash: []byte{byte(i - 1)},
				DataHash:     []byte{byte(i)},
			},
		}

		verifier.UpdateBlockHeader(newBlock)
		assert.Equal(t, uint64(i), verifier.lastBlockHeader.Number)
	}
}
