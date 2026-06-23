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

func TestConsenterBlockVerifier_Clone(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &ConsenterBlockVerifier{
		channelID: "test-channel",
		lastBlockHeader: &common.BlockHeader{
			Number:       5,
			PreviousHash: []byte("prev-hash"),
			DataHash:     []byte("data-hash"),
		},
		logger: logger,
	}

	cloned := verifier.Clone()
	assert.NotNil(t, cloned)

	clonedVerifier, ok := cloned.(*ConsenterBlockVerifier)
	require.True(t, ok)

	assert.Equal(t, verifier.channelID, clonedVerifier.channelID)
	assert.Equal(t, verifier.lastBlockHeader.Number, clonedVerifier.lastBlockHeader.Number)
	assert.Equal(t, verifier.lastBlockHeader.PreviousHash, clonedVerifier.lastBlockHeader.PreviousHash)
	assert.Equal(t, verifier.lastBlockHeader.DataHash, clonedVerifier.lastBlockHeader.DataHash)
}

func TestConsenterBlockVerifier_UpdateBlockHeader(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &ConsenterBlockVerifier{
		channelID: "test-channel",
		lastBlockHeader: &common.BlockHeader{
			Number: 5,
		},
		logger: logger,
	}

	newBlock := &common.Block{
		Header: &common.BlockHeader{
			Number:       6,
			PreviousHash: []byte("prev-hash"),
			DataHash:     []byte("data-hash"),
		},
	}

	verifier.UpdateBlockHeader(newBlock)

	assert.Equal(t, uint64(6), verifier.lastBlockHeader.Number)
	assert.Equal(t, []byte("prev-hash"), verifier.lastBlockHeader.PreviousHash)
	assert.Equal(t, []byte("data-hash"), verifier.lastBlockHeader.DataHash)
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
		name                string
		blockData           *common.BlockData
		sigVerifierFunc     SigVerifierFunc
		expectedErrContains string
	}{
		{
			name:                "nil data",
			blockData:           nil,
			expectedErrContains: "does not have data",
		},
		{
			name:                "empty data",
			blockData:           &common.BlockData{Data: [][]byte{}},
			expectedErrContains: "data length is zero",
		},
		{
			name:      "data hash mismatch",
			blockData: &common.BlockData{Data: [][]byte{[]byte("some-transaction-data")}},
			sigVerifierFunc: func(block *common.Block, verifyData bool) error {
				return nil // Mock signature verification
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

			block := &common.Block{
				Header: &common.BlockHeader{
					Number:       6,
					PreviousHash: correctPreviousHash,
					DataHash:     []byte("wrong-hash"), // Intentionally wrong for data hash mismatch test
				},
				Metadata: &common.BlockMetadata{
					Metadata: make([][]byte, len(common.BlockMetadataIndex_name)),
				},
				Data: tc.blockData,
			}

			err := verifier.VerifyBlock(block)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)
		})
	}
}

func TestConsenterBlockVerifier_VerifyBlockAttestation_NilBlock(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &ConsenterBlockVerifier{
		channelID: "test-channel",
		lastBlockHeader: &common.BlockHeader{
			Number: 5,
		},
		logger: logger,
	}

	err := verifier.VerifyBlockAttestation(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block must be different from nil")
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
