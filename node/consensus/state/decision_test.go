/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestProposalToBytes(t *testing.T) {
	t.Run("valid proposal", func(t *testing.T) {
		proposal := smartbft_types.Proposal{
			Header:   []byte{1, 2, 3},
			Payload:  []byte{4, 5, 6},
			Metadata: []byte{7, 8, 9},
		}
		bytes := ProposalToBytes(proposal)
		assert.NotNil(t, bytes)
		assert.NotEmpty(t, bytes)
	})

	t.Run("empty proposal", func(t *testing.T) {
		proposal := smartbft_types.Proposal{}
		bytes := ProposalToBytes(proposal)
		assert.NotNil(t, bytes)
	})

	t.Run("proposal with nil fields", func(t *testing.T) {
		proposal := smartbft_types.Proposal{
			Header:   nil,
			Payload:  []byte{1, 2, 3},
			Metadata: nil,
		}
		bytes := ProposalToBytes(proposal)
		assert.NotNil(t, bytes)
	})
}

func TestBytesToProposal(t *testing.T) {
	t.Run("valid bytes", func(t *testing.T) {
		original := smartbft_types.Proposal{
			Header:   []byte{1, 2, 3},
			Payload:  []byte{4, 5, 6},
			Metadata: []byte{7, 8, 9},
		}
		bytes := ProposalToBytes(original)
		proposal, err := BytesToProposal(bytes)
		assert.NoError(t, err)
		assert.Equal(t, original, proposal)
	})

	t.Run("empty bytes", func(t *testing.T) {
		proposal, err := BytesToProposal([]byte{})
		assert.Error(t, err)
		assert.Equal(t, smartbft_types.Proposal{}, proposal)
	})

	t.Run("nil bytes", func(t *testing.T) {
		proposal, err := BytesToProposal(nil)
		assert.Error(t, err)
		assert.Equal(t, smartbft_types.Proposal{}, proposal)
	})

	t.Run("invalid bytes", func(t *testing.T) {
		proposal, err := BytesToProposal([]byte{1, 2, 3})
		assert.Error(t, err)
		assert.Equal(t, smartbft_types.Proposal{}, proposal)
	})
}

func TestDecisionSignaturesToBytes(t *testing.T) {
	t.Run("valid signatures", func(t *testing.T) {
		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: []byte{1, 2, 3},
				Msg:   []byte{4, 5, 6},
			},
			{
				ID:    2,
				Value: []byte{7, 8, 9},
				Msg:   []byte{10, 11, 12},
			},
		}
		bytes := DecisionSignaturesToBytes(signatures)
		assert.NotNil(t, bytes)
		assert.NotEmpty(t, bytes)
	})

	t.Run("empty signatures", func(t *testing.T) {
		signatures := []smartbft_types.Signature{}
		bytes := DecisionSignaturesToBytes(signatures)
		assert.NotNil(t, bytes)
	})

	t.Run("nil signatures", func(t *testing.T) {
		bytes := DecisionSignaturesToBytes(nil)
		assert.NotNil(t, bytes)
	})

	t.Run("signatures with nil fields", func(t *testing.T) {
		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: nil,
				Msg:   []byte{1, 2, 3},
			},
		}
		bytes := DecisionSignaturesToBytes(signatures)
		assert.NotNil(t, bytes)
	})
}

func TestBytesToDecisionSignatures(t *testing.T) {
	t.Run("valid bytes", func(t *testing.T) {
		original := []smartbft_types.Signature{
			{
				ID:    1,
				Value: []byte{1, 2, 3},
				Msg:   []byte{4, 5, 6},
			},
			{
				ID:    2,
				Value: []byte{7, 8, 9},
				Msg:   []byte{10, 11, 12},
			},
		}
		bytes := DecisionSignaturesToBytes(original)
		signatures, err := BytesToDecisionSignatures(bytes)
		assert.NoError(t, err)
		assert.Equal(t, original, signatures)
	})

	t.Run("empty bytes", func(t *testing.T) {
		signatures, err := BytesToDecisionSignatures([]byte{})
		assert.Error(t, err)
		assert.Nil(t, signatures)
	})

	t.Run("nil bytes", func(t *testing.T) {
		signatures, err := BytesToDecisionSignatures(nil)
		assert.Error(t, err)
		assert.Nil(t, signatures)
	})

	t.Run("invalid bytes", func(t *testing.T) {
		signatures, err := BytesToDecisionSignatures([]byte{1, 2, 3})
		assert.Error(t, err)
		assert.Nil(t, signatures)
	})
}

// Helper functions for TestConsenterBlockToDecision

func createValidTestProposal() smartbft_types.Proposal {
	return smartbft_types.Proposal{
		Header:   []byte{1, 2, 3},
		Payload:  []byte{4, 5, 6},
		Metadata: []byte{7, 8, 9},
	}
}

func createValidTestSignatures() []smartbft_types.Signature {
	return []smartbft_types.Signature{
		{
			ID:    1,
			Value: []byte{1, 2, 3},
			Msg:   []byte{4, 5, 6},
		},
		{
			ID:    2,
			Value: []byte{7, 8, 9},
			Msg:   []byte{10, 11, 12},
		},
	}
}

func createTestBlock(t *testing.T, blockNumber uint64, proposalBytes []byte, metadataEntries [][]byte) *common.Block {
	t.Helper()

	var data *common.BlockData
	if proposalBytes != nil {
		data = &common.BlockData{
			Data: [][]byte{proposalBytes},
		}
	}

	var metadata *common.BlockMetadata
	if metadataEntries != nil {
		metadata = &common.BlockMetadata{
			Metadata: metadataEntries,
		}
	}

	return &common.Block{
		Header: &common.BlockHeader{
			Number: blockNumber,
		},
		Data:     data,
		Metadata: metadata,
	}
}

func assertErrorResult(t *testing.T, decision *smartbft_types.Decision, err error) {
	t.Helper()
	assert.Error(t, err)
	assert.Nil(t, decision)
}

func assertSuccessResult(t *testing.T, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature,
	err error, expectedProposal smartbft_types.Proposal, expectedSignatures []smartbft_types.Signature,
) {
	t.Helper()
	assert.NoError(t, err)
	assert.Equal(t, expectedProposal, proposal)
	assert.Equal(t, expectedSignatures, signatures)
}

func createTestMetadata(signaturesBytes []byte) [][]byte {
	metadata := make([][]byte, 2)
	metadata[common.BlockMetadataIndex_SIGNATURES] = signaturesBytes
	return metadata
}

func createValidTestBlock(t *testing.T, blockNumber uint64) *common.Block {
	t.Helper()
	proposal := createValidTestProposal()
	signatures := createValidTestSignatures()
	proposalBytes := ProposalToBytes(proposal)
	signaturesBytes := DecisionSignaturesToBytes(signatures)
	metadata := createTestMetadata(signaturesBytes)
	return createTestBlock(t, blockNumber, proposalBytes, metadata)
}

func createTestBlockWithNilData(t *testing.T) *common.Block {
	t.Helper()
	return &common.Block{
		Header: &common.BlockHeader{Number: 1},
		Data:   nil,
		Metadata: &common.BlockMetadata{
			Metadata: make([][]byte, 2),
		},
	}
}

func createTestBlockWithEmptyData(t *testing.T) *common.Block {
	t.Helper()
	return &common.Block{
		Header: &common.BlockHeader{Number: 1},
		Data:   &common.BlockData{Data: [][]byte{}},
		Metadata: &common.BlockMetadata{
			Metadata: make([][]byte, 2),
		},
	}
}

func createTestBlockWithNilMetadata(t *testing.T, proposalBytes []byte) *common.Block {
	t.Helper()
	block := createTestBlock(t, 1, proposalBytes, nil)
	block.Metadata = nil
	return block
}

func TestConsenterBlockToProposal(t *testing.T) {
	tests := []struct {
		name             string
		setupBlock       func(t *testing.T) *common.Block
		expectError      bool
		expectedProposal smartbft_types.Proposal
	}{
		{
			// Validates that ConsenterBlockToProposal correctly extracts
			// proposal from a properly formatted block
			name: "valid block with proposal",
			setupBlock: func(t *testing.T) *common.Block {
				return createValidTestBlock(t, 42)
			},
			expectError:      false,
			expectedProposal: createValidTestProposal(),
		},
		{
			// Validates that ConsenterBlockToProposal properly handles
			// nil blocks (should return error)
			name: "nil block",
			setupBlock: func(t *testing.T) *common.Block {
				return nil
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToProposal properly handles
			// blocks with nil header (should return error)
			name: "block with nil header",
			setupBlock: func(t *testing.T) *common.Block {
				block := createValidTestBlock(t, 1)
				block.Header = nil
				return block
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToProposal properly handles
			// blocks where the Data field is nil (should return error)
			name: "block with nil data",
			setupBlock: func(t *testing.T) *common.Block {
				return createTestBlockWithNilData(t)
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToProposal properly handles
			// blocks where the Data field contains an empty slice (should return error)
			name: "block with empty data",
			setupBlock: func(t *testing.T) *common.Block {
				return createTestBlockWithEmptyData(t)
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToProposal properly handles
			// blocks where the Metadata field is nil (should return error)
			name: "block with nil metadata passes as metadata is not accessed",
			setupBlock: func(t *testing.T) *common.Block {
				proposal := createValidTestProposal()
				proposalBytes := ProposalToBytes(proposal)
				return createTestBlockWithNilMetadata(t, proposalBytes)
			},
			expectError:      false,
			expectedProposal: createValidTestProposal(),
		},
		{
			// Validates that ConsenterBlockToProposal properly handles
			// blocks with empty metadata (should return error)
			name: "block with empty metadata passes as metadata is not accessed",
			setupBlock: func(t *testing.T) *common.Block {
				proposal := createValidTestProposal()
				proposalBytes := ProposalToBytes(proposal)
				return createTestBlock(t, 1, proposalBytes, [][]byte{})
			},
			expectError:      false,
			expectedProposal: createValidTestProposal(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := tt.setupBlock(t)

			proposal, err := ConsenterBlockToProposal(block)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, proposal)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, proposal)
				assert.Equal(t, tt.expectedProposal, *proposal)
			}
		})
	}
}

func TestConsenterBlockToDecision(t *testing.T) {
	tests := []struct {
		name               string
		setupBlock         func(t *testing.T) *common.Block
		expectError        bool
		expectedProposal   smartbft_types.Proposal
		expectedSignatures []smartbft_types.Signature
	}{
		{
			// Validates that ConsenterBlockToDecision correctly extracts
			// proposal and signatures from a properly formatted block
			name: "valid block with proposal and signatures",
			setupBlock: func(t *testing.T) *common.Block {
				return createValidTestBlock(t, 42)
			},
			expectError:        false,
			expectedProposal:   createValidTestProposal(),
			expectedSignatures: createValidTestSignatures(),
		},
		{
			// Validates that ConsenterBlockToDecision properly handles
			// blocks where the Data field is nil (should return error)
			name: "block with nil data",
			setupBlock: func(t *testing.T) *common.Block {
				return createTestBlockWithNilData(t)
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToDecision properly handles
			// blocks where the Data field contains an empty slice (should return error)
			name: "block with empty data",
			setupBlock: func(t *testing.T) *common.Block {
				return createTestBlockWithEmptyData(t)
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToDecision properly handles
			// blocks where the Metadata field is nil (should return error)
			name: "block with nil metadata",
			setupBlock: func(t *testing.T) *common.Block {
				proposal := createValidTestProposal()
				proposalBytes := ProposalToBytes(proposal)
				return createTestBlockWithNilMetadata(t, proposalBytes)
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToDecision properly handles
			// blocks with insufficient metadata entries (should return error)
			name: "block with insufficient metadata entries",
			setupBlock: func(t *testing.T) *common.Block {
				proposal := createValidTestProposal()
				proposalBytes := ProposalToBytes(proposal)
				// Only one metadata entry, need at least two
				return createTestBlock(t, 1, proposalBytes, [][]byte{nil})
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToDecision properly handles
			// blocks with malformed proposal bytes (should return error)
			name: "block with invalid proposal bytes",
			setupBlock: func(t *testing.T) *common.Block {
				signatures := createValidTestSignatures()
				signaturesBytes := DecisionSignaturesToBytes(signatures)
				metadata := createTestMetadata(signaturesBytes)
				// Invalid proposal bytes
				return createTestBlock(t, 1, []byte{1, 2, 3}, metadata)
			},
			expectError: true,
		},
		{
			// Validates that ConsenterBlockToDecision properly handles
			// blocks with malformed signatures bytes (should return error)
			name: "block with invalid signatures bytes",
			setupBlock: func(t *testing.T) *common.Block {
				proposal := createValidTestProposal()
				proposalBytes := ProposalToBytes(proposal)
				// Invalid signatures bytes
				metadata := createTestMetadata([]byte{1, 2, 3})
				return createTestBlock(t, 1, proposalBytes, metadata)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := tt.setupBlock(t)

			decision, err := ConsenterBlockToDecision(block)

			if tt.expectError {
				assertErrorResult(t, decision, err)
			} else {
				assertSuccessResult(t, decision.Proposal, decision.Signatures, err, tt.expectedProposal, tt.expectedSignatures)
			}
		})
	}
}

func TestCreateBlockToAppendFromDecision(t *testing.T) {
	tests := []struct {
		name          string
		blockNum      uint64
		proposal      smartbft_types.Proposal
		signatures    []smartbft_types.Signature
		prevHash      []byte
		lastConfig    uint64
		validateBlock func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64)
	}{
		{
			name:       "valid block with standard values",
			blockNum:   1,
			proposal:   createValidTestProposal(),
			signatures: createValidTestSignatures(),
			prevHash:   []byte{1, 2, 3},
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				assert.Equal(t, prevHash, block.Header.PreviousHash)
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
				assert.Equal(t, protoutil.ComputeBlockDataHash(testData), block.Header.DataHash)
				assert.Equal(t, DecisionSignaturesToBytes(signatures), block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
				assert.Equal(t, proposal.Metadata, block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER])
				metadata, err := protoutil.GetMetadataFromBlock(block, common.BlockMetadataIndex_LAST_CONFIG)
				assert.NoError(t, err)
				assert.NotNil(t, metadata)
				lc := &common.LastConfig{}
				err = proto.Unmarshal(metadata.Value, lc)
				assert.NoError(t, err)
				assert.Equal(t, lastConfig, lc.Index)
			},
		},
		{
			name:       "block with zero block number",
			blockNum:   0,
			proposal:   createValidTestProposal(),
			signatures: createValidTestSignatures(),
			prevHash:   []byte{1, 2, 3},
			lastConfig: 0,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that block number 0 is handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, uint64(0), block.Header.Number)
				assert.Equal(t, prevHash, block.Header.PreviousHash)
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
				assert.Equal(t, protoutil.ComputeBlockDataHash(testData), block.Header.DataHash)
				assert.Equal(t, DecisionSignaturesToBytes(signatures), block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
				assert.Equal(t, proposal.Metadata, block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER])
			},
		},
		{
			name:       "block with nil previous hash",
			blockNum:   1,
			proposal:   createValidTestProposal(),
			signatures: createValidTestSignatures(),
			prevHash:   nil,
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that nil previous hash is handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				assert.Nil(t, block.Header.PreviousHash)
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
				assert.Equal(t, protoutil.ComputeBlockDataHash(testData), block.Header.DataHash)
				assert.Equal(t, DecisionSignaturesToBytes(signatures), block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
			},
		},
		{
			name:       "block with empty previous hash",
			blockNum:   1,
			proposal:   createValidTestProposal(),
			signatures: createValidTestSignatures(),
			prevHash:   []byte{},
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that empty previous hash is handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				assert.Empty(t, block.Header.PreviousHash)
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
				assert.Equal(t, protoutil.ComputeBlockDataHash(testData), block.Header.DataHash)
			},
		},
		{
			name:       "block with empty proposal",
			blockNum:   1,
			proposal:   smartbft_types.Proposal{},
			signatures: createValidTestSignatures(),
			prevHash:   []byte{1, 2, 3},
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that empty proposal is serialized correctly
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
				assert.Equal(t, protoutil.ComputeBlockDataHash(testData), block.Header.DataHash)
				assert.Equal(t, DecisionSignaturesToBytes(signatures), block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
			},
		},
		{
			name:       "block with empty signatures",
			blockNum:   1,
			proposal:   createValidTestProposal(),
			signatures: []smartbft_types.Signature{},
			prevHash:   []byte{1, 2, 3},
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that empty signatures slice is handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				assert.NotNil(t, block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
				assert.Equal(t, DecisionSignaturesToBytes(signatures), block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
			},
		},
		{
			name:       "block with nil signatures",
			blockNum:   1,
			proposal:   createValidTestProposal(),
			signatures: nil,
			prevHash:   []byte{1, 2, 3},
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that nil signatures are handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				assert.NotNil(t, block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES])
			},
		},
		{
			name:     "block with proposal containing nil metadata",
			blockNum: 1,
			proposal: smartbft_types.Proposal{
				Header:   []byte{1, 2, 3},
				Payload:  []byte{4, 5, 6},
				Metadata: nil,
			},
			signatures: createValidTestSignatures(),
			prevHash:   []byte{1, 2, 3},
			lastConfig: 1,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that proposal with nil metadata is handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, blockNum, block.Header.Number)
				assert.Nil(t, block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER])
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
			},
		},
		{
			name:       "block with high block number",
			blockNum:   999999,
			proposal:   createValidTestProposal(),
			signatures: createValidTestSignatures(),
			prevHash:   []byte{1, 2, 3},
			lastConfig: 999999,
			validateBlock: func(t *testing.T, block *common.Block, blockNum uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, lastConfig uint64) {
				// Validates that high block numbers are handled correctly
				assert.NotNil(t, block)
				assert.Equal(t, uint64(999999), block.Header.Number)
				proposalBytes := ProposalToBytes(proposal)
				testData := &common.BlockData{
					Data: [][]byte{proposalBytes},
				}
				assert.Equal(t, testData, block.Data)
				assert.Equal(t, protoutil.ComputeBlockDataHash(testData), block.Header.DataHash)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := CreateBlockToAppendFromDecision(tt.blockNum, tt.proposal, tt.signatures, tt.prevHash, tt.lastConfig)
			tt.validateBlock(t, block, tt.blockNum, tt.proposal, tt.signatures, tt.prevHash, tt.lastConfig)
		})
	}
}
