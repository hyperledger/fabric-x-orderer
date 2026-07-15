/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"encoding/asn1"
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSigVerifierCreator_SigVerifierFromConfig(t *testing.T) {
	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
	require.NoError(t, err)
	logger := flogging.MustGetLogger("test")

	svc := &synchronizer.SigVerifierCreator{
		Logger: logger,
		BCCSP:  cryptoProvider,
	}

	testCases := []struct {
		name                string
		configEnvelope      *common.ConfigEnvelope
		channel             string
		expectedErrContains string
	}{
		{
			name:                "nil configuration",
			configEnvelope:      nil,
			channel:             "",
			expectedErrContains: "nil",
		},
		{
			name:                "missing channel group",
			configEnvelope:      &common.ConfigEnvelope{Config: &common.Config{}},
			channel:             "",
			expectedErrContains: "channel group",
		},
		{
			name:                "nil config",
			configEnvelope:      &common.ConfigEnvelope{Config: nil},
			channel:             "test-channel",
			expectedErrContains: "cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			verifierFunc, err := svc.SigVerifierFromConfig(tc.configEnvelope, tc.channel, 0)
			assert.NotNil(t, verifierFunc)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)

			// Verify the returned function also returns an error
			block := &common.Block{Header: &common.BlockHeader{Number: 1}}
			funcErr := verifierFunc(block, false)
			assert.Error(t, funcErr)
			assert.Contains(t, funcErr.Error(), "failed to initialize sig verifier function")
		})
	}
}

func TestBlockSigVerifier_Verify(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &synchronizer.BlockSigVerifier{
		Logger:         logger,
		ConfigBlockNum: 0,
		Consenters:     []*common.Consenter{},
	}

	testCases := []struct {
		name                string
		block               *common.Block
		verifyData          bool
		expectedErrContains string
	}{
		{
			name:                "nil block",
			block:               nil,
			verifyData:          false,
			expectedErrContains: "consenter block is nil",
		},
		{
			name: "nil metadata",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Metadata: nil,
			},
			verifyData:          false,
			expectedErrContains: "consenter block metadata is nil",
		},
		{
			name: "empty metadata",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Metadata: &common.BlockMetadata{Metadata: [][]byte{}},
			},
			verifyData:          false,
			expectedErrContains: "consenter block metadata len is less than expected",
		},
		{
			name: "nil block header",
			block: &common.Block{
				Header:   nil,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{}},
			},
			verifyData:          false,
			expectedErrContains: "consenter block header is nil",
		},
		{
			name: "nil block data",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Data:     nil,
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			verifyData:          true,
			expectedErrContains: "consenter block data is nil or empty",
		},
		{
			name: "empty block data",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Data:     &common.BlockData{Data: [][]byte{}},
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			verifyData:          true,
			expectedErrContains: "consenter block data is nil or empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifier.Verify(tc.block, tc.verifyData)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)
		})
	}
}

func TestBlockSigVerifier_VerifyWithMockedPolicy_InvalidSignatureInputs(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	// Create consenters with identities
	consenters := []*common.Consenter{
		{
			Id:       1,
			MspId:    "OrdererMSP",
			Identity: []byte("identity1"),
		},
	}

	// Helper function to create a valid base block
	createValidBaseBlock := func() *common.Block {
		proposal := smartbft_types.Proposal{
			Header:               []byte{1, 2, 3},
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		blockNumber := uint64(10)
		configBlockNum := uint64(0)
		prevHash := []byte{0, 1, 2, 3}
		return state.CreateBlockToAppendFromDecision(blockNumber, proposal, []smartbft_types.Signature{}, prevHash, configBlockNum)
	}

	t.Run("invalid signatures bytes in metadata", func(t *testing.T) {
		block := createValidBaseBlock()
		// Set invalid signature bytes that cannot be deserialized
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = []byte{1, 2, 3, 4, 5}

		mockPolicy := &mocks.FakePolicy{}
		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err := verifier.Verify(block, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to deserialize signatures from metadata")
		assert.Equal(t, 0, mockPolicy.EvaluateSignedDataCallCount())
	})

	t.Run("signature with invalid value bytes", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a signature with invalid value bytes (not ASN.1 encoded array)
		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: []byte{1, 2, 3}, // Invalid - not ASN.1 encoded array
				Msg:   []byte{4, 5, 6},
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err := verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature with invalid msg bytes", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a signature with invalid msg bytes (not ASN.1 encoded array)
		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: valuesBytes,
				Msg:   []byte{4, 5, 6}, // Invalid - not ASN.1 encoded array
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature with mismatched values and msgs length", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a signature with different number of values and msgs
		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{{1, 2, 3}})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: valuesBytes, // 2 values
				Msg:   msgsBytes,   // 1 msg
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature with no messages", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a signature with empty messages
		valuesBytes, err := asn1.Marshal([][]byte{})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: valuesBytes,
				Msg:   msgsBytes,
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature with invalid proposal message", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a signature with invalid proposal message (not ASN.1 MessageToSign)
		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{{1, 2, 3}}) // Invalid MessageToSign
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: valuesBytes,
				Msg:   msgsBytes,
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature with invalid identifier header", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a message with invalid identifier header
		msgToSign := &protoutil.MessageToSign{
			IdentifierHeader:     []byte{1, 2, 3}, // Invalid identifier header
			BlockHeader:          protoutil.BlockHeaderBytes(block.GetHeader()),
			OrdererBlockMetadata: []byte{7, 8, 9},
		}
		marshaledMsg := msgToSign.ASN1MarshalOrPanic()

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{marshaledMsg})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: valuesBytes,
				Msg:   msgsBytes,
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature ID mismatch with identifier header", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create identifier header with different ID than signature
		idHeader := &common.IdentifierHeader{Identifier: 2} // Different from sig.ID
		idHeaderBytes := protoutil.MarshalOrPanic(idHeader)

		msgToSign := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(block.GetHeader()),
			OrdererBlockMetadata: []byte{7, 8, 9},
		}
		marshaledMsg := msgToSign.ASN1MarshalOrPanic()

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{marshaledMsg})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1, // Different from identifier header
				Value: valuesBytes,
				Msg:   msgsBytes,
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature with unknown consenter identity", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create identifier header with ID not in consenters list
		idHeader := &common.IdentifierHeader{Identifier: 99} // Unknown ID
		idHeaderBytes := protoutil.MarshalOrPanic(idHeader)

		msgToSign := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(block.GetHeader()),
			OrdererBlockMetadata: []byte{7, 8, 9},
		}
		marshaledMsg := msgToSign.ASN1MarshalOrPanic()

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{marshaledMsg})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    99, // Unknown consenter
				Value: valuesBytes,
				Msg:   msgsBytes,
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})

	t.Run("signature message does not match computed message", func(t *testing.T) {
		block := createValidBaseBlock()

		// Create a valid identifier header
		idHeader := &common.IdentifierHeader{Identifier: 1}
		idHeaderBytes := protoutil.MarshalOrPanic(idHeader)

		// Create a message with a different block header
		msgToSign := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          []byte{99, 99, 99}, // different block header
			OrdererBlockMetadata: []byte{7, 8, 9},
		}
		marshaledMsg := msgToSign.ASN1MarshalOrPanic()

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{marshaledMsg})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: valuesBytes,
				Msg:   msgsBytes,
			},
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(block, false)
		// The signature is skipped with a warning, so policy is called with empty set
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 0) // Empty because signature was invalid
	})
}

func TestBlockSigVerifier_VerifyWithMockedPolicy(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	// Create consenters with identities
	consenters := []*common.Consenter{
		{
			Id:       1,
			MspId:    "OrdererMSP",
			Identity: []byte("identity1"),
		},
		{
			Id:       2,
			MspId:    "OrdererMSP",
			Identity: []byte("identity2"),
		},
	}

	// Create a valid proposal
	proposal := smartbft_types.Proposal{
		Header:               []byte{1, 2, 3},
		Payload:              []byte{4, 5, 6},
		Metadata:             []byte{7, 8, 9},
		VerificationSequence: 0,
	}

	// Create identifier header for signer 1
	idHeader1 := &common.IdentifierHeader{Identifier: 1}
	idHeaderBytes1 := protoutil.MarshalOrPanic(idHeader1)

	// Create identifier header for signer 2
	idHeader2 := &common.IdentifierHeader{Identifier: 2}
	idHeaderBytes2 := protoutil.MarshalOrPanic(idHeader2)

	// Build a valid block
	blockNumber := uint64(10)
	configBlockNum := uint64(0)
	prevHash := []byte{0, 1, 2, 3}

	// Create the block using the helper function
	block := state.CreateBlockToAppendFromDecision(blockNumber, proposal, []smartbft_types.Signature{}, prevHash, configBlockNum)

	// Now we need to add proper signatures to the block
	// Create the message to sign for the proposal
	blockHeaderBytes := protoutil.BlockHeaderBytes(block.GetHeader())
	proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
		LastConfig:        &common.LastConfig{Index: configBlockNum},
		ConsenterMetadata: proposal.Metadata,
	})

	// Create message to sign for signer 1
	msgToSign1 := &protoutil.MessageToSign{
		IdentifierHeader:     idHeaderBytes1,
		BlockHeader:          blockHeaderBytes,
		OrdererBlockMetadata: proposalOrdererBlockMetadata,
	}
	marshaledMsg1 := msgToSign1.ASN1MarshalOrPanic()

	// Create message to sign for signer 2
	msgToSign2 := &protoutil.MessageToSign{
		IdentifierHeader:     idHeaderBytes2,
		BlockHeader:          blockHeaderBytes,
		OrdererBlockMetadata: proposalOrdererBlockMetadata,
	}
	marshaledMsg2 := msgToSign2.ASN1MarshalOrPanic()

	// Create signatures with proper structure
	// Each signature has a single message (the proposal) and a single value
	sig1Value := []byte{10, 11, 12}
	sig2Value := []byte{13, 14, 15}

	// Marshal the values and messages as ASN.1 arrays
	valuesBytes1, err := asn1.Marshal([][]byte{sig1Value})
	require.NoError(t, err)
	msgsBytes1, err := asn1.Marshal([][]byte{marshaledMsg1})
	require.NoError(t, err)

	valuesBytes2, err := asn1.Marshal([][]byte{sig2Value})
	require.NoError(t, err)
	msgsBytes2, err := asn1.Marshal([][]byte{marshaledMsg2})
	require.NoError(t, err)

	signatures := []smartbft_types.Signature{
		{
			ID:    1,
			Value: valuesBytes1,
			Msg:   msgsBytes1,
		},
		{
			ID:    2,
			Value: valuesBytes2,
			Msg:   msgsBytes2,
		},
	}

	// Update the block metadata with the signatures
	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

	t.Run("policy returns nil - verification succeeds", func(t *testing.T) {
		// Create a mock policy that returns nil (success)
		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: configBlockNum,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		// Call Verify with verifyData=false
		err := verifier.Verify(block, false)

		// Verify the result
		assert.NoError(t, err)
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())

		// Verify the signed data passed to the policy
		signedData := mockPolicy.EvaluateSignedDataArgsForCall(0)
		assert.Len(t, signedData, 2) // Two signatures
	})

	t.Run("policy returns error - verification fails", func(t *testing.T) {
		// Create a mock policy that returns an error
		mockPolicy := &mocks.FakePolicy{}
		policyErr := errors.New("policy evaluation failed")
		mockPolicy.EvaluateSignedDataReturns(policyErr)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: configBlockNum,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		// Call Verify with verifyData=false
		err := verifier.Verify(block, false)

		// Verify the result
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to evaluate signature set of the first (proposal) signature")
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
	})
}
