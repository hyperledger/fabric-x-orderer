/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"encoding/asn1"
	"fmt"
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
			require.NotNil(t, verifierFunc)
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
		{
			// Block number > 0 forces GetLastConfigIndexFromConsenterBlock to read
			// the LAST_CONFIG slot. Garbage bytes there cause it to fail, which is
			// returned as "failed to get last config index from consenter block".
			// CreateBlockToAppendFromDecision initialises SIGNATURES correctly so
			// BytesToDecisionSignatures doesn't fail first; then we replace LAST_CONFIG.
			name: "corrupted LAST_CONFIG metadata",
			block: func() *common.Block {
				b := state.CreateBlockToAppendFromDecision(
					5,
					smartbft_types.Proposal{Header: []byte{1, 2, 3}, Payload: []byte{4, 5, 6}, Metadata: []byte{7, 8, 9}},
					[]smartbft_types.Signature{},
					[]byte{0, 1, 2},
					0,
				)
				// Overwrite LAST_CONFIG with bytes that cannot be proto-unmarshaled.
				b.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = []byte{0xFF, 0xFE, 0xFD}
				return b
			}(),
			verifyData:          false,
			expectedErrContains: "failed to get last config index from consenter block",
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

// TestBlockSigVerifier_VerifyWithMockedPolicy_InvalidSignatureInputs runs the same invalid-input
// table against both verifyData=false and verifyData=true. For verifyData=true the block carries
// a proper proposal header (no AvailableCommonBlocks) so the verifyData pre-checks pass and the
// same per-signature error / skip behaviour is exercised.
func TestBlockSigVerifier_VerifyWithMockedPolicy_InvalidSignatureInputs(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	consenters := []*common.Consenter{
		{Id: 1, MspId: "OrdererMSP", Identity: []byte("identity1")},
	}

	// buildMsg marshals a MessageToSign into a single-element ASN.1 value+msg pair.
	buildMsg := func(msg *protoutil.MessageToSign) (valuesBytes, msgsBytes []byte) {
		var err error
		valuesBytes, err = asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err = asn1.Marshal([][]byte{msg.ASN1MarshalOrPanic()})
		require.NoError(t, err)
		return
	}

	testCases := []struct {
		name string
		// buildSigs constructs the raw signatures slice to inject into the block.
		buildSigs func(block *common.Block) []smartbft_types.Signature
		// wantErr: when true the call must return an error containing wantErrContains
		// and policy must not be called. When false the sig is skipped with a
		// warning, policy is called once with an empty signed-data set.
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "invalid signatures bytes in metadata",
			buildSigs: func(_ *common.Block) []smartbft_types.Signature {
				return nil // caller injects raw bytes directly
			},
			wantErr:         true,
			wantErrContains: "failed to deserialize signatures from metadata",
		},
		{
			name: "invalid value bytes",
			buildSigs: func(_ *common.Block) []smartbft_types.Signature {
				return []smartbft_types.Signature{{ID: 1, Value: []byte{1, 2, 3}, Msg: []byte{4, 5, 6}}}
			},
		},
		{
			name: "invalid msg bytes",
			buildSigs: func(_ *common.Block) []smartbft_types.Signature {
				valuesBytes, _ := asn1.Marshal([][]byte{{10, 11, 12}})
				return []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: []byte{4, 5, 6}}}
			},
		},
		{
			name: "mismatched values and msgs length",
			buildSigs: func(_ *common.Block) []smartbft_types.Signature {
				valuesBytes, _ := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}}) // 2 values
				msgsBytes, _ := asn1.Marshal([][]byte{{1, 2, 3}})                    // 1 msg
				return []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
			},
		},
		{
			name: "no messages",
			buildSigs: func(_ *common.Block) []smartbft_types.Signature {
				valuesBytes, _ := asn1.Marshal([][]byte{})
				msgsBytes, _ := asn1.Marshal([][]byte{})
				return []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
			},
		},
		{
			name: "invalid proposal message bytes",
			buildSigs: func(_ *common.Block) []smartbft_types.Signature {
				valuesBytes, _ := asn1.Marshal([][]byte{{10, 11, 12}})
				msgsBytes, _ := asn1.Marshal([][]byte{{1, 2, 3}}) // raw bytes, not a MessageToSign
				return []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
			},
		},
		{
			name: "invalid identifier header in proposal message",
			buildSigs: func(block *common.Block) []smartbft_types.Signature {
				v, m := buildMsg(&protoutil.MessageToSign{
					IdentifierHeader:     []byte{1, 2, 3}, // not a valid proto
					BlockHeader:          protoutil.BlockHeaderBytes(block.GetHeader()),
					OrdererBlockMetadata: []byte{7, 8, 9},
				})
				return []smartbft_types.Signature{{ID: 1, Value: v, Msg: m}}
			},
		},
		{
			name: "signature ID mismatch with identifier header",
			buildSigs: func(block *common.Block) []smartbft_types.Signature {
				v, m := buildMsg(&protoutil.MessageToSign{
					IdentifierHeader:     protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 2}),
					BlockHeader:          protoutil.BlockHeaderBytes(block.GetHeader()),
					OrdererBlockMetadata: []byte{7, 8, 9},
				})
				return []smartbft_types.Signature{{ID: 1, Value: v, Msg: m}} // sig.ID=1 ≠ header.ID=2
			},
		},
		{
			name: "unknown consenter identity",
			buildSigs: func(block *common.Block) []smartbft_types.Signature {
				v, m := buildMsg(&protoutil.MessageToSign{
					IdentifierHeader:     protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 99}),
					BlockHeader:          protoutil.BlockHeaderBytes(block.GetHeader()),
					OrdererBlockMetadata: []byte{7, 8, 9},
				})
				return []smartbft_types.Signature{{ID: 99, Value: v, Msg: m}}
			},
		},
		{
			name: "proposal message does not match computed message",
			buildSigs: func(block *common.Block) []smartbft_types.Signature {
				v, m := buildMsg(&protoutil.MessageToSign{
					IdentifierHeader:     protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1}),
					BlockHeader:          []byte{99, 99, 99}, // wrong header bytes
					OrdererBlockMetadata: []byte{7, 8, 9},
				})
				return []smartbft_types.Signature{{ID: 1, Value: v, Msg: m}}
			},
		},
	}

	// createBlock returns a block appropriate for each verifyData mode.
	// verifyData=false: raw proposal bytes are fine; verifyData=true needs a
	// deserializable proposal header with no AvailableCommonBlocks so the
	// pre-checks pass and the per-signature loop is reached.
	createBlock := func(verifyData bool) *common.Block {
		var proposal smartbft_types.Proposal
		if verifyData {
			hdr := &state.Header{Num: 10, AvailableCommonBlocks: nil}
			proposal = smartbft_types.Proposal{
				Header:               hdr.Serialize(),
				Payload:              []byte{4, 5, 6},
				Metadata:             []byte{7, 8, 9},
				VerificationSequence: 0,
			}
		} else {
			proposal = smartbft_types.Proposal{
				Header:   []byte{1, 2, 3},
				Payload:  []byte{4, 5, 6},
				Metadata: []byte{7, 8, 9},
			}
		}
		return state.CreateBlockToAppendFromDecision(10, proposal, []smartbft_types.Signature{}, []byte{0, 1, 2, 3}, 0)
	}

	for _, verifyData := range []bool{false, true} {
		verifyData := verifyData // capture
		t.Run(fmt.Sprintf("verifyData=%v", verifyData), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					block := createBlock(verifyData)

					sigs := tc.buildSigs(block)
					if sigs == nil {
						// "invalid signatures bytes" case: inject raw undeserializable bytes.
						block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = []byte{1, 2, 3, 4, 5}
					} else {
						block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(sigs)
					}

					mockPolicy := &mocks.FakePolicy{}
					mockPolicy.EvaluateSignedDataReturns(nil)

					verifier := &synchronizer.BlockSigVerifier{
						Logger:         logger,
						ConfigBlockNum: 0,
						Consenters:     consenters,
						Policy:         mockPolicy,
					}

					err := verifier.Verify(block, verifyData)
					if tc.wantErr {
						assert.Error(t, err)
						assert.Contains(t, err.Error(), tc.wantErrContains)
						assert.Equal(t, 0, mockPolicy.EvaluateSignedDataCallCount())
					} else {
						// Signature skipped with a warning; policy called once with an
						// empty signed-data set (no AvailableCommonBlocks).
						assert.NoError(t, err)
						assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
						assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0)
					}
				})
			}
		})
	}
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

// TestBlockSigVerifier_VerifyWithData_EarlyErrors covers verifyData=true error paths that occur
// before the per-signature loop: invalid proposal bytes, bad proposal header deserialization,
// available common blocks with nil header, nil metadata, and missing ORDERER metadata entry.
func TestBlockSigVerifier_VerifyWithData_EarlyErrors(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	consenters := []*common.Consenter{
		{
			Id:       1,
			MspId:    "OrdererMSP",
			Identity: []byte("identity1"),
		},
	}

	makeVerifier := func() *synchronizer.BlockSigVerifier {
		return &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         &mocks.FakePolicy{},
		}
	}

	t.Run("invalid proposal bytes in block data", func(t *testing.T) {
		// Use CreateBlockToAppendFromDecision so that the metadata (including the
		// SIGNATURES slot) is properly initialized; then overwrite Data.Data[0] with
		// bytes that are not a valid ASN.1 proposal.
		baseProposal := smartbft_types.Proposal{
			Header:               []byte{1, 2, 3},
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		block := state.CreateBlockToAppendFromDecision(1, baseProposal, []smartbft_types.Signature{}, []byte{0}, 0)
		// Replace the proposal bytes with invalid content.
		block.Data.Data[0] = []byte{0xFF, 0xFE, 0xFD}
		err := makeVerifier().Verify(block, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to deserialize proposal from block data")
	})

	t.Run("invalid proposal header deserialization", func(t *testing.T) {
		// Build a proposal whose Header field is not a valid serialized state.Header proto.
		proposal := smartbft_types.Proposal{
			Header:               []byte{0xFF, 0xFE}, // invalid proto bytes
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		block := state.CreateBlockToAppendFromDecision(1, proposal, []smartbft_types.Signature{}, []byte{0}, 0)
		err := makeVerifier().Verify(block, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed deserializing proposal header")
	})

	t.Run("available common block header is nil", func(t *testing.T) {
		hdr := &state.Header{
			Num:                   1,
			AvailableCommonBlocks: []*common.Block{{Header: nil, Metadata: &common.BlockMetadata{}}},
		}
		proposal := smartbft_types.Proposal{
			Header:               hdr.Serialize(),
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		block := state.CreateBlockToAppendFromDecision(1, proposal, []smartbft_types.Signature{}, []byte{0}, 0)
		err := makeVerifier().Verify(block, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "available common block header is nil at index 0")
	})

	t.Run("available common block metadata is nil", func(t *testing.T) {
		commonBlock := &common.Block{
			Header:   &common.BlockHeader{Number: 5},
			Metadata: nil,
		}
		hdr := &state.Header{
			Num:                   1,
			AvailableCommonBlocks: []*common.Block{commonBlock},
		}
		proposal := smartbft_types.Proposal{
			Header:               hdr.Serialize(),
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		block := state.CreateBlockToAppendFromDecision(1, proposal, []smartbft_types.Signature{}, []byte{0}, 0)
		err := makeVerifier().Verify(block, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "available common block metadata is nil at index 0")
	})

	t.Run("available common block metadata missing ORDERER entry", func(t *testing.T) {
		// Metadata present but shorter than BlockMetadataIndex_ORDERER requires.
		commonBlock := &common.Block{
			Header:   &common.BlockHeader{Number: 5},
			Metadata: &common.BlockMetadata{Metadata: [][]byte{}}, // empty, missing ORDERER
		}
		hdr := &state.Header{
			Num:                   1,
			AvailableCommonBlocks: []*common.Block{commonBlock},
		}
		proposal := smartbft_types.Proposal{
			Header:               hdr.Serialize(),
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		block := state.CreateBlockToAppendFromDecision(1, proposal, []smartbft_types.Signature{}, []byte{0}, 0)
		err := makeVerifier().Verify(block, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "available common block metadata is missing ORDERER entry at index 0")
	})
}

// TestBlockSigVerifier_VerifyWithData_InvalidSignatureInputs covers verifyData=true paths
// inside the per-signature loop that are only active when verifyData is true.
func TestBlockSigVerifier_VerifyWithData_InvalidSignatureInputs(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	consenters := []*common.Consenter{
		{
			Id:       1,
			MspId:    "OrdererMSP",
			Identity: []byte("identity1"),
		},
	}

	// createValidCommonBlock returns a fully-initialized normal common block.
	createValidCommonBlock := func(blockNum uint64) *common.Block {
		block := &common.Block{
			Header:   &common.BlockHeader{Number: blockNum},
			Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
		}
		return block
	}

	// buildConsenterBlock builds a consenter block whose proposal embeds the given
	// availableCommonBlocks; it returns both the block and the pre-computed fields
	// needed to construct valid per-block signatures.
	buildConsenterBlock := func(availableCommonBlocks []*common.Block) *common.Block {
		hdr := &state.Header{
			Num:                   10,
			AvailableCommonBlocks: availableCommonBlocks,
		}
		proposal := smartbft_types.Proposal{
			Header:               hdr.Serialize(),
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		return state.CreateBlockToAppendFromDecision(10, proposal, []smartbft_types.Signature{}, []byte{0, 1, 2}, 0)
	}

	// buildValidSignature creates a structurally correct signature for one signer
	// covering the consenter block and all available common blocks.
	buildValidSignature := func(
		signerID uint32,
		consenterBlock *common.Block,
		availableCommonBlocks []*common.Block,
		configBlockNum uint64,
	) smartbft_types.Signature {
		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: signerID})

		// Proposal message
		lastConfigIndex, _ := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}

		allMsgs := [][]byte{proposalMsg.ASN1MarshalOrPanic()}
		allValues := [][]byte{{10, 20, 30}}

		// Per-available-block messages
		lastCfgNum := configBlockNum
		for _, ab := range availableCommonBlocks {
			if protoutil.IsConfigBlock(ab) {
				lastCfgNum = ab.Header.Number
			}
			obm := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
				LastConfig:        &common.LastConfig{Index: lastCfgNum},
				ConsenterMetadata: ab.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
			})
			msg := &protoutil.MessageToSign{
				IdentifierHeader:     idHeaderBytes,
				BlockHeader:          protoutil.BlockHeaderBytes(ab.GetHeader()),
				OrdererBlockMetadata: obm,
			}
			allMsgs = append(allMsgs, msg.ASN1MarshalOrPanic())
			allValues = append(allValues, []byte{11, 22, 33})
		}

		valuesBytes, err := asn1.Marshal(allValues)
		if err != nil {
			panic(err)
		}
		msgsBytes, err := asn1.Marshal(allMsgs)
		if err != nil {
			panic(err)
		}
		return smartbft_types.Signature{ID: uint64(signerID), Value: valuesBytes, Msg: msgsBytes}
	}

	t.Run("signature msg count does not match available common blocks count", func(t *testing.T) {
		// One available common block means each sig should carry 2 msgs (proposal + 1 block).
		// We provide only 1 msg (just the proposal) — the count check skips the sig.
		commonBlock := createValidCommonBlock(5)
		consenterBlock := buildConsenterBlock([]*common.Block{commonBlock})

		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: []byte{7, 8, 9},
		}
		// Only 1 message, but 2 are required (1 proposal + 1 common block).
		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic()})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(consenterBlock, true)
		// Signature skipped; policy called with empty set for the proposal block
		// and empty set for each common block.
		assert.NoError(t, err)
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount()) // proposal + 1 common block
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0)
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 0)
	})

	t.Run("per-block signature msg cannot be unmarshaled as MessageToSign", func(t *testing.T) {
		commonBlock := createValidCommonBlock(5)
		consenterBlock := buildConsenterBlock([]*common.Block{commonBlock})

		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		lastConfigIndex, _ := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}

		// Second message (for the common block) is invalid bytes — not a MessageToSign.
		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic(), {1, 2, 3}})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(consenterBlock, true)
		// Signature skipped; both signature sets are empty.
		assert.NoError(t, err)
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0)
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 0)
	})

	t.Run("per-block signature message does not match computed message", func(t *testing.T) {
		commonBlock := createValidCommonBlock(5)
		consenterBlock := buildConsenterBlock([]*common.Block{commonBlock})

		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		lastConfigIndex, _ := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}

		// Per-block message has wrong BlockHeader — will not match the computed value.
		wrongBlockMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          []byte{99, 99, 99}, // wrong
			OrdererBlockMetadata: []byte{7, 8, 9},
		}

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic(), wrongBlockMsg.ASN1MarshalOrPanic()})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(consenterBlock, true)
		// Signature skipped; both signature sets are empty.
		assert.NoError(t, err)
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0)
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 0)
	})

	t.Run("per-block identifier header ID mismatch", func(t *testing.T) {
		commonBlock := createValidCommonBlock(5)
		consenterBlock := buildConsenterBlock([]*common.Block{commonBlock})

		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		// Per-block message uses a different identifier (2) than the outer sig (1).
		wrongIDHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 2})
		lastConfigIndex, _ := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}
		wrongIDBlockMsg := &protoutil.MessageToSign{
			IdentifierHeader:     wrongIDHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(commonBlock.GetHeader()),
			OrdererBlockMetadata: []byte{7, 8, 9},
		}

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic(), wrongIDBlockMsg.ASN1MarshalOrPanic()})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(consenterBlock, true)
		// collectBlocksSignatures returns error, whole sig is skipped.
		assert.NoError(t, err)
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0)
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 0)
	})

	t.Run("per-block unknown consenter identity", func(t *testing.T) {
		commonBlock := createValidCommonBlock(5)
		consenterBlock := buildConsenterBlock([]*common.Block{commonBlock})

		// Outer sig has ID 1 (known); per-block msg has ID 99 (unknown).
		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		unknownIDHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 99})
		lastConfigIndex, _ := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}

		// obm for block 5 with the correct last-config
		obm := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: 0},
			ConsenterMetadata: commonBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		unknownIDBlockMsg := &protoutil.MessageToSign{
			IdentifierHeader:     unknownIDHeaderBytes, // unknown; searchConsenterIdentityByID returns nil
			BlockHeader:          protoutil.BlockHeaderBytes(commonBlock.GetHeader()),
			OrdererBlockMetadata: obm,
		}

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic(), unknownIDBlockMsg.ASN1MarshalOrPanic()})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(consenterBlock, true)
		// collectBlocksSignatures returns error; whole sig is skipped.
		assert.NoError(t, err)
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0)
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 0)
	})

	t.Run("valid signatures accepted with verifyData true - no available common blocks", func(t *testing.T) {
		// Proposal header with zero AvailableCommonBlocks; each sig must have exactly 1 msg.
		hdr := &state.Header{
			Num:                   10,
			AvailableCommonBlocks: nil,
		}
		proposal := smartbft_types.Proposal{
			Header:               hdr.Serialize(),
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		consenterBlock := state.CreateBlockToAppendFromDecision(10, proposal, []smartbft_types.Signature{}, []byte{0, 1, 2}, 0)

		sig := buildValidSignature(1, consenterBlock, nil, 0)
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes([]smartbft_types.Signature{sig})

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err := verifier.Verify(consenterBlock, true)
		assert.NoError(t, err)
		// Only the proposal signature set is evaluated (no common blocks).
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 1)
	})

	t.Run("per-block msg has invalid identifier header bytes", func(t *testing.T) {
		// Build a consenter block with one available common block.
		commonBlock := createValidCommonBlock(5)
		consenterBlock := buildConsenterBlock([]*common.Block{commonBlock})

		// Build a valid proposal msg so the sig is not skipped before
		// collectBlocksSignatures is reached.
		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		lastConfigIndex, _ := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		proposalObm := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalObm,
		}

		// Per-block MessageToSign whose IdentifierHeader is garbage proto bytes.
		// ASN1Unmarshal of the MessageToSign struct succeeds, but
		// protoutil.UnmarshalIdentifierHeader will fail on the field value.
		badIDBlockMsg := &protoutil.MessageToSign{
			IdentifierHeader:     []byte{0xFF, 0xFE, 0xFD},
			BlockHeader:          protoutil.BlockHeaderBytes(commonBlock.GetHeader()),
			OrdererBlockMetadata: []byte{7, 8, 9},
		}

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic(), badIDBlockMsg.ASN1MarshalOrPanic()})
		require.NoError(t, err)

		signatures := []smartbft_types.Signature{{ID: 1, Value: valuesBytes, Msg: msgsBytes}}
		consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: 0,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(consenterBlock, true)
		// collectBlocksSignatures returns an error; the whole signature is skipped.
		assert.NoError(t, err)
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 0) // proposal set empty
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 0) // common-block set empty
	})
}

// TestBlockSigVerifier_VerifyWithData_MockedPolicy mirrors TestBlockSigVerifier_VerifyWithMockedPolicy
// but calls Verify with verifyData=true and a block that carries AvailableCommonBlocks.
func TestBlockSigVerifier_VerifyWithData_MockedPolicy(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	consenters := []*common.Consenter{
		{Id: 1, MspId: "OrdererMSP", Identity: []byte("identity1")},
		{Id: 2, MspId: "OrdererMSP", Identity: []byte("identity2")},
	}

	configBlockNum := uint64(0)

	// Build one available common block (a normal block).
	commonBlock := &common.Block{
		Header:   &common.BlockHeader{Number: 5},
		Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
	}

	hdr := &state.Header{
		Num:                   10,
		AvailableCommonBlocks: []*common.Block{commonBlock},
	}
	proposal := smartbft_types.Proposal{
		Header:               hdr.Serialize(),
		Payload:              []byte{4, 5, 6},
		Metadata:             []byte{7, 8, 9},
		VerificationSequence: 0,
	}

	consenterBlock := state.CreateBlockToAppendFromDecision(10, proposal, []smartbft_types.Signature{}, []byte{0, 1, 2}, configBlockNum)

	// Helper: build a correctly-structured signature for a given signer.
	buildSig := func(signerID uint32) smartbft_types.Signature {
		idHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: signerID})

		lastConfigIndex, err := state.GetLastConfigIndexFromConsenterBlock(consenterBlock)
		if err != nil {
			panic(err)
		}
		proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: lastConfigIndex},
			ConsenterMetadata: consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		proposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(consenterBlock.GetHeader()),
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}

		commonBlockObm := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: configBlockNum},
			ConsenterMetadata: commonBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		commonBlockMsg := &protoutil.MessageToSign{
			IdentifierHeader:     idHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(commonBlock.GetHeader()),
			OrdererBlockMetadata: commonBlockObm,
		}

		valuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		if err != nil {
			panic(err)
		}
		msgsBytes, err := asn1.Marshal([][]byte{proposalMsg.ASN1MarshalOrPanic(), commonBlockMsg.ASN1MarshalOrPanic()})
		if err != nil {
			panic(err)
		}
		return smartbft_types.Signature{ID: uint64(signerID), Value: valuesBytes, Msg: msgsBytes}
	}

	sig1 := buildSig(1)
	sig2 := buildSig(2)
	consenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes([]smartbft_types.Signature{sig1, sig2})

	t.Run("policy returns nil - verification succeeds with verifyData true", func(t *testing.T) {
		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: configBlockNum,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err := verifier.Verify(consenterBlock, true)
		assert.NoError(t, err)
		// EvaluateSignedData is called once for the proposal and once per common block.
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 2) // two signers for proposal
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 2) // two signers for common block
	})

	t.Run("policy returns error for common block - verification fails with verifyData true", func(t *testing.T) {
		mockPolicy := &mocks.FakePolicy{}
		// First call (proposal block) succeeds; second call (common block) fails.
		mockPolicy.EvaluateSignedDataReturnsOnCall(0, nil)
		mockPolicy.EvaluateSignedDataReturnsOnCall(1, errors.New("policy evaluation failed for common block"))

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: configBlockNum,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err := verifier.Verify(consenterBlock, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to evaluate signature set in index 0 for block ID")
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
	})

	t.Run("policy returns error for proposal - verification fails with verifyData true", func(t *testing.T) {
		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(errors.New("policy evaluation failed"))

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: configBlockNum,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err := verifier.Verify(consenterBlock, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to evaluate signature set of the first (proposal) signature")
		assert.Equal(t, 1, mockPolicy.EvaluateSignedDataCallCount())
	})

	t.Run("config block in AvailableCommonBlocks updates lastConfigNum", func(t *testing.T) {
		// Build a Fabric config block (HeaderType_CONFIG makes IsConfigBlock return true).
		// When the verifier processes AvailableCommonBlocks it must update lastConfigNum
		// to the config block's number (sig_verifier.go:161-163).
		configPayload := &common.Payload{
			Header: &common.Header{
				ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
					Type:      int32(common.HeaderType_CONFIG),
					ChannelId: "testchannel",
				}),
			},
			Data: protoutil.MarshalOrPanic(&common.ConfigEnvelope{
				Config: &common.Config{ChannelGroup: &common.ConfigGroup{}},
			}),
		}
		configEnv := &common.Envelope{Payload: protoutil.MarshalOrPanic(configPayload)}
		cfgCommonBlock := &common.Block{
			Header: &common.BlockHeader{Number: 7},
			Data:   &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(configEnv)}},
			Metadata: &common.BlockMetadata{
				Metadata: make([][]byte, len(common.BlockMetadataIndex_name)),
			},
		}

		hdrCfg := &state.Header{
			Num:                   10,
			AvailableCommonBlocks: []*common.Block{cfgCommonBlock},
		}
		cfgProposal := smartbft_types.Proposal{
			Header:               hdrCfg.Serialize(),
			Payload:              []byte{4, 5, 6},
			Metadata:             []byte{7, 8, 9},
			VerificationSequence: 0,
		}
		cfgConsenterBlock := state.CreateBlockToAppendFromDecision(10, cfgProposal, []smartbft_types.Signature{}, []byte{0, 1, 2}, configBlockNum)

		// Build signature: lastConfigNum for the common block must be 7 (the config
		// block's own number), which is what the verifier computes via the IsConfigBlock
		// branch.
		cfgIDHeaderBytes := protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1})
		cfgLastConfigIndex, err := state.GetLastConfigIndexFromConsenterBlock(cfgConsenterBlock)
		require.NoError(t, err)
		cfgProposalObm := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: cfgLastConfigIndex},
			ConsenterMetadata: cfgConsenterBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		cfgProposalMsg := &protoutil.MessageToSign{
			IdentifierHeader:     cfgIDHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(cfgConsenterBlock.GetHeader()),
			OrdererBlockMetadata: cfgProposalObm,
		}
		// The config common block's OBM uses lastConfigNum = 7 (its own number).
		cfgBlockObm := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig:        &common.LastConfig{Index: cfgCommonBlock.Header.Number},
			ConsenterMetadata: cfgCommonBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER],
		})
		cfgBlockMsg := &protoutil.MessageToSign{
			IdentifierHeader:     cfgIDHeaderBytes,
			BlockHeader:          protoutil.BlockHeaderBytes(cfgCommonBlock.GetHeader()),
			OrdererBlockMetadata: cfgBlockObm,
		}

		cfgValuesBytes, err := asn1.Marshal([][]byte{{10, 11, 12}, {13, 14, 15}})
		require.NoError(t, err)
		cfgMsgsBytes, err := asn1.Marshal([][]byte{cfgProposalMsg.ASN1MarshalOrPanic(), cfgBlockMsg.ASN1MarshalOrPanic()})
		require.NoError(t, err)

		cfgSig := smartbft_types.Signature{ID: 1, Value: cfgValuesBytes, Msg: cfgMsgsBytes}
		cfgConsenterBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes([]smartbft_types.Signature{cfgSig})

		mockPolicy := &mocks.FakePolicy{}
		mockPolicy.EvaluateSignedDataReturns(nil)

		verifier := &synchronizer.BlockSigVerifier{
			Logger:         logger,
			ConfigBlockNum: configBlockNum,
			Consenters:     consenters,
			Policy:         mockPolicy,
		}

		err = verifier.Verify(cfgConsenterBlock, true)
		assert.NoError(t, err)
		// Policy called once for the proposal, once for the config common block.
		assert.Equal(t, 2, mockPolicy.EvaluateSignedDataCallCount())
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(0), 1)
		assert.Len(t, mockPolicy.EvaluateSignedDataArgsForCall(1), 1)
	})
}
