/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"encoding/asn1"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
)

type asn1Signature struct {
	ID    int64
	Value []byte
	Msg   []byte
}

type asn1Proposal struct {
	Payload              []byte
	Header               []byte
	Metadata             []byte
	VerificationSequence int64 // int64 for asn1 marshaling
}

// ConsenterBlockToProposal deserializes a consenter block (common.Block) into a proposal.
// The proposal is extracted from the block's Data field.
func ConsenterBlockToProposal(block *common.Block) (*smartbft_types.Proposal, error) {
	if block == nil {
		return nil, errors.Errorf("block is nil")
	}

	if block.Header == nil {
		return nil, errors.Errorf("block header is nil")
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.Errorf("data is empty for block: %d", block.Header.Number)
	}

	// Extract proposal from block data
	proposalBytes := block.Data.Data[0]
	proposal, err := BytesToProposal(proposalBytes)
	return &proposal, err
}

// ConsenterBlockToDecision deserializes a consenter block (common.Block) into a decision (proposal and signatures).
// The proposal is extracted from the block's Data field, and signatures are extracted from the Metadata field.
// It performs basic validation checks to ensure the block structure is valid.
func ConsenterBlockToDecision(block *common.Block) (*smartbft_types.Decision, error) {
	proposal, err := ConsenterBlockToProposal(block)
	if err != nil {
		return nil, errors.Wrap(err, "failed to deserialize proposal for block")
	}

	if block.Metadata == nil || len(block.Metadata.Metadata) == 0 {
		return nil, errors.Errorf("metadata is empty for block: %d", block.Header.Number)
	}

	if int(common.BlockMetadataIndex_SIGNATURES) >= len(block.Metadata.Metadata) {
		return nil, errors.Errorf("block metadata index %d is out of range (length: %d) for block: %d",
			common.BlockMetadataIndex_SIGNATURES, len(block.Metadata.Metadata), block.Header.Number)
	}

	// Extract signatures from block metadata
	signaturesBytes := block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES]
	if len(signaturesBytes) == 0 {
		return nil, errors.Errorf("signatures metadata is empty for block: %d", block.Header.Number)
	}
	signatures, err := BytesToDecisionSignatures(signaturesBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to deserialize signatures for block: %d", block.Header.Number)
	}

	return &smartbft_types.Decision{Proposal: *proposal, Signatures: signatures}, nil
}

func ProposalToBytes(proposal smartbft_types.Proposal) []byte {
	rawBytes, err := asn1.Marshal(asn1Proposal{
		VerificationSequence: proposal.VerificationSequence,
		Metadata:             proposal.Metadata,
		Payload:              proposal.Payload,
		Header:               proposal.Header,
	})
	if err != nil {
		panic(err)
	}
	return rawBytes
}

func BytesToProposal(rawBytes []byte) (smartbft_types.Proposal, error) {
	prop := &asn1Proposal{}
	if _, err := asn1.Unmarshal(rawBytes, prop); err != nil {
		return smartbft_types.Proposal{}, err
	}
	return smartbft_types.Proposal{
		Header:               prop.Header,
		Payload:              prop.Payload,
		Metadata:             prop.Metadata,
		VerificationSequence: prop.VerificationSequence,
	}, nil
}

func DecisionSignaturesToBytes(signatures []smartbft_types.Signature) []byte {
	rawSigs := make([][]byte, 0, len(signatures))
	for _, sig := range signatures {
		rawSig, err := asn1.Marshal(asn1Signature{Msg: sig.Msg, Value: sig.Value, ID: int64(sig.ID)})
		if err != nil {
			panic(err)
		}
		rawSigs = append(rawSigs, rawSig)
	}

	bytes, err := asn1.Marshal(rawSigs)
	if err != nil {
		panic(err)
	}

	return bytes
}

func BytesToDecisionSignatures(rawBytes []byte) ([]smartbft_types.Signature, error) {
	rawSigs := [][]byte{}
	if _, err := asn1.Unmarshal(rawBytes, &rawSigs); err != nil {
		return nil, err
	}

	var sigs []smartbft_types.Signature
	for _, rawSig := range rawSigs {
		sig := asn1Signature{}
		if _, err := asn1.Unmarshal(rawSig, &sig); err != nil {
			return nil, err
		}
		sigs = append(sigs, smartbft_types.Signature{
			Msg:   sig.Msg,
			Value: sig.Value,
			ID:    uint64(sig.ID),
		})
	}

	return sigs, nil
}

func CreateBlockToAppendFromDecision(number uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, prevHash []byte, decisionNumOfLastConfigBlock uint64) *common.Block {
	proposalBytes := ProposalToBytes(proposal)
	data := &common.BlockData{
		Data: [][]byte{proposalBytes},
	}

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       number,
			DataHash:     protoutil.ComputeBlockDataHash(data),
			PreviousHash: prevHash,
		},
		Data: data,
	}

	protoutil.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = DecisionSignaturesToBytes(signatures)
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
		LastConfig:        &common.LastConfig{Index: decisionNumOfLastConfigBlock},
		ConsenterMetadata: proposal.Metadata,
	})

	return block
}
