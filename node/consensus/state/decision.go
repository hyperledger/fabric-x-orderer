/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"encoding/asn1"
	"fmt"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
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

// ConsenterBlockToDecision deserializes a consenter block (common.Block) into a decision (proposal and signatures).
// The proposal is extracted from the block's Data field, and signatures are extracted from the Metadata field.
// It performs basic validation checks to ensure the block structure is valid.
func ConsenterBlockToDecision(block *common.Block) (*smartbft_types.Decision, error) {
	if block == nil {
		return nil, errors.Errorf("block is nil")
	}

	if block.Header == nil {
		return nil, errors.Errorf("block header is nil")
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.Errorf("data is empty for block: %d", block.Header.Number)
	}

	if block.Metadata == nil || len(block.Metadata.Metadata) == 0 {
		return nil, errors.Errorf("metadata is empty for block: %d", block.Header.Number)
	}

	if int(common.BlockMetadataIndex_SIGNATURES) >= len(block.Metadata.Metadata) {
		return nil, errors.Errorf("block metadata index %d is out of range (length: %d) for block: %d",
			common.BlockMetadataIndex_SIGNATURES, len(block.Metadata.Metadata), block.Header.Number)
	}

	// Extract proposal from block data
	proposalBytes := block.Data.Data[0]
	proposal, err := BytesToProposal(proposalBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize proposal for block: %d; err: %s", block.Header.Number, err)
	}

	// Extract signatures from block metadata
	signaturesBytes := block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES]
	signatures, err := BytesToDecisionSignatures(signaturesBytes)
	if err != nil {
		return nil, errors.Errorf("failed to deserialize signatures for block: %d; err: %s", block.Header.Number, err)
	}

	return &smartbft_types.Decision{Proposal: proposal, Signatures: signatures}, nil
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
