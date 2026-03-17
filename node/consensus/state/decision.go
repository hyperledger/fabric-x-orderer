/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"encoding/asn1"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
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

// TODO add a func to serialize and deserialize a consenter block into a proposal and signatures including basic checks

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
