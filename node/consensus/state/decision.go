/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"encoding/asn1"
	"encoding/binary"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

type asn1Signature struct {
	ID    int64
	Value []byte
	Msg   []byte
}

func DecisionToBytes(proposal smartbft_types.Proposal, signatures []smartbft_types.Signature) []byte {
	sigBuff := DecisionSignaturesToBytes(signatures)

	buff := make([]byte, 4*3+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata)+len(sigBuff))
	binary.BigEndian.PutUint32(buff, uint32(len(proposal.Header)))
	binary.BigEndian.PutUint32(buff[4:], uint32(len(proposal.Payload)))
	binary.BigEndian.PutUint32(buff[8:], uint32(len(proposal.Metadata)))
	copy(buff[12:], proposal.Header)
	copy(buff[12+len(proposal.Header):], proposal.Payload)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload):], proposal.Metadata)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata):], sigBuff)

	return buff
}

func BytesToDecision(rawBytes []byte) (smartbft_types.Proposal, []smartbft_types.Signature, error) {
	buff := bytes.NewBuffer(rawBytes)
	headerSize := make([]byte, 4)
	if _, err := buff.Read(headerSize); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	payloadSize := make([]byte, 4)
	if _, err := buff.Read(payloadSize); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	metadataSize := make([]byte, 4)
	if _, err := buff.Read(metadataSize); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	header := make([]byte, binary.BigEndian.Uint32(headerSize))
	if _, err := buff.Read(header); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	payload := make([]byte, binary.BigEndian.Uint32(payloadSize))
	if _, err := buff.Read(payload); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	metadata := make([]byte, binary.BigEndian.Uint32(metadataSize))
	if _, err := buff.Read(metadata); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	proposalSize := 4*3 + len(header) + len(payload) + len(metadata)

	signatureBuff := make([]byte, len(rawBytes)-proposalSize)

	if _, err := buff.Read(signatureBuff); err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	sigs, err := BytesToDecisionSignatures(signatureBuff)
	if err != nil {
		return smartbft_types.Proposal{}, nil, err
	}

	return smartbft_types.Proposal{
		Header:   header,
		Payload:  payload,
		Metadata: metadata,
	}, sigs, nil
}

func DecisionSignaturesToBytes(signatures []smartbft_types.Signature) []byte { // TODO unit test
	sigBuff := bytes.Buffer{}

	for _, sig := range signatures {
		rawSig, err := asn1.Marshal(asn1Signature{Msg: sig.Msg, Value: sig.Value, ID: int64(sig.ID)})
		if err != nil {
			panic(err)
		}
		rawSigSize := make([]byte, 2)
		binary.BigEndian.PutUint16(rawSigSize, uint16(len(rawSig))) // TODO the sig Msg is not limited in size... so uint16 might not be enough
		sigBuff.Write(rawSigSize)
		sigBuff.Write(rawSig)
	}

	buff := make([]byte, sigBuff.Len()) // TODO maybe we can just return sigBuff.Bytes()
	copy(buff, sigBuff.Bytes())
	return buff
}

func BytesToDecisionSignatures(rawBytes []byte) ([]smartbft_types.Signature, error) {
	var sigs []smartbft_types.Signature

	var pos int
	for pos < len(rawBytes) {
		sigSize := int(binary.BigEndian.Uint16([]byte{rawBytes[pos], rawBytes[pos+1]}))
		pos += 2
		sig := asn1Signature{}
		if _, err := asn1.Unmarshal(rawBytes[pos:pos+sigSize], &sig); err != nil {
			return nil, err
		}
		pos += sigSize
		sigs = append(sigs, smartbft_types.Signature{
			Msg:   sig.Msg,
			Value: sig.Value,
			ID:    uint64(sig.ID),
		})
	}

	return sigs, nil
}
