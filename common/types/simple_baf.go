/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"encoding/asn1"
	"fmt"
	"math"
	"math/big"

	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"google.golang.org/protobuf/proto"
)

type SimpleBatchAttestationFragment struct {
	shard   ShardID
	primary PartyID
	seq     BatchSequence
	digest  []byte

	signer           PartyID
	signature        []byte
	primarySignature []byte

	configSequence ConfigSequence
	txCount        uint64
}

// NewSimpleBatchAttestationFragment creates a new, unsigned, SimpleBatchAttestationFragment.
func NewSimpleBatchAttestationFragment(shard ShardID, primary PartyID, seq BatchSequence, digest []byte, signer PartyID, configSqn ConfigSequence, txCount uint64, primarySignature []byte) *SimpleBatchAttestationFragment {
	return &SimpleBatchAttestationFragment{
		seq:              seq,
		primary:          primary,
		signer:           signer,
		shard:            shard,
		digest:           digest,
		configSequence:   configSqn,
		txCount:          txCount,
		primarySignature: primarySignature,
	}
}

func (s *SimpleBatchAttestationFragment) Seq() BatchSequence {
	return s.seq
}

func (s *SimpleBatchAttestationFragment) Primary() PartyID {
	return s.primary
}

func (s *SimpleBatchAttestationFragment) Signer() PartyID {
	return s.signer
}

func (s *SimpleBatchAttestationFragment) Shard() ShardID {
	return s.shard
}

func (s *SimpleBatchAttestationFragment) Digest() []byte {
	return s.digest
}

func (s *SimpleBatchAttestationFragment) ConfigSequence() ConfigSequence {
	return s.configSequence
}

func (s *SimpleBatchAttestationFragment) TXCount() uint64 {
	return s.txCount
}

func (s *SimpleBatchAttestationFragment) Signature() []byte {
	return s.signature
}

func (s *SimpleBatchAttestationFragment) SetSignature(sig []byte) {
	s.signature = sig
}

func (s *SimpleBatchAttestationFragment) PrimarySignature() []byte {
	return s.primarySignature
}

func (s *SimpleBatchAttestationFragment) String() string {
	return fmt.Sprintf("BAF: Signer: %d; %s; Config Seq: %d; TX Count: %d", s.signer, BatchIDToString(s), s.configSequence, s.txCount)
}

type asn1BAF struct {
	Shard          int
	Primary        int
	Seq            *big.Int
	Digest         []byte
	ConfigSequence *big.Int
	TXCount        *big.Int
	Signer         int
	Sig            []byte
	PrimarySig     []byte
}

// ToProto converts SimpleBatchAttestationFragment to protobuf message
func (s *SimpleBatchAttestationFragment) ToProto() *stateprotos.BatchAttestationFragment {
	return &stateprotos.BatchAttestationFragment{
		Shard:            uint32(s.shard),
		Primary:          uint32(s.primary),
		Seq:              uint64(s.seq),
		Digest:           s.digest,
		ConfigSequence:   uint64(s.configSequence),
		TxCount:          s.txCount,
		Signer:           uint32(s.signer),
		Signature:        s.signature,
		PrimarySignature: s.primarySignature,
	}
}

// FromProto populates SimpleBatchAttestationFragment from protobuf message
func (s *SimpleBatchAttestationFragment) FromProto(pb *stateprotos.BatchAttestationFragment) error {
	if pb.GetShard() > math.MaxUint16 {
		return fmt.Errorf("the BAF Shard value %d exceeds uint16 maximum %d", pb.Shard, math.MaxUint16)
	}
	if pb.GetPrimary() > math.MaxUint16 {
		return fmt.Errorf("the BAF Primary value %d exceeds uint16 maximum %d", pb.Primary, math.MaxUint16)
	}
	if pb.GetSigner() > math.MaxUint16 {
		return fmt.Errorf("the BAF Signer value %d exceeds uint16 maximum %d", pb.Signer, math.MaxUint16)
	}

	s.shard = ShardID(pb.GetShard())
	s.primary = PartyID(pb.GetPrimary())
	s.seq = BatchSequence(pb.GetSeq())
	s.digest = pb.GetDigest()
	s.configSequence = ConfigSequence(pb.GetConfigSequence())
	s.txCount = pb.GetTxCount()
	s.signer = PartyID(pb.GetSigner())

	// Normalize nil to empty slice for signature to maintain backward compatibility
	if pb.GetSignature() == nil {
		s.signature = []byte{}
	} else {
		s.signature = pb.GetSignature()
	}

	// Normalize empty slice to nil for primarySignature (original behavior)
	if len(pb.GetPrimarySignature()) == 0 {
		s.primarySignature = nil
	} else {
		s.primarySignature = pb.GetPrimarySignature()
	}

	return nil
}

// Serialize marshals every field including the signatures using protobuf.
func (s *SimpleBatchAttestationFragment) Serialize() []byte {
	pb := s.ToProto()
	result, err := proto.Marshal(pb)
	if err != nil {
		panic(err)
	}
	return result
}

// ToBeSigned marshals every field except the signatures, using an auxiliary ASN1 struct and asn1.Marshal.
func (s *SimpleBatchAttestationFragment) ToBeSigned() []byte {
	a := asn1BAF{
		Shard:          int(s.shard),
		Primary:        int(s.primary),
		Seq:            new(big.Int).SetUint64(uint64(s.seq)),
		Digest:         s.digest,
		ConfigSequence: new(big.Int).SetUint64(uint64(s.configSequence)),
		TXCount:        new(big.Int).SetUint64(uint64(s.txCount)),
		Signer:         int(s.signer),
		Sig:            nil, // everything but the signatures
		PrimarySig:     nil,
	}
	result, err := asn1.Marshal(a)
	if err != nil {
		panic(err)
	}
	return result
}

// Deserialize unmarshals every field including the signatures using protobuf.
func (s *SimpleBatchAttestationFragment) Deserialize(bytes []byte) error {
	pb := &stateprotos.BatchAttestationFragment{}
	err := proto.Unmarshal(bytes, pb)
	if err != nil {
		return err
	}

	return s.FromProto(pb)
}
