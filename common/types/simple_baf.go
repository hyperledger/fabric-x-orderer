/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"encoding/asn1"
	"fmt"
	"math/big"
)

type SimpleBatchAttestationFragment struct {
	shard   ShardID
	primary PartyID
	seq     BatchSequence
	digest  []byte

	signer    PartyID
	signature []byte

	configSequence ConfigSequence
}

// NewSimpleBatchAttestationFragment creates a new, unsigned, SimpleBatchAttestationFragment.
func NewSimpleBatchAttestationFragment(shard ShardID, primary PartyID, seq BatchSequence, digest []byte, signer PartyID, configSqn ConfigSequence) *SimpleBatchAttestationFragment {
	return &SimpleBatchAttestationFragment{
		seq:            seq,
		primary:        primary,
		signer:         signer,
		shard:          shard,
		digest:         digest,
		configSequence: configSqn,
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

func (s *SimpleBatchAttestationFragment) Signature() []byte {
	return s.signature
}

func (s *SimpleBatchAttestationFragment) SetSignature(sig []byte) {
	s.signature = sig
}

func (s *SimpleBatchAttestationFragment) String() string {
	return fmt.Sprintf("BAF: Signer: %d; %s; Config Seq: %d", s.signer, BatchIDToString(s), s.configSequence)
}

type asn1BAF struct {
	Shard          int
	Primary        int
	Seq            *big.Int
	Digest         []byte
	ConfigSequence *big.Int
	Signer         int
	Sig            []byte
}

// Serialize marshals every field including the signature, using an auxiliary ASN1 struct and asn1.Marshal.
func (s *SimpleBatchAttestationFragment) Serialize() []byte {
	a := asn1BAF{
		Shard:          int(s.shard),
		Primary:        int(s.primary),
		Seq:            new(big.Int).SetUint64(uint64(s.seq)),
		Digest:         s.digest,
		ConfigSequence: new(big.Int).SetUint64(uint64(s.configSequence)),
		Signer:         int(s.signer),
		Sig:            s.signature,
	}
	result, err := asn1.Marshal(a)
	if err != nil {
		panic(err)
	}
	return result
}

// ToBeSigned marshals every field except the signature, using an auxiliary ASN1 struct and asn1.Marshal.
func (s *SimpleBatchAttestationFragment) ToBeSigned() []byte {
	a := asn1BAF{
		Shard:          int(s.shard),
		Primary:        int(s.primary),
		Seq:            new(big.Int).SetUint64(uint64(s.seq)),
		Digest:         s.digest,
		ConfigSequence: new(big.Int).SetUint64(uint64(s.configSequence)),
		Signer:         int(s.signer),
		Sig:            nil, // everything but the signature
	}
	result, err := asn1.Marshal(a)
	if err != nil {
		panic(err)
	}
	return result
}

// Deserialize unmarshalls every field including the signature, using an auxiliary ASN1 struct and asn1.Unmarshal.
func (s *SimpleBatchAttestationFragment) Deserialize(bytes []byte) error {
	a := &asn1BAF{}
	_, err := asn1.Unmarshal(bytes, a)
	if err != nil {
		return err
	}

	s.shard = ShardID(a.Shard)
	s.primary = PartyID(a.Primary)
	s.seq = BatchSequence(a.Seq.Uint64())
	s.digest = a.Digest
	s.configSequence = ConfigSequence(a.ConfigSequence.Uint64())
	s.signer = PartyID(a.Signer)
	s.signature = a.Sig

	return nil
}
