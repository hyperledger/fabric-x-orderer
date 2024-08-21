package types

import (
	"encoding/asn1"
	"math/big"
)

type SimpleBatchAttestationFragment struct {
	shard   ShardID
	primary PartyID
	seq     BatchSequence
	digest  []byte

	signer    PartyID
	signature []byte

	epoch          int64
	garbageCollect [][]byte
}

func NewSimpleBatchAttestationFragment(
	shard ShardID,
	primary PartyID,
	seq BatchSequence,
	digest []byte,
	signer PartyID,
	sig []byte,
	epoch int64,
	garbageCollect [][]byte,
) *SimpleBatchAttestationFragment {
	return &SimpleBatchAttestationFragment{
		epoch:          epoch,
		seq:            seq,
		primary:        primary,
		signer:         signer,
		shard:          shard,
		digest:         digest,
		signature:      sig,
		garbageCollect: garbageCollect,
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

func (s *SimpleBatchAttestationFragment) Epoch() int64 {
	return (s.epoch)
}

func (s *SimpleBatchAttestationFragment) Signature() []byte {
	return s.signature
}

func (s *SimpleBatchAttestationFragment) SetSignature(sig []byte) {
	s.signature = sig
}

type asn1BAF struct {
	Shard          int
	Primary        int
	Seq            *big.Int
	Digest         []byte
	Signer         int
	Sig            []byte
	Epoch          *big.Int
	GarbageCollect [][]byte
}

func (s *SimpleBatchAttestationFragment) Serialize() []byte {
	a := asn1BAF{
		Shard:          int(s.shard),
		Primary:        int(s.primary),
		Seq:            new(big.Int).SetUint64(uint64(s.seq)),
		Digest:         s.digest,
		Signer:         int(s.signer),
		Sig:            s.signature,
		Epoch:          new(big.Int).SetInt64(s.epoch),
		GarbageCollect: s.garbageCollect,
	}
	result, err := asn1.Marshal(a)
	if err != nil {
		panic(err)
	}
	return result
}

func (s *SimpleBatchAttestationFragment) ToBeSigned() []byte {
	a := asn1BAF{
		Shard:          int(s.shard),
		Primary:        int(s.primary),
		Seq:            new(big.Int).SetUint64(uint64(s.seq)),
		Digest:         s.digest,
		Signer:         int(s.signer),
		Sig:            nil, // everything but the signature
		Epoch:          new(big.Int).SetInt64(s.epoch),
		GarbageCollect: s.garbageCollect,
	}
	result, err := asn1.Marshal(a)
	if err != nil {
		panic(err)
	}
	return result
}

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
	s.signer = PartyID(a.Signer)
	s.signature = a.Sig
	s.epoch = a.Epoch.Int64()
	s.garbageCollect = a.GarbageCollect

	return nil
}

func (s *SimpleBatchAttestationFragment) GarbageCollect() [][]byte {
	return s.garbageCollect
}
