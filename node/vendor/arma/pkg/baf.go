package arma

import (
	"encoding/asn1"
	"sort"
)

type SimpleBatchAttestationFragment struct {
	Ep        int
	Se        int
	P, Si, Sh int
	Dig       []byte
	Sig       []byte
	Gc        [][]byte
}

func (s *SimpleBatchAttestationFragment) Seq() uint64 {
	return uint64(s.Se)
}

func (s *SimpleBatchAttestationFragment) Primary() PartyID {
	return PartyID(s.P)
}

func (s *SimpleBatchAttestationFragment) Signer() PartyID {
	return PartyID(s.Si)
}

func (s *SimpleBatchAttestationFragment) Shard() ShardID {
	return ShardID(s.Sh)
}

func (s *SimpleBatchAttestationFragment) Digest() []byte {
	return s.Dig
}

func (s *SimpleBatchAttestationFragment) Epoch() uint64 {
	return uint64(s.Ep)
}

func (s *SimpleBatchAttestationFragment) Serialize() []byte {
	bytes, err := asn1.Marshal(*s)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (s *SimpleBatchAttestationFragment) Deserialize(bytes []byte) error {
	_, err := asn1.Unmarshal(bytes, s)
	return err
}

func (s *SimpleBatchAttestationFragment) GarbageCollect() [][]byte {
	return s.Gc
}

type SimpleBatchAttestation struct {
	F []SimpleBatchAttestationFragment
}

func (b *SimpleBatchAttestation) Fragments() []BatchAttestationFragment {
	res := make([]BatchAttestationFragment, len(b.F))
	for i := 0; i < len(b.F); i++ {
		res[i] = &b.F[i]
	}
	return res
}

func (b *SimpleBatchAttestation) Epoch() uint64 {
	epochs := make([]int, len(b.F))
	for i := 0; i < len(b.F); i++ {
		epochs[i] = int(b.F[i].Epoch())
	}

	sort.Ints(epochs)

	return uint64(epochs[len(epochs)/2])
}

func (b *SimpleBatchAttestation) Digest() []byte {
	if len(b.F) == 0 {
		panic("empty batch attestation")
	}
	return b.F[0].Digest()
}

func (b *SimpleBatchAttestation) Seq() uint64 {
	if len(b.F) == 0 {
		panic("empty batch attestation")
	}
	return b.F[0].Seq()
}

func (b *SimpleBatchAttestation) Primary() PartyID {
	if len(b.F) == 0 {
		panic("empty batch attestation")
	}
	return b.F[0].Primary()
}

func (b *SimpleBatchAttestation) Shard() ShardID {
	if len(b.F) == 0 {
		panic("empty batch attestation")
	}
	return b.F[0].Shard()
}

func (b *SimpleBatchAttestation) Serialize() []byte {
	if len(b.F) == 0 {
		panic("empty batch attestation")
	}

	bytes, err := asn1.Marshal(*b)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (b *SimpleBatchAttestation) Deserialize(bytes []byte) error {
	_, err := asn1.Unmarshal(bytes, b)
	return err
}
