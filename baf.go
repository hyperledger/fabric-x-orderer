package arma

import (
	"encoding/asn1"
)

type SimpleBatchAttestationFragment struct {
	Se        int
	P, Si, Sh int
	Dig       []byte
	Sig       []byte
}

func (s *SimpleBatchAttestationFragment) Seq() uint64 {
	return uint64(s.Se)
}

func (s *SimpleBatchAttestationFragment) Primary() uint16 {
	return uint16(s.P)
}

func (s *SimpleBatchAttestationFragment) Signer() uint16 {
	return uint16(s.Si)
}

func (s *SimpleBatchAttestationFragment) Shard() uint16 {
	return uint16(s.Sh)
}

func (s *SimpleBatchAttestationFragment) Digest() []byte {
	return s.Dig
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

func (b *SimpleBatchAttestation) Primary() uint16 {
	if len(b.F) == 0 {
		panic("empty batch attestation")
	}
	return b.F[0].Primary()
}

func (b *SimpleBatchAttestation) Shard() uint16 {
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
