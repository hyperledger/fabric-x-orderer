package node

import (
	"encoding/asn1"

	"arma/core"
)

type SimpleBatchAttestationFragment struct {
	Ep        int
	Se        int // TODO change to uint64
	P, Si, Sh int // TODO change to party signer and shard with types
	Dig       []byte
	Sig       []byte
	Gc        [][]byte
}

func (s *SimpleBatchAttestationFragment) Seq() core.BatchSequence {
	return core.BatchSequence(s.Se)
}

func (s *SimpleBatchAttestationFragment) Primary() core.PartyID {
	return core.PartyID(s.P)
}

func (s *SimpleBatchAttestationFragment) Signer() core.PartyID {
	return core.PartyID(s.Si)
}

func (s *SimpleBatchAttestationFragment) Shard() core.ShardID {
	return core.ShardID(s.Sh)
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
