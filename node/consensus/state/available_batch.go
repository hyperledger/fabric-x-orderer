package state

import (
	"encoding/binary"

	arma "arma/core"
)

type AvailableBatch struct {
	// TODO change the types in here to PartyID / ShardID, and define a type for the uint64, maybe BatchSequence
	primary uint16
	shard   uint16
	seq     uint64
	digest  []byte
}

func NewAvailableBatch(
	primary uint16,
	shard uint16,
	seq uint64,
	digest []byte,
) AvailableBatch {
	return AvailableBatch{
		primary: primary,
		shard:   shard,
		seq:     seq,
		digest:  digest,
	}
}

// TODO define a seprate interface for AvailableBatch
func (ab *AvailableBatch) Fragments() []arma.BatchAttestationFragment {
	panic("should not be called")
}

func (ab *AvailableBatch) Digest() []byte {
	return ab.digest
}

func (ab *AvailableBatch) Seq() uint64 {
	return ab.seq
}

func (ab *AvailableBatch) Primary() arma.PartyID {
	return arma.PartyID(ab.primary)
}

func (ab *AvailableBatch) Shard() arma.ShardID {
	return arma.ShardID(ab.shard)
}

var AvailableBatchSerializedSize = 2 + 2 + 8 + 32 // uint16 + uint16 + uint64 + digest

func (ab *AvailableBatch) Serialize() []byte {
	buff := make([]byte, AvailableBatchSerializedSize)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], ab.primary)
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], ab.shard)
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], ab.seq)
	pos += 8
	copy(buff[pos:], ab.digest)

	return buff
}

func (ab *AvailableBatch) Deserialize(bytes []byte) error {
	ab.primary = binary.BigEndian.Uint16(bytes[0:2])
	ab.shard = binary.BigEndian.Uint16(bytes[2:4])
	ab.seq = binary.BigEndian.Uint64(bytes[4:12])
	ab.digest = bytes[12:]

	return nil
}
