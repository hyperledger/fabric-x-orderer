package state

import (
	"encoding/binary"

	arma "arma/core"
)

type AvailableBatch struct {
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

func (ab *AvailableBatch) Serialize() []byte {
	buff := make([]byte, 32+2*2+8)
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

type Header struct {
	Num              uint64
	AvailableBatches []AvailableBatch
	State            []byte
}

func (h *Header) FromBytes(rawHeader []byte) error {
	h.Num = binary.BigEndian.Uint64(rawHeader[0:8])
	availableBatchCount := int(binary.BigEndian.Uint16(rawHeader[8:10]))

	pos := 10

	h.AvailableBatches = nil
	for i := 0; i < availableBatchCount; i++ {
		abSize := 2 + 2 + 8 + 32
		var ab AvailableBatch
		ab.Deserialize(rawHeader[pos : pos+abSize])
		pos += abSize
		h.AvailableBatches = append(h.AvailableBatches, ab)
	}

	h.State = rawHeader[pos:]

	return nil
}

func (h *Header) Bytes() []byte {
	prefix := make([]byte, 8+2)
	binary.BigEndian.PutUint64(prefix, h.Num)
	binary.BigEndian.PutUint16(prefix[8:10], uint16(len(h.AvailableBatches)))

	availableBatchesBytes := availableBatchesToBytes(h.AvailableBatches)

	buff := make([]byte, len(availableBatchesBytes)+len(prefix)+len(h.State))
	copy(buff, prefix)
	copy(buff[len(prefix):], availableBatchesBytes)
	copy(buff[len(prefix)+len(availableBatchesBytes):], h.State)

	return buff
}

func availableBatchesToBytes(availableBatches []AvailableBatch) []byte {
	sequencesBuff := make([]byte, len(availableBatches)*(32+2*2+8))

	var pos int
	for _, ab := range availableBatches {
		bytes := ab.Serialize()
		copy(sequencesBuff[pos:], bytes)
		pos += len(bytes)
	}
	return sequencesBuff
}

func BatchAttestationFromBytes(in []byte) (arma.BatchAttestationFragment, error) {
	var baf arma.SimpleBatchAttestationFragment
	if err := baf.Deserialize(in); err != nil {
		return nil, err
	}

	return &baf, nil
}
