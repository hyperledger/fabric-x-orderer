package state

import (
	"bytes"
	"encoding/binary"

	"arma/core"

	"github.com/pkg/errors"
)

type AvailableBatch struct {
	primary core.PartyID
	shard   core.ShardID
	seq     core.BatchSequence
	digest  []byte
}

func NewAvailableBatch(
	primary core.PartyID,
	shard core.ShardID,
	seq core.BatchSequence,
	digest []byte,
) AvailableBatch {
	return AvailableBatch{
		primary: primary,
		shard:   shard,
		seq:     seq,
		digest:  digest,
	}
}

func (ab *AvailableBatch) Equal(ab2 *AvailableBatch) bool {
	if ab.primary != ab2.primary || ab.shard != ab2.shard || ab.seq != ab2.seq {
		return false
	}
	return bytes.Equal(ab.digest, ab.digest)
}

// TODO define a seprate interface for AvailableBatch
func (ab *AvailableBatch) Fragments() []core.BatchAttestationFragment {
	panic("should not be called")
}

func (ab *AvailableBatch) Digest() []byte {
	return ab.digest
}

func (ab *AvailableBatch) Seq() core.BatchSequence {
	return core.BatchSequence(ab.seq)
}

func (ab *AvailableBatch) Primary() core.PartyID {
	return core.PartyID(ab.primary)
}

func (ab *AvailableBatch) Shard() core.ShardID {
	return core.ShardID(ab.shard)
}

var AvailableBatchSerializedSize = 2 + 2 + 8 + 32 // uint16 + uint16 + uint64 + digest

func (ab *AvailableBatch) Serialize() []byte {
	buff := make([]byte, AvailableBatchSerializedSize)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], uint16(ab.primary))
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(ab.shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], uint64(ab.seq))
	pos += 8
	copy(buff[pos:], ab.digest)

	return buff
}

func (ab *AvailableBatch) Deserialize(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) != AvailableBatchSerializedSize {
		return errors.Errorf("len of bytes %d does not equal the available batch size %d", len(bytes), AvailableBatchSerializedSize)
	}
	ab.primary = core.PartyID(binary.BigEndian.Uint16(bytes[0:2]))
	ab.shard = core.ShardID(binary.BigEndian.Uint16(bytes[2:4]))
	ab.seq = core.BatchSequence(binary.BigEndian.Uint64(bytes[4:12]))
	ab.digest = bytes[12:]

	return nil
}
