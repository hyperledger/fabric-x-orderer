/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"encoding/binary"

	"github.com/hyperledger/fabric-x-orderer/common/types"

	"github.com/pkg/errors"
)

type AvailableBatch struct {
	primary types.PartyID
	shard   types.ShardID
	seq     types.BatchSequence
	digest  []byte
}

func NewAvailableBatch(
	primary types.PartyID,
	shard types.ShardID,
	seq types.BatchSequence,
	digest []byte,
) *AvailableBatch {
	return &AvailableBatch{
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

// Fragments
// TODO return the fragments with minimal data (at least the batchers that signed)
func (ab *AvailableBatch) Fragments() []types.BatchAttestationFragment {
	panic("should not be called")
}

func (ab *AvailableBatch) Digest() []byte {
	return ab.digest
}

func (ab *AvailableBatch) Seq() types.BatchSequence {
	return types.BatchSequence(ab.seq)
}

func (ab *AvailableBatch) Primary() types.PartyID {
	return types.PartyID(ab.primary)
}

func (ab *AvailableBatch) Shard() types.ShardID {
	return types.ShardID(ab.shard)
}

const availableBatchSerializedSize = 2 + 2 + 8 + 32 // uint16 + uint16 + uint64 + digest

func (ab *AvailableBatch) Serialize() []byte {
	buff := make([]byte, availableBatchSerializedSize)
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
	if len(bytes) != availableBatchSerializedSize {
		return errors.Errorf("len of bytes %d does not equal the available batch size %d", len(bytes), availableBatchSerializedSize)
	}
	ab.primary = types.PartyID(binary.BigEndian.Uint16(bytes[0:2]))
	ab.shard = types.ShardID(binary.BigEndian.Uint16(bytes[2:4]))
	ab.seq = types.BatchSequence(binary.BigEndian.Uint64(bytes[4:12]))
	ab.digest = bytes[12:]

	return nil
}

func (ab *AvailableBatch) String() string {
	return types.BatchIDToString(ab)
}
