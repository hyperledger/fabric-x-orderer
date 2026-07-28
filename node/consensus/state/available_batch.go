/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
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

// Fragments
// TODO return the fragments with minimal data (at least the batchers that signed)
func (ab *AvailableBatch) Fragments() []types.BatchAttestationFragment {
	panic("should not be called")
}

func (ab *AvailableBatch) Digest() []byte {
	return ab.digest
}

func (ab *AvailableBatch) Seq() types.BatchSequence {
	return ab.seq
}

func (ab *AvailableBatch) Primary() types.PartyID {
	return ab.primary
}

func (ab *AvailableBatch) Shard() types.ShardID {
	return ab.shard
}

const (
	// availableBatchDigestSize is the fixed digest size the serialized format encodes.
	availableBatchDigestSize = 32
	// availableBatchSerializedSize is the exact size of a serialized AvailableBatch.
	availableBatchSerializedSize = 2 + 2 + 8 + availableBatchDigestSize // uint16 + uint16 + uint64 + digest
)

func (ab *AvailableBatch) Serialize() []byte {
	buff := make([]byte, availableBatchSerializedSize)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], uint16(ab.primary))
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(ab.shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], uint64(ab.seq))
	pos += 8
	// copy is bounded by the destination, so a wrong-sized digest is silently
	// truncated / zero-padded to availableBatchDigestSize.
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
	// Copy the digest into an independent slice so the batch does not alias (and get
	// corrupted by mutations of) the caller's input buffer.
	ab.digest = make([]byte, availableBatchDigestSize)
	copy(ab.digest, bytes[12:])

	return nil
}

func (ab *AvailableBatch) String() string {
	return types.BatchIDToString(ab)
}
