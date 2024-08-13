package state

import (
	"encoding/binary"

	arma_types "arma/common/types"
	arma "arma/core"
)

func ToBeSignedBAF(baf arma.BatchAttestationFragment) []byte {
	buff := make([]byte, 2+8+2+8+2+32+len(baf.GarbageCollect())*32)
	var pos int
	binary.BigEndian.PutUint16(buff, uint16(baf.Shard()))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], uint64(baf.Seq()))
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(baf.Signer()))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], baf.Epoch())
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(baf.Primary()))
	pos += 2
	copy(buff[pos:], baf.Digest())
	pos += 32
	for _, gc := range baf.GarbageCollect() {
		copy(buff[pos:], gc)
		pos += 32
	}

	return buff
}

type BAFDeserializer struct{}

func (bafd *BAFDeserializer) Deserialize(bytes []byte) (arma.BatchAttestationFragment, error) {
	var baf arma_types.SimpleBatchAttestationFragment
	if err := baf.Deserialize(bytes); err != nil {
		return nil, err
	}
	return &baf, nil
}
