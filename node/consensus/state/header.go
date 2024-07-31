package state

import (
	"encoding/binary"
)

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
