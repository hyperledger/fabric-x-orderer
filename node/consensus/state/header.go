package state

import (
	"encoding/binary"

	arma "arma/core"

	"github.com/pkg/errors"
)

type Header struct {
	Num              uint64
	AvailableBatches []AvailableBatch
	BlockHeaders     []BlockHeader
	State            arma.State
}

func (h *Header) FromBytes(rawHeader []byte) error {
	if rawHeader == nil {
		return errors.Errorf("nil rawHeader")
	}
	if len(rawHeader) < 8+4+4 {
		return errors.Errorf("len of rawHeader is just %d which is not enough", len(rawHeader))
	}
	h.Num = binary.BigEndian.Uint64(rawHeader[0:8])
	availableBatchCount := int(binary.BigEndian.Uint32(rawHeader[8:12]))
	pos := 12
	h.AvailableBatches = nil
	for i := 0; i < availableBatchCount; i++ {
		var ab AvailableBatch
		if err := ab.Deserialize(rawHeader[pos : pos+AvailableBatchSerializedSize]); err != nil {
			return errors.Errorf("failed deserializing available batch; index: %d", i)
		}
		pos += AvailableBatchSerializedSize
		h.AvailableBatches = append(h.AvailableBatches, ab)
	}

	blockHeaderCount := int(binary.BigEndian.Uint32(rawHeader[pos : pos+4]))
	pos += 4
	h.BlockHeaders = nil
	for i := 0; i < blockHeaderCount; i++ {
		var bh BlockHeader
		if err := bh.FromBytes(rawHeader[pos : pos+BlockHeaderBytesSize]); err != nil {
			return errors.Errorf("failed deserializing block header; index: %d", i)
		}
		pos += BlockHeaderBytesSize
		h.BlockHeaders = append(h.BlockHeaders, bh)
	}

	if availableBatchCount != blockHeaderCount {
		panic("availableBatchCount != blockHeaderCount")
	}

	rawState := rawHeader[pos:]
	h.State.Deserialize(rawState, &BAFDeserializer{})

	return nil
}

func (h *Header) Bytes() []byte {
	if len(h.AvailableBatches) != len(h.BlockHeaders) {
		panic("len(h.AvailableBatches) != len(h.BlockHeaders)")
	}
	availableBatchesBytes := availableBatchesToBytes(h.AvailableBatches)
	blockHeadersBytes := blockHeadersToBytes(h.BlockHeaders)
	rawState := h.State.Serialize()
	buff := make([]byte, 8+len(availableBatchesBytes)+len(blockHeadersBytes)+len(rawState))

	binary.BigEndian.PutUint64(buff, h.Num)
	copy(buff[8:], availableBatchesBytes)
	copy(buff[8+len(availableBatchesBytes):], blockHeadersBytes)
	copy(buff[8+len(availableBatchesBytes)+len(blockHeadersBytes):], rawState)

	return buff
}

func availableBatchesToBytes(availableBatches []AvailableBatch) []byte {
	buff := make([]byte, 4+len(availableBatches)*AvailableBatchSerializedSize)
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(availableBatches)))
	pos := 4
	for _, ab := range availableBatches {
		bytes := ab.Serialize()
		copy(buff[pos:], bytes)
		pos += len(bytes)
	}
	return buff
}

func blockHeadersToBytes(blockHeaders []BlockHeader) []byte {
	buff := make([]byte, 4+len(blockHeaders)*BlockHeaderBytesSize)
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(blockHeaders)))
	pos := 4
	for _, bh := range blockHeaders {
		bytes := bh.Bytes()
		copy(buff[pos:], bytes)
		pos += len(bytes)
	}
	return buff
}
