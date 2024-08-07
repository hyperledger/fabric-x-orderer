package state

import (
	"encoding/binary"
)

type Header struct {
	Num              uint64
	AvailableBatches []AvailableBatch
	BlockHeaders     []BlockHeader
	State            []byte // TODO change to actual State instaed of bytes
}

func (h *Header) FromBytes(rawHeader []byte) error {
	h.Num = binary.BigEndian.Uint64(rawHeader[0:8])
	availableBatchCount := int(binary.BigEndian.Uint16(rawHeader[8:10])) // TODO this is a limitation on the number of availble batches in a block
	blockHeaderCount := int(binary.BigEndian.Uint16(rawHeader[10:12]))
	if availableBatchCount != blockHeaderCount {
		panic("availableBatchCount != blockHeaderCount")
	}

	pos := 12

	h.AvailableBatches = nil
	for i := 0; i < availableBatchCount; i++ {
		var ab AvailableBatch
		ab.Deserialize(rawHeader[pos : pos+AvailableBatchSerializedSize])
		pos += AvailableBatchSerializedSize
		h.AvailableBatches = append(h.AvailableBatches, ab)
	}

	h.BlockHeaders = nil
	for i := 0; i < blockHeaderCount; i++ {
		var bh BlockHeader
		bh.FromBytes(rawHeader[pos : pos+BlockHeaderBytesSize])
		pos += BlockHeaderBytesSize
		h.BlockHeaders = append(h.BlockHeaders, bh)
	}

	h.State = rawHeader[pos:]

	return nil
}

func (h *Header) Bytes() []byte {
	if len(h.AvailableBatches) != len(h.BlockHeaders) {
		panic("len(h.AvailableBatches) != len(h.BlockHeaders)")
	}
	availableBatchesBytes := availableBatchesToBytes(h.AvailableBatches)
	blockHeadersBytes := blockHeadersToBytes(h.BlockHeaders)
	prefix := 8 + 2 + 2
	buff := make([]byte, prefix+len(blockHeadersBytes)+len(availableBatchesBytes)+len(h.State))

	binary.BigEndian.PutUint64(buff, h.Num)
	binary.BigEndian.PutUint16(buff[8:10], uint16(len(h.AvailableBatches)))
	binary.BigEndian.PutUint16(buff[10:12], uint16(len(h.BlockHeaders)))
	copy(buff[prefix:], availableBatchesBytes)
	copy(buff[prefix+len(availableBatchesBytes):], blockHeadersBytes)
	copy(buff[prefix+len(availableBatchesBytes)+len(blockHeadersBytes):], h.State)

	return buff
}

func availableBatchesToBytes(availableBatches []AvailableBatch) []byte {
	sequencesBuff := make([]byte, len(availableBatches)*AvailableBatchSerializedSize)

	var pos int
	for _, ab := range availableBatches {
		bytes := ab.Serialize()
		copy(sequencesBuff[pos:], bytes)
		pos += len(bytes)
	}
	return sequencesBuff
}

func blockHeadersToBytes(blockHeaders []BlockHeader) []byte {
	buff := make([]byte, len(blockHeaders)*BlockHeaderBytesSize)
	var pos int
	for _, bh := range blockHeaders {
		bytes := bh.Bytes()
		copy(buff[pos:], bytes)
		pos += len(bytes)
	}
	return buff
}
