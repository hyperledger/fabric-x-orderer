package state

import (
	"encoding/binary"

	"arma/core"

	"github.com/pkg/errors"
)

type Header struct {
	Num              uint64
	AvailableBatches []AvailableBatch // TODO consider consolidating available batches and block headers into a single struct (available blocks)
	BlockHeaders     []BlockHeader
	State            *core.State
}

func (h *Header) FromBytes(rawHeader []byte) error {
	if rawHeader == nil {
		return errors.Errorf("nil rawHeader")
	}
	if len(rawHeader) < 8+4+4 { // at least header number (uint64) and length of available batches and length of block headers
		return errors.Errorf("len of rawHeader is just %d which is not enough", len(rawHeader))
	}
	h.Num = binary.BigEndian.Uint64(rawHeader[0:8])
	availableBatchCount := int(binary.BigEndian.Uint32(rawHeader[8:12]))
	pos := 12
	h.AvailableBatches = nil
	if len(rawHeader) < pos+availableBatchSerializedSize*availableBatchCount {
		return errors.Errorf("len of rawHeader is just %d which is not enough to read the available batches", len(rawHeader))
	}
	for i := 0; i < availableBatchCount; i++ {
		var ab AvailableBatch
		if err := ab.Deserialize(rawHeader[pos : pos+availableBatchSerializedSize]); err != nil {
			return errors.Errorf("failed deserializing available batch; index: %d", i)
		}
		pos += availableBatchSerializedSize
		h.AvailableBatches = append(h.AvailableBatches, ab)
	}

	if len(rawHeader) < pos+4 {
		return errors.Errorf("len of rawHeader is just %d which is not enough to read the length of block headers", len(rawHeader))
	}
	blockHeaderCount := int(binary.BigEndian.Uint32(rawHeader[pos : pos+4]))
	if availableBatchCount != blockHeaderCount {
		return errors.Errorf("availableBatchCount (%d) != blockHeaderCount (%d)", availableBatchCount, blockHeaderCount)
	}
	pos += 4
	h.BlockHeaders = nil
	if len(rawHeader) < pos+blockHeaderBytesSize*blockHeaderCount {
		return errors.Errorf("len of rawHeader is just %d which is not enough to read the block headers", len(rawHeader))
	}
	for i := 0; i < blockHeaderCount; i++ {
		var bh BlockHeader
		if err := bh.FromBytes(rawHeader[pos : pos+blockHeaderBytesSize]); err != nil {
			return errors.Errorf("failed deserializing block header; index: %d", i)
		}
		pos += blockHeaderBytesSize
		h.BlockHeaders = append(h.BlockHeaders, bh)
	}

	rawState := rawHeader[pos:]
	if len(rawState) == 0 {
		h.State = nil
		return nil
	}
	h.State = &core.State{}
	h.State.Deserialize(rawState, &BAFDeserializer{})

	return nil
}

func (h *Header) Bytes() []byte {
	if len(h.AvailableBatches) != len(h.BlockHeaders) {
		panic("len(h.AvailableBatches) != len(h.BlockHeaders)")
	}
	availableBatchesBytes := availableBatchesToBytes(h.AvailableBatches)
	blockHeadersBytes := blockHeadersToBytes(h.BlockHeaders)
	rawState := []byte{}
	if h.State != nil {
		rawState = h.State.Serialize()
	}
	buff := make([]byte, 8+len(availableBatchesBytes)+len(blockHeadersBytes)+len(rawState))

	binary.BigEndian.PutUint64(buff, h.Num)
	copy(buff[8:], availableBatchesBytes)
	copy(buff[8+len(availableBatchesBytes):], blockHeadersBytes)
	copy(buff[8+len(availableBatchesBytes)+len(blockHeadersBytes):], rawState)

	return buff
}

func availableBatchesToBytes(availableBatches []AvailableBatch) []byte {
	buff := make([]byte, 4+len(availableBatches)*availableBatchSerializedSize)
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
	buff := make([]byte, 4+len(blockHeaders)*blockHeaderBytesSize)
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(blockHeaders)))
	pos := 4
	for _, bh := range blockHeaders {
		bytes := bh.Bytes()
		copy(buff[pos:], bytes)
		pos += len(bytes)
	}
	return buff
}
