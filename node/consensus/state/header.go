package state

import (
	"encoding/binary"

	"arma/common/types"
	"arma/core"

	"github.com/pkg/errors"
)

type Header struct {
	Num             types.DecisionNum
	AvailableBlocks []AvailableBlock
	State           *core.State
}

func (h *Header) Deserialize(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) < 8+4 { // at least header number (uint64) and length of available blocks (uint32)
		return errors.Errorf("len of bytes is just %d; expected at least 12", len(bytes))
	}
	h.Num = types.DecisionNum(binary.BigEndian.Uint64(bytes[0:8]))
	availableBlockCount := int(binary.BigEndian.Uint32(bytes[8:12]))
	pos := 12
	h.AvailableBlocks = nil
	if len(bytes) < pos+availableBlockSerializedSize*availableBlockCount {
		return errors.Errorf("len of bytes is just %d which is not enough to read the available blocks; expected at least %d", len(bytes), pos+availableBlockSerializedSize*availableBlockCount)
	}
	for i := 0; i < availableBlockCount; i++ {
		var ab AvailableBlock
		if err := ab.Deserialize(bytes[pos : pos+availableBlockSerializedSize]); err != nil {
			return errors.Errorf("failed deserializing available block; index: %d", i)
		}
		pos += availableBlockSerializedSize
		h.AvailableBlocks = append(h.AvailableBlocks, ab)
	}
	rawState := bytes[pos:]
	if len(rawState) == 0 {
		h.State = nil
		return nil
	}
	h.State = &core.State{}
	h.State.Deserialize(rawState, &BAFDeserializer{})

	return nil
}

func (h *Header) Serialize() []byte {
	availableBlocksBytes := availableBlocksToBytes(h.AvailableBlocks)
	rawState := []byte{}
	if h.State != nil {
		rawState = h.State.Serialize()
	}

	buff := make([]byte, 8+len(availableBlocksBytes)+len(rawState))

	binary.BigEndian.PutUint64(buff, uint64(h.Num))
	copy(buff[8:], availableBlocksBytes)
	copy(buff[8+len(availableBlocksBytes):], rawState)

	return buff
}

func availableBlocksToBytes(availableBlocks []AvailableBlock) []byte {
	buff := make([]byte, 4+len(availableBlocks)*availableBlockSerializedSize)
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(availableBlocks)))
	pos := 4
	for _, ab := range availableBlocks {
		bytes := ab.Serialize()
		copy(buff[pos:], bytes)
		pos += len(bytes)
	}
	return buff
}
