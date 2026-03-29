/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"encoding/binary"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type Header struct {
	Num                          types.DecisionNum
	PrevHash                     []byte
	AvailableCommonBlocks        []*common.Block
	State                        *State
	DecisionNumOfLastConfigBlock types.DecisionNum
}

const (
	// headerFixedSize is the minimum size of a serialized header in bytes:
	// 8 bytes for Num (uint64)
	// 8 bytes for DecisionNumOfLastConfigBlock (uint64)
	// 4 bytes for raw state size (uint32)
	// 2 bytes for prev hash size (uint16)
	// 4 bytes for number of available common blocks (uint32)
	headerFixedSize = 8 + 8 + 4 + 2 + 4
)

func (h *Header) Deserialize(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) < headerFixedSize {
		return errors.Errorf("len of bytes is just %d; expected at least %d", len(bytes), headerFixedSize)
	}
	h.Num = types.DecisionNum(binary.BigEndian.Uint64(bytes[0:8]))
	h.DecisionNumOfLastConfigBlock = types.DecisionNum(binary.BigEndian.Uint64(bytes[8:16]))
	rawStateSize := binary.BigEndian.Uint32(bytes[16:20])
	prevHashSize := binary.BigEndian.Uint16(bytes[20:22])
	availableCommonBlockCount := int(binary.BigEndian.Uint32(bytes[22:26]))
	pos := 26
	h.AvailableCommonBlocks = nil
	if availableCommonBlockCount > 0 {
		h.AvailableCommonBlocks = make([]*common.Block, availableCommonBlockCount)
	}
	for i := 0; i < availableCommonBlockCount; i++ {

		// Check if we have enough bytes to read the block size
		if pos+4 > len(bytes) {
			return errors.Errorf("insufficient bytes to read block size at index %d; need %d bytes, have %d", i, pos+4, len(bytes))
		}

		rawBlockSize := int(binary.BigEndian.Uint32(bytes[pos : pos+4]))
		pos += 4

		// Check if we have enough bytes to read the actual block
		if pos+rawBlockSize > len(bytes) {
			return errors.Errorf("insufficient bytes to read block data at index %d; need %d bytes, have %d", i, pos+rawBlockSize, len(bytes))
		}

		rawBlock := bytes[pos : pos+rawBlockSize]
		block := &common.Block{}
		if err := proto.Unmarshal(rawBlock, block); err != nil {
			return errors.Wrapf(err, "failed to unmarshal available common block; index: %d", i)
		}
		pos += rawBlockSize
		h.AvailableCommonBlocks[i] = block
	}

	// Check if we have enough bytes to read the state
	if pos+int(rawStateSize) > len(bytes) {
		return errors.Errorf("insufficient bytes to read state; need %d bytes, have %d", pos+int(rawStateSize), len(bytes))
	}

	rawState := bytes[pos : pos+int(rawStateSize)]
	if len(rawState) == 0 {
		h.State = nil
	} else {
		h.State = &State{}
		err := h.State.Deserialize(rawState, &BAFDeserialize{})
		if err != nil {
			return errors.Wrap(err, "failed to deserialize state")
		}
	}

	pos += int(rawStateSize)

	// Check if we have enough bytes to read the prev hash
	if pos+int(prevHashSize) > len(bytes) {
		return errors.Errorf("insufficient bytes to read prev hash; need %d bytes, have %d", pos+int(prevHashSize), len(bytes))
	}
	h.PrevHash = bytes[pos : pos+int(prevHashSize)]
	if len(h.PrevHash) == 0 {
		h.PrevHash = nil
	}

	return nil
}

func (h *Header) Serialize() []byte {
	availableCommonBlocksBytes := availableCommonBlocksToBytes(h.AvailableCommonBlocks)
	rawState := []byte{}
	if h.State != nil {
		rawState = h.State.Serialize()
	}

	buff := make([]byte, headerFixedSize-4+len(availableCommonBlocksBytes)+len(rawState)+len(h.PrevHash)) // 4 bytes less because the number of blocks is already included in availableCommonBlocksBytes

	binary.BigEndian.PutUint64(buff, uint64(h.Num))
	binary.BigEndian.PutUint64(buff[8:16], uint64(h.DecisionNumOfLastConfigBlock))
	binary.BigEndian.PutUint32(buff[16:20], uint32(len(rawState)))
	binary.BigEndian.PutUint16(buff[20:22], uint16(len(h.PrevHash)))
	copy(buff[22:], availableCommonBlocksBytes)
	copy(buff[22+len(availableCommonBlocksBytes):], rawState)
	copy(buff[22+len(availableCommonBlocksBytes)+len(rawState):], h.PrevHash)

	return buff
}

func availableCommonBlocksToBytes(blocks []*common.Block) []byte {
	blockBuff := bytes.Buffer{}
	for _, b := range blocks {
		rawBlock, err := proto.Marshal(b)
		if err != nil {
			panic(err)
		}
		rawBlockSize := make([]byte, 4)
		binary.BigEndian.PutUint32(rawBlockSize, uint32(len(rawBlock))) // TODO check the size limit, I guess uint32 is enough
		blockBuff.Write(rawBlockSize)
		blockBuff.Write(rawBlock)
	}
	bytes := blockBuff.Bytes()
	buff := make([]byte, 4+len(bytes))
	binary.BigEndian.PutUint32(buff[0:4], uint32(len(blocks)))
	copy(buff[4:], bytes)
	return buff
}
