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
	"google.golang.org/protobuf/proto"

	"github.com/pkg/errors"
)

type Header struct {
	Num                   types.DecisionNum
	AvailableBlocks       []AvailableBlock
	AvailableCommonBlocks []*common.Block
	State                 *State
}

func (h *Header) Deserialize(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) < 8+4+4 { // at least header number (uint64) and number of available blocks (uint32) and number of available common blocks (uint32)
		return errors.Errorf("len of bytes is just %d; expected at least 16", len(bytes))
	}
	h.Num = types.DecisionNum(binary.BigEndian.Uint64(bytes[0:8]))
	availableBlockCount := int(binary.BigEndian.Uint32(bytes[8:12]))
	pos := 12
	h.AvailableBlocks = nil
	if availableBlockCount > 0 {
		h.AvailableBlocks = make([]AvailableBlock, availableBlockCount)
	}
	if len(bytes) < pos+availableBlockSerializedSize*availableBlockCount {
		return errors.Errorf("len of bytes is just %d which is not enough to read the available blocks; expected at least %d", len(bytes), pos+availableBlockSerializedSize*availableBlockCount)
	}
	for i := 0; i < availableBlockCount; i++ {
		var ab AvailableBlock
		if err := ab.Deserialize(bytes[pos : pos+availableBlockSerializedSize]); err != nil {
			return errors.Errorf("failed deserializing available block; index: %d", i)
		}
		pos += availableBlockSerializedSize
		h.AvailableBlocks[i] = ab
	}
	availableCommonBlockCount := int(binary.BigEndian.Uint32(bytes[pos : pos+4]))
	pos += 4
	h.AvailableCommonBlocks = nil
	if availableCommonBlockCount > 0 {
		h.AvailableCommonBlocks = make([]*common.Block, availableCommonBlockCount)
	}
	for i := 0; i < availableCommonBlockCount; i++ {
		rawBlockSize := int(binary.BigEndian.Uint32(bytes[pos : pos+4]))
		pos += 4
		rawBlock := bytes[pos : pos+rawBlockSize]
		block := &common.Block{}
		if err := proto.Unmarshal(rawBlock, block); err != nil {
			return errors.Errorf("failed to unmarshal available common block; index: %d", i)
		}
		pos += rawBlockSize
		h.AvailableCommonBlocks[i] = block
	}
	rawState := bytes[pos:]
	if len(rawState) == 0 {
		h.State = nil
		return nil
	}
	h.State = &State{}
	h.State.Deserialize(rawState, &BAFDeserialize{})

	return nil
}

func (h *Header) Serialize() []byte {
	availableBlocksBytes := availableBlocksToBytes(h.AvailableBlocks)
	availableCommonBlocksBytes := availableCommonBlocksToBytes(h.AvailableCommonBlocks)
	rawState := []byte{}
	if h.State != nil {
		rawState = h.State.Serialize()
	}

	buff := make([]byte, 8+len(availableBlocksBytes)+len(availableCommonBlocksBytes)+len(rawState))

	binary.BigEndian.PutUint64(buff, uint64(h.Num))
	copy(buff[8:], availableBlocksBytes)
	copy(buff[8+len(availableBlocksBytes):], availableCommonBlocksBytes)
	copy(buff[8+len(availableBlocksBytes)+len(availableCommonBlocksBytes):], rawState)

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
