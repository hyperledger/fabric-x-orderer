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
	AvailableCommonBlocks        []*common.Block
	State                        *State
	DecisionNumOfLastConfigBlock types.DecisionNum
}

func (h *Header) Deserialize(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) < 8+8+4 { // at least header number (uint64) and config num (uint64) and number of available common blocks (uint32)
		return errors.Errorf("len of bytes is just %d; expected at least 20", len(bytes))
	}
	h.Num = types.DecisionNum(binary.BigEndian.Uint64(bytes[0:8]))
	h.DecisionNumOfLastConfigBlock = types.DecisionNum(binary.BigEndian.Uint64(bytes[8:16]))
	availableCommonBlockCount := int(binary.BigEndian.Uint32(bytes[16:20]))
	pos := 20
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
	availableCommonBlocksBytes := availableCommonBlocksToBytes(h.AvailableCommonBlocks)
	rawState := []byte{}
	if h.State != nil {
		rawState = h.State.Serialize()
	}

	buff := make([]byte, 8+8+len(availableCommonBlocksBytes)+len(rawState))

	binary.BigEndian.PutUint64(buff, uint64(h.Num))
	binary.BigEndian.PutUint64(buff[8:16], uint64(h.DecisionNumOfLastConfigBlock))
	copy(buff[16:], availableCommonBlocksBytes)
	copy(buff[16+len(availableCommonBlocksBytes):], rawState)

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
