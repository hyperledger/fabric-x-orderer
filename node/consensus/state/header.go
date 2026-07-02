/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Header represents the header of a proposal as part of a consensus decision.
// It contains metadata about the decision, including the decision number,
// previous hash, available common blocks, and the consensus state.
type Header struct {
	// Num is the decision number for this header
	Num types.DecisionNum
	// PrevHash is the hash of the previous header
	PrevHash []byte
	// AvailableCommonBlocks contains the common blocks that are available at this decision
	AvailableCommonBlocks []*common.Block
	// State is the consensus state at this decision
	State *State
	// DecisionNumOfLastConfigBlock is the decision number of the last configuration block
	DecisionNumOfLastConfigBlock types.DecisionNum
}

// Serialize converts the Header to a byte slice using protocol buffer serialization.
// It marshals the Header into a deterministic protobuf format for consistent serialization.
// The method panics if marshaling fails, as this indicates a programming error.
func (h *Header) Serialize() []byte {
	// Serialize available common blocks
	var availableCommonBlocksBytes [][]byte
	if len(h.AvailableCommonBlocks) > 0 {
		availableCommonBlocksBytes = make([][]byte, len(h.AvailableCommonBlocks))
		for i, block := range h.AvailableCommonBlocks {
			rawBlock, err := proto.MarshalOptions{Deterministic: true}.Marshal(block)
			if err != nil {
				panic(fmt.Sprintf("failed to marshal available common block at index %d: %v", i, err))
			}
			availableCommonBlocksBytes[i] = rawBlock
		}
	}

	// Serialize state
	var rawState []byte
	if h.State != nil {
		rawState = h.State.Serialize()
	}

	// Convert Header to proto stateprotos.Header
	protoHeader := &stateprotos.Header{
		Num:                          uint64(h.Num),
		DecisionNumOfLastConfigBlock: uint64(h.DecisionNumOfLastConfigBlock),
		PrevHash:                     h.PrevHash,
		AvailableCommonBlocks:        availableCommonBlocksBytes,
		State:                        rawState,
	}

	buff, err := proto.MarshalOptions{Deterministic: true}.Marshal(protoHeader)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal header: %v", err))
	}

	return buff
}

// Deserialize populates the Header from a byte slice using protocol buffer deserialization.
// It unmarshals the protobuf-encoded bytes and reconstructs the Header structure.
// Returns an error if the input is nil, if unmarshaling fails, or if any nested
// deserialization (blocks or state) fails.
func (h *Header) Deserialize(rawBytes []byte) error {
	if rawBytes == nil {
		return errors.New("nil bytes")
	}

	if len(rawBytes) == 0 {
		return errors.New("empty bytes")
	}

	var protoHeader stateprotos.Header
	if err := proto.Unmarshal(rawBytes, &protoHeader); err != nil {
		return errors.Wrap(err, "failed to unmarshal header")
	}

	h.Num = types.DecisionNum(protoHeader.Num)
	h.DecisionNumOfLastConfigBlock = types.DecisionNum(protoHeader.DecisionNumOfLastConfigBlock)
	h.PrevHash = protoHeader.PrevHash

	// Deserialize available common blocks
	h.AvailableCommonBlocks = nil
	if len(protoHeader.AvailableCommonBlocks) > 0 {
		h.AvailableCommonBlocks = make([]*common.Block, len(protoHeader.AvailableCommonBlocks))
		for i, rawBlock := range protoHeader.AvailableCommonBlocks {
			block := &common.Block{}
			if err := proto.Unmarshal(rawBlock, block); err != nil {
				return errors.Wrapf(err, "failed to unmarshal available common block at index %d", i)
			}
			h.AvailableCommonBlocks[i] = block
		}
	}

	// Deserialize state
	if len(protoHeader.State) == 0 {
		h.State = nil
	} else {
		h.State = &State{}
		if err := h.State.Deserialize(protoHeader.State); err != nil {
			return errors.Wrap(err, "failed to deserialize state")
		}
	}

	return nil
}
