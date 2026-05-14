/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer"
	"github.com/stretchr/testify/require"
)

func TestConsenterBlockOperations_IsConfigBlock(t *testing.T) {
	ops := &synchronizer.ConsenterBlockOperations{}

	t.Run("returns true for consenter config block", func(t *testing.T) {
		configBlock := createConsenterConfigBlock(t, 5)
		require.True(t, ops.IsConfigBlock(configBlock))
	})

	t.Run("returns true for genesis block", func(t *testing.T) {
		genesisBlock := createConsenterConfigBlock(t, 0)
		require.True(t, ops.IsConfigBlock(genesisBlock))
	})

	t.Run("returns false for non-config consenter block", func(t *testing.T) {
		normalBlock := createConsenterNormalBlock(t, 10)
		require.False(t, ops.IsConfigBlock(normalBlock))
	})

	t.Run("returns false for nil block", func(t *testing.T) {
		require.False(t, ops.IsConfigBlock(nil))
	})

	t.Run("returns false for block with nil header", func(t *testing.T) {
		block := &common.Block{
			Data: &common.BlockData{Data: [][]byte{[]byte("data")}},
		}
		require.False(t, ops.IsConfigBlock(block))
	})
}

func TestConsenterBlockOperations_ConfigFromBlock(t *testing.T) {
	ops := &synchronizer.ConsenterBlockOperations{}

	t.Run("extracts config from consenter config block", func(t *testing.T) {
		configBlock := createConsenterConfigBlock(t, 5)
		configEnv, err := ops.ConfigFromBlock(configBlock)
		require.NoError(t, err)
		require.NotNil(t, configEnv)
		require.NotNil(t, configEnv.Config)
	})

	t.Run("returns error for nil block", func(t *testing.T) {
		_, err := ops.ConfigFromBlock(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block is nil")
	})

	t.Run("returns error for block with nil header", func(t *testing.T) {
		block := &common.Block{
			Data: &common.BlockData{Data: [][]byte{[]byte("data")}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block header is nil")
	})

	t.Run("returns error for block with empty data", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{Number: 5},
			Data:   &common.BlockData{Data: [][]byte{}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block data is empty")
	})

	t.Run("returns error for consenter block with no available common blocks", func(t *testing.T) {
		// Create a consenter block with empty AvailableCommonBlocks
		header := &state.Header{
			Num:                          5,
			DecisionNumOfLastConfigBlock: 5,
			AvailableCommonBlocks:        []*common.Block{}, // Empty
		}
		proposal := smartbft_types.Proposal{
			Header: header.Serialize(),
		}
		block := state.CreateBlockToAppendFromDecision(5, proposal, []smartbft_types.Signature{}, []byte("prevhash"), 5)

		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no available common blocks")
	})
}

// Helper functions

func createCommonConfigBlock(t *testing.T, blockNum uint64) *common.Block {
	configEnv := &common.ConfigEnvelope{
		Config: &common.Config{
			Sequence:     1,
			ChannelGroup: &common.ConfigGroup{},
		},
	}

	payload := &common.Payload{
		Header: &common.Header{
			ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
				Type:      int32(common.HeaderType_CONFIG),
				ChannelId: "testchannel",
			}),
		},
		Data: protoutil.MarshalOrPanic(configEnv),
	}

	env := &common.Envelope{
		Payload: protoutil.MarshalOrPanic(payload),
	}

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:   blockNum,
			DataHash: []byte("datahash"),
		},
		Data: &common.BlockData{
			Data: [][]byte{protoutil.MarshalOrPanic(env)},
		},
	}

	protoutil.InitBlockMetadata(block)
	setLastConfigIndex(t, block, blockNum)

	return block
}

func createCommonNormalBlock(t *testing.T, blockNum uint64) *common.Block {
	payload := &common.Payload{
		Header: &common.Header{
			ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				ChannelId: "testchannel",
			}),
		},
		Data: []byte("transaction data"),
	}

	env := &common.Envelope{
		Payload: protoutil.MarshalOrPanic(payload),
	}

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:   blockNum,
			DataHash: []byte("datahash"),
		},
		Data: &common.BlockData{
			Data: [][]byte{protoutil.MarshalOrPanic(env)},
		},
	}

	protoutil.InitBlockMetadata(block)
	setLastConfigIndex(t, block, blockNum-1)

	return block
}

func createConsenterConfigBlock(t *testing.T, blockNum uint64) *common.Block {
	// Create a Fabric config block
	fabricConfigBlock := createCommonConfigBlock(t, blockNum)

	// Create a consenter decision header with the config block
	header := &state.Header{
		Num:                          types.DecisionNum(blockNum),
		DecisionNumOfLastConfigBlock: types.DecisionNum(blockNum),
		AvailableCommonBlocks:        []*common.Block{fabricConfigBlock},
	}

	proposal := smartbft_types.Proposal{
		Header:               header.Serialize(),
		Payload:              []byte("payload"),
		Metadata:             []byte("metadata"),
		VerificationSequence: 1,
	}

	signatures := []smartbft_types.Signature{
		{ID: 1, Value: []byte("sig1"), Msg: []byte("msg1")},
	}

	return state.CreateBlockToAppendFromDecision(blockNum, proposal, signatures, []byte("prevhash"), blockNum)
}

func createConsenterNormalBlock(t *testing.T, blockNum uint64) *common.Block {
	// Create normal Fabric blocks (non-config)
	fabricBlock1 := createCommonNormalBlock(t, blockNum)
	fabricBlock2 := createCommonNormalBlock(t, blockNum+1)

	// Create a consenter decision header with normal blocks
	header := &state.Header{
		Num:                          types.DecisionNum(blockNum),
		DecisionNumOfLastConfigBlock: 0,
		AvailableCommonBlocks:        []*common.Block{fabricBlock1, fabricBlock2},
	}

	proposal := smartbft_types.Proposal{
		Header:               header.Serialize(),
		Payload:              []byte("payload"),
		Metadata:             []byte("metadata"),
		VerificationSequence: 1,
	}

	signatures := []smartbft_types.Signature{
		{ID: 1, Value: []byte("sig1"), Msg: []byte("msg1")},
	}

	return state.CreateBlockToAppendFromDecision(blockNum, proposal, signatures, []byte("prevhash"), 0)
}

func setLastConfigIndex(t *testing.T, block *common.Block, index uint64) {
	if block.Metadata == nil {
		protoutil.InitBlockMetadata(block)
	}

	lastConfig := &common.LastConfig{Index: index}
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(lastConfig),
	})
}
