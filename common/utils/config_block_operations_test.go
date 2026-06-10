/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/stretchr/testify/require"
)

func TestCommonBlockOperations_IsConfigBlock(t *testing.T) {
	ops := &utils.CommonConfigBlockOperations{}

	t.Run("returns true for config block", func(t *testing.T) {
		configBlock := createCommonConfigBlock(t, 5)
		require.True(t, ops.IsConfigBlock(configBlock))
	})

	t.Run("returns false for non-config block", func(t *testing.T) {
		normalBlock := createCommonNormalBlock(t, 5)
		require.False(t, ops.IsConfigBlock(normalBlock))
	})

	t.Run("returns false for nil block", func(t *testing.T) {
		require.False(t, ops.IsConfigBlock(nil))
	})
}

func TestCommonBlockOperations_ConfigFromBlock(t *testing.T) {
	ops := &utils.CommonConfigBlockOperations{}

	t.Run("extracts config from config block", func(t *testing.T) {
		configBlock := createCommonConfigBlock(t, 5)
		configEnv, err := ops.ConfigFromBlock(configBlock)
		require.NoError(t, err)
		require.NotNil(t, configEnv)
		require.NotNil(t, configEnv.Config)
	})

	t.Run("extracts config from genesis block", func(t *testing.T) {
		genesisBlock := createCommonConfigBlock(t, 0)
		configEnv, err := ops.ConfigFromBlock(genesisBlock)
		require.NoError(t, err)
		require.NotNil(t, configEnv)
		require.NotNil(t, configEnv.Config)
	})

	t.Run("returns error for non-config block", func(t *testing.T) {
		normalBlock := createCommonNormalBlock(t, 5)
		_, err := ops.ConfigFromBlock(normalBlock)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a config block")
	})

	t.Run("returns error for nil block", func(t *testing.T) {
		_, err := ops.ConfigFromBlock(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty block")
	})

	t.Run("returns error for block with no data", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{Number: 5},
			Data:   &common.BlockData{Data: [][]byte{}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty block")
	})

	t.Run("returns error for nil block data", func(t *testing.T) {
		block := &common.Block{}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty block")
	})

	t.Run("returns error for invalid envelope", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{Number: 1},
			Data:   &common.BlockData{Data: [][]byte{{1, 2, 3}}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error unmarshalling Envelope")
	})

	t.Run("returns error for invalid payload in envelope", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{Number: 2},
			Data: &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(&common.Envelope{
				Payload: []byte{1, 2, 3},
			})}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error unmarshalling Payload")
	})

	t.Run("returns error for bad genesis block with invalid config", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{},
			Data: &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(&common.Envelope{
				Payload: protoutil.MarshalOrPanic(&common.Payload{
					Data: []byte{1, 2, 3},
				}),
			})}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid config envelope")
	})

	t.Run("returns error for invalid channel header", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{Number: 1},
			Data: &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(&common.Envelope{
				Payload: protoutil.MarshalOrPanic(&common.Payload{
					Header: &common.Header{
						ChannelHeader: []byte{1, 2, 3},
					},
				}),
			})}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error unmarshalling ChannelHeader")
	})

	t.Run("returns error for invalid config envelope in config block", func(t *testing.T) {
		block := &common.Block{
			Header: &common.BlockHeader{Number: 1},
			Data: &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(&common.Envelope{
				Payload: protoutil.MarshalOrPanic(&common.Payload{
					Data: []byte{1, 2, 3},
					Header: &common.Header{
						ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
							Type: int32(common.HeaderType_CONFIG),
						}),
					},
				}),
			})}},
		}
		_, err := ops.ConfigFromBlock(block)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid config envelope")
	})
}

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

func setLastConfigIndex(t *testing.T, block *common.Block, index uint64) {
	if block.Metadata == nil {
		protoutil.InitBlockMetadata(block)
	}

	lastConfig := &common.LastConfig{Index: index}
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(lastConfig),
	})
}
