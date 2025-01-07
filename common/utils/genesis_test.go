/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils_test

import (
	"testing"

	"arma/common/utils"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestEmptyGenesisBlock(t *testing.T) {
	block := utils.EmptyGenesisBlock("arma")

	t.Run("test for transaction id", func(t *testing.T) {
		configEnv, _ := protoutil.ExtractEnvelope(block, 0)
		configEnvPayload, _ := protoutil.UnmarshalPayload(configEnv.Payload)
		configEnvPayloadChannelHeader, _ := protoutil.UnmarshalChannelHeader(configEnvPayload.GetHeader().ChannelHeader)
		require.NotEmpty(t, configEnvPayloadChannelHeader.TxId, "tx_id of configuration transaction should not be empty")
	})

	t.Run("test for last config in SIGNATURES field", func(t *testing.T) {
		metadata := &cb.Metadata{}
		err := proto.Unmarshal(block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES], metadata)
		require.NoError(t, err)
		ordererBlockMetadata := &cb.OrdererBlockMetadata{}
		err = proto.Unmarshal(metadata.Value, ordererBlockMetadata)
		require.NoError(t, err)
		require.NotNil(t, ordererBlockMetadata.LastConfig)
		require.Equal(t, uint64(0), ordererBlockMetadata.LastConfig.Index)
	})

	t.Run("test for last config in LAST_CONFIG field", func(t *testing.T) {
		metadata := &cb.Metadata{}
		err := proto.Unmarshal(block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG], metadata)
		require.NoError(t, err)
		lastConfig := &cb.LastConfig{}
		err = proto.Unmarshal(metadata.Value, lastConfig)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastConfig.Index)
	})

	t.Run("test for is config block", func(t *testing.T) {
		require.True(t, protoutil.IsConfigBlock(block))
	})
}
