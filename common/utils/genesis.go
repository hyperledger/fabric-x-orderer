/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EmptyGenesisBlock constructs and returns an empty genesis block for a given channel ID.
func EmptyGenesisBlock(channelID string) *common.Block {
	payloadChannelHeader := &common.ChannelHeader{
		Type:      int32(common.HeaderType_CONFIG),
		Version:   1,
		Timestamp: &timestamppb.Timestamp{}, // no time
		ChannelId: channelID,
		Epoch:     0,
	}

	payloadSignatureHeader := protoutil.MakeSignatureHeader(nil, make([]byte, 24)) // zero nonce
	protoutil.SetTxID(payloadChannelHeader, payloadSignatureHeader)
	payloadHeader := protoutil.MakePayloadHeader(payloadChannelHeader, payloadSignatureHeader)
	payload := &common.Payload{Header: payloadHeader, Data: protoutil.MarshalOrPanic(&common.ConfigEnvelope{Config: &common.Config{ChannelGroup: protoutil.NewConfigGroup()}})}
	envelope := &common.Envelope{Payload: protoutil.MarshalOrPanic(payload), Signature: nil}

	block := protoutil.NewBlock(0, nil)
	block.Data = &common.BlockData{Data: [][]byte{protoutil.MarshalOrPanic(envelope)}}
	block.Header.DataHash = protoutil.ComputeBlockDataHash(block.Data)
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: 0}),
	})
	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
			LastConfig: &common.LastConfig{Index: 0},
		}),
	})
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = GenesisBlockMetadataBytes()
	return block
}

// EmptyGenesisBlockBytes constructs and returns an empty, marshalled, genesis block for a given channel ID.
func EmptyGenesisBlockBytes(channelID string) []byte {
	return protoutil.MarshalOrPanic(EmptyGenesisBlock(channelID))
}

// GenesisBlockMetadataBytes returns the serialized metadata for the genesis block using protobuf.
func GenesisBlockMetadataBytes() []byte {
	md := &stateprotos.AssemblerBlockMetadata{
		Primary:          0,
		Shard:            uint32(types.ShardIDConsensus),
		Sequence:         0,
		DecisionNum:      0,
		BatchIndex:       0,
		BatchCount:       0,
		TransactionCount: 1,
	}
	metadata, err := proto.Marshal(md)
	if err != nil {
		panic(err)
	}
	return metadata
}
