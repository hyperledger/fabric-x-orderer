/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"encoding/binary"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
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
	// TODO an empty config for now
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

// uint16 + uint16 + uint64 + uint64 + uint32 + uint32 + uint64
const blockMetadataSerializedSize = 2 + 2 + 8 + 8 + 4 + 4 + 8

func GenesisBlockMetadataBytes() []byte {
	buff := make([]byte, blockMetadataSerializedSize)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], 0) // primary
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(types.ShardIDConsensus))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], 0) // seq
	pos += 8

	binary.BigEndian.PutUint64(buff[pos:], 0) // decision num
	pos += 8
	binary.BigEndian.PutUint32(buff[pos:], 0) // batch index
	pos += 4
	binary.BigEndian.PutUint32(buff[pos:], 0) // batch count
	pos += 4
	binary.BigEndian.PutUint64(buff[pos:], 1) // transaction count

	return buff
}
