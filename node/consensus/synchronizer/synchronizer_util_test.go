/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"
)

func noopUpdateLastHash(_ *cb.Block) types.Reconfig { return types.Reconfig{} }

func makeBlockWithMetadata(sqnNum, lastConfigIndex uint64, viewMetadata *smartbftprotos.ViewMetadata) *cb.Block {
	block := protoutil.NewBlock(sqnNum, nil)
	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(
		&cb.Metadata{
			Value: protoutil.MarshalOrPanic(&cb.LastConfig{Index: lastConfigIndex}),
		},
	)
	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
			ConsenterMetadata: protoutil.MarshalOrPanic(viewMetadata),
			LastConfig: &cb.LastConfig{
				Index: lastConfigIndex,
			},
		}),
	})
	return block
}

func makeConfigBlockWithMetadata(configBlock *cb.Block, sqnNum uint64, viewMetadata *smartbftprotos.ViewMetadata) *cb.Block {
	block := proto.Clone(configBlock).(*cb.Block)
	block.Header.Number = sqnNum

	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(
		&cb.Metadata{
			Value: protoutil.MarshalOrPanic(&cb.LastConfig{Index: sqnNum}),
		},
	)
	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
			ConsenterMetadata: protoutil.MarshalOrPanic(viewMetadata),
			LastConfig: &cb.LastConfig{
				Index: sqnNum,
			},
		}),
	})
	return block
}
