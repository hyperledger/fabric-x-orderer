/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package block

import (
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

// TODO: use CreateConfigBlock from Dor's PR
// BlockWithGroups is taken from Fabric for testing purpose.
func BlockWithGroups(groups *cb.ConfigGroup, channelID string, blockNumber uint64) *cb.Block {
	block := protoutil.NewBlock(blockNumber, nil)
	block.Data = &cb.BlockData{
		Data: [][]byte{
			protoutil.MarshalOrPanic(&cb.Envelope{
				Payload: protoutil.MarshalOrPanic(&cb.Payload{
					Data: protoutil.MarshalOrPanic(&cb.ConfigEnvelope{
						Config: &cb.Config{
							Sequence:     uint64(0),
							ChannelGroup: groups,
						},
					}),
					Header: &cb.Header{
						ChannelHeader: protoutil.MarshalOrPanic(&cb.ChannelHeader{
							Type:      int32(cb.HeaderType_CONFIG),
							ChannelId: channelID,
						}),
					},
				}),
			}),
		},
	}
	block.Header.DataHash = protoutil.ComputeBlockDataHash(block.Data)
	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
			LastConfig: &cb.LastConfig{
				Index: uint64(blockNumber),
			},
		}),
	})

	return block
}
