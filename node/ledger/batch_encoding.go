/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-x-orderer/common/types"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/pkg/errors"
)

// BlockMetadataIndex_PartyShardConfigSequence is the location where we encode the ShardID, primary PartyID, and config sequence.
// We reuse the location of the orderer metadata.
const BlockMetadataIndex_PartyShardConfigSequence = common.BlockMetadataIndex_ORDERER

// The size of the partyShardConfigSequenceSize metadata: shard uint16 + party uint16 + config sequence uint64
const partyShardConfigSequenceSize = 2 + 2 + 8

// FabricBatch is a types.Batch encoded in a Fabric block.
type FabricBatch common.Block

func (b *FabricBatch) Digest() []byte {
	return (*common.Block)(b).GetHeader().GetDataHash()
}

func (b *FabricBatch) Requests() types.BatchedRequests {
	return (*common.Block)(b).GetData().GetData()
}

// Primary returns the PartyID if encoded correctly, or 0.
func (b *FabricBatch) Primary() types.PartyID {
	buff := ordererMetadata((*common.Block)(b).GetMetadata().GetMetadata())
	if len(buff) == 0 {
		return 0
	}

	return types.PartyID(binary.BigEndian.Uint16(buff[2:4]))
}

// Shard returns the ShardID if encoded correctly, or 0.
func (b *FabricBatch) Shard() types.ShardID {
	buff := ordererMetadata((*common.Block)(b).GetMetadata().GetMetadata())
	if len(buff) == 0 {
		return 0
	}

	return types.ShardID(binary.BigEndian.Uint16(buff[0:2]))
}

// ConfigSequence returns the ConfigSequence if encoded correctly, or 0.
func (b *FabricBatch) ConfigSequence() types.ConfigSequence {
	buff := ordererMetadata((*common.Block)(b).GetMetadata().GetMetadata())
	if len(buff) == 0 {
		return 0
	}

	return types.ConfigSequence(binary.BigEndian.Uint64(buff[4:]))
}

// PrimarySignature returns the primary signature from block metadata at SIGNATURES index.
func (b *FabricBatch) PrimarySignature() []byte {
	m := (*common.Block)(b).GetMetadata().GetMetadata()
	if len(m) <= int(common.BlockMetadataIndex_SIGNATURES) {
		return nil
	}
	return m[common.BlockMetadataIndex_SIGNATURES]
}

func ordererMetadata(m [][]byte) []byte {
	if len(m) <= int(BlockMetadataIndex_PartyShardConfigSequence) {
		return nil
	}

	buff := m[BlockMetadataIndex_PartyShardConfigSequence]
	if len(buff) < partyShardConfigSequenceSize {
		return nil
	}

	return buff
}

func (b *FabricBatch) Seq() types.BatchSequence {
	return types.BatchSequence((*common.Block)(b).GetHeader().GetNumber())
}

func NewFabricBatchFromRequests(
	shardID types.ShardID,
	partyID types.PartyID,
	seq types.BatchSequence,
	batchedRequests types.BatchedRequests,
	configSeq types.ConfigSequence,
	prevHash []byte,
	primarySignature []byte,
) *FabricBatch {
	buff := make([]byte, partyShardConfigSequenceSize)
	binary.BigEndian.PutUint16(buff[:2], uint16(shardID))
	binary.BigEndian.PutUint16(buff[2:4], uint16(partyID))
	binary.BigEndian.PutUint64(buff[4:], uint64(configSeq))

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       uint64(seq),
			PreviousHash: prevHash,
			DataHash:     batchedRequests.Digest(),
		},
		Data: &common.BlockData{
			Data: batchedRequests,
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{primarySignature, {}, {}, buff, {}},
		},
	}

	return (*FabricBatch)(block)
}

func NewFabricBatchFromBlock(block *common.Block) (*FabricBatch, error) {
	if block == nil {
		return nil, errors.New("empty block")
	}
	if block.Header == nil {
		return nil, errors.New("empty block header")
	}
	if block.Data == nil {
		return nil, errors.New("empty block data")
	}
	if block.Metadata == nil || len(block.GetMetadata().GetMetadata()) == 0 {
		return nil, errors.New("empty block metadata")
	}

	m := block.GetMetadata().GetMetadata()
	if len(m) <= int(BlockMetadataIndex_PartyShardConfigSequence) {
		return nil, errors.New("missing orderer metadata")
	}

	buff := m[BlockMetadataIndex_PartyShardConfigSequence]
	if len(buff) < partyShardConfigSequenceSize {
		return nil, errors.New("bad orderer metadata")
	}

	batch := (*FabricBatch)(block)
	return batch, nil
}

// ShardPartyChannelIDToChannelName builds the channel name of the form
// "shard<shardID>party<partyID>-<channelID>".
func ShardPartyChannelIDToChannelName(shardID types.ShardID, partyID types.PartyID, channelID string) string {
	return fmt.Sprintf("shard%dparty%d-%s", shardID, partyID, channelID)
}

// ChannelNameToShardPartyChannelID parses the ShardID, PartyID and ChannelID out of a channel name of
// the form "shard<shardID>party<partyID>-<channelID>". The "-<channelID>" suffix is mandatory.
func ChannelNameToShardPartyChannelID(channelName string) (types.ShardID, types.PartyID, string, error) {
	s, ok := strings.CutPrefix(channelName, "shard")
	if !ok {
		return 0, 0, "", errors.Errorf("channel name does not start with 'shard': %s", channelName)
	}

	shard, rest, found := strings.Cut(s, "party")
	if !found {
		return 0, 0, "", errors.Errorf("channel name does not contain 'party': %s", channelName)
	}

	// ShardID and PartyID are uint16, so parse directly into 16 bits to reject overflow and negatives.
	shardID, err := strconv.ParseUint(shard, 10, 16)
	if err != nil {
		return 0, 0, "", errors.Errorf("cannot extract 'shardID' from channel name: %s, err: %s", channelName, err)
	}

	// The remainder is "<partyID>-<channelID>"; the channelID itself may contain further '-'.
	partyStr, channelID, found := strings.Cut(rest, "-")
	if !found {
		return 0, 0, "", errors.Errorf("channel name does not contain the '-<channelID>' suffix: %s", channelName)
	}

	partyID, err := strconv.ParseUint(partyStr, 10, 16)
	if err != nil {
		return 0, 0, "", errors.Errorf("cannot extract 'partyID' from channel name: %s, err: %s", channelName, err)
	}

	if err := configtx.ValidateChannelID(channelID); err != nil {
		return 0, 0, "", errors.Errorf("cannot extract 'channelID' from channel name: %s, err: %s", channelName, err)
	}

	return types.ShardID(shardID), types.PartyID(partyID), channelID, nil
}
