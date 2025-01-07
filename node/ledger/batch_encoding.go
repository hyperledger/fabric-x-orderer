package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"arma/common/types"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
)

// BlockMetadataIndex_PartyShard is one location after the last entry that Fabric uses, which evaluates to 5.
// It includes the primary PartyID and ShardID.
var BlockMetadataIndex_PartyShard = len(common.BlockMetadataIndex_name)

// FabricBatch is a core.Batch encoded in a Fabric block
type FabricBatch common.Block

func (b *FabricBatch) Digest() []byte {
	return (*common.Block)(b).GetHeader().GetDataHash()
}

func (b *FabricBatch) Requests() types.BatchedRequests {
	return (*common.Block)(b).GetData().GetData()
}

// Primary returns the PartyID if encoded correctly, or 0.
func (b *FabricBatch) Primary() types.PartyID {
	m := (*common.Block)(b).GetMetadata().GetMetadata()
	if len(m) <= BlockMetadataIndex_PartyShard {
		return 0
	}

	buff := m[BlockMetadataIndex_PartyShard]
	if len(buff) < 4 {
		return 0
	}

	return types.PartyID(binary.BigEndian.Uint16(buff[:2]))
}

// Shard returns the ShardID if encoded correctly, or 0.
func (b *FabricBatch) Shard() types.ShardID {
	m := (*common.Block)(b).GetMetadata().GetMetadata()
	if len(m) <= BlockMetadataIndex_PartyShard {
		return 0
	}

	buff := m[BlockMetadataIndex_PartyShard]
	if len(buff) < 4 {
		return 0
	}

	return types.ShardID(binary.BigEndian.Uint16(buff[2:]))
}

func (b *FabricBatch) Seq() types.BatchSequence {
	return types.BatchSequence((*common.Block)(b).GetHeader().GetNumber())
}

func NewFabricBatchFromRaw(partyID types.PartyID, shardID types.ShardID, seq uint64, batchBytes []byte, prevHash []byte) (*FabricBatch, error) {
	var batchedRequests types.BatchedRequests
	if err := batchedRequests.Deserialize(batchBytes); err != nil {
		return nil, err
	}

	buff := make([]byte, 4)
	binary.BigEndian.PutUint16(buff[:2], uint16(partyID))
	binary.BigEndian.PutUint16(buff[2:], uint16(shardID))

	digest := sha256.Sum256(batchBytes)

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       seq,
			PreviousHash: prevHash,
			DataHash:     digest[:],
		},
		Data: &common.BlockData{
			Data: batchedRequests,
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}, buff},
		},
	}

	return (*FabricBatch)(block), nil
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
	if len(m) <= BlockMetadataIndex_PartyShard {
		return nil, errors.New("missing shard party metadata")
	}

	buff := m[BlockMetadataIndex_PartyShard]
	if len(buff) < 4 {
		return nil, errors.New("bad shard party metadata")
	}

	batch := (*FabricBatch)(block)
	return batch, nil
}

func ShardPartyToChannelName(shardID types.ShardID, partyID types.PartyID) string {
	return fmt.Sprintf("shard%dparty%d", shardID, partyID)
}

func ChannelNameToShardParty(channelName string) (types.ShardID, types.PartyID, error) {
	s, ok := strings.CutPrefix(channelName, "shard")
	if !ok {
		return 0, 0, errors.Errorf("channel name does not start with 'shard': %s", channelName)
	}

	shard, party, found := strings.Cut(s, "party")
	if !found {
		return 0, 0, errors.Errorf("channel name does not contain 'party': %s", channelName)
	}

	shardID, err := strconv.Atoi(shard)
	if err != nil {
		return 0, 0, errors.Errorf("cannot extract 'shardID' from channel name: %s, err: %s", channelName, err)
	}

	partyID, err := strconv.Atoi(party)
	if err != nil {
		return 0, 0, errors.Errorf("cannot extract 'partyID' from channel name: %s, err: %s", channelName, err)
	}

	return types.ShardID(shardID), types.PartyID(partyID), nil
}
