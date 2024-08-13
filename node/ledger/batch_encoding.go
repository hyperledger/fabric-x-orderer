package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"arma/core"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/pkg/errors"
)

// BlockMetadataIndex_Party is one location after the last entry that Fabric uses, which evaluates to 5.
var BlockMetadataIndex_Party = len(common.BlockMetadataIndex_name)

// FabricBatch is a core.Batch encoded in a Fabric block
type FabricBatch common.Block

func (b *FabricBatch) Digest() []byte {
	return (*common.Block)(b).GetHeader().GetDataHash()
}

func (b *FabricBatch) Requests() core.BatchedRequests {
	return (*common.Block)(b).GetData().GetData()
}

// Party returns the PartyID if encoded correctly, or 0.
func (b *FabricBatch) Party() core.PartyID {
	m := (*common.Block)(b).GetMetadata().GetMetadata()
	if len(m) <= BlockMetadataIndex_Party {
		return 0
	}

	buff := m[BlockMetadataIndex_Party]
	if len(buff) < 4 {
		return 0
	}

	return core.PartyID(binary.BigEndian.Uint16(buff[:2]))
}

// Shard returns the ShardID if encoded correctly, or 0.
func (b *FabricBatch) Shard() core.ShardID {
	m := (*common.Block)(b).GetMetadata().GetMetadata()
	if len(m) <= BlockMetadataIndex_Party {
		return 0
	}

	buff := m[BlockMetadataIndex_Party]
	if len(buff) < 4 {
		return 0
	}

	return core.ShardID(binary.BigEndian.Uint16(buff[2:]))
}

func (b *FabricBatch) Seq() core.BatchSequence {
	return core.BatchSequence((*common.Block)(b).GetHeader().GetNumber())
}

func NewFabricBatchFromRaw(partyID core.PartyID, shardID core.ShardID, seq uint64, batchBytes []byte, prevHash []byte) (*FabricBatch, error) {
	batchedRequests := core.BatchFromRaw(batchBytes) // TODO return an error, don't panic. See: https://github.ibm.com/decentralized-trust-research/ARMA/issues/132

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

func ShardPartyToChannelName(shardID core.ShardID, partyID core.PartyID) string {
	return fmt.Sprintf("shard%dparty%d", shardID, partyID)
}

func ChannelNameToShardParty(channelName string) (core.ShardID, core.PartyID, error) {
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

	return core.ShardID(shardID), core.PartyID(partyID), nil
}
