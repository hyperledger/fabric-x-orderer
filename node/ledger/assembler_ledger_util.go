/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"

	"github.com/pkg/errors"
)

// uint16 + uint16 + uint64 + uint64 + uint32 + uint32 + uint64
const assemblerBlockMetadataSerializedSize = 2 + 2 + 8 + 8 + 4 + 4 + 8

func AssemblerBlockMetadataToBytes(batchID types.BatchID, orderingInfo *state.OrderingInformation, transactionCount uint64) ([]byte, error) {
	if batchID == nil {
		return nil, errors.Errorf("nil batchID")
	}
	if orderingInfo == nil {
		return nil, errors.Errorf("nil orderingInfo")
	}

	buff := make([]byte, assemblerBlockMetadataSerializedSize)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], uint16(batchID.Primary()))
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(batchID.Shard()))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], uint64(batchID.Seq()))
	pos += 8

	binary.BigEndian.PutUint64(buff[pos:], uint64(orderingInfo.DecisionNum))
	pos += 8
	binary.BigEndian.PutUint32(buff[pos:], uint32(orderingInfo.BatchIndex))
	pos += 4
	binary.BigEndian.PutUint32(buff[pos:], uint32(orderingInfo.BatchCount))
	pos += 4
	binary.BigEndian.PutUint64(buff[pos:], transactionCount)

	return buff, nil
}

func AssemblerBlockMetadataFromBytes(metadata []byte) (primary types.PartyID, shard types.ShardID, seq types.BatchSequence, num types.DecisionNum, batchIndex, batchCount uint32, transactionCount uint64, err error) {
	if metadata == nil {
		return 0, 0, 0, 0, 0, 0, 0, errors.Errorf("nil bytes")
	}
	if len(metadata) < assemblerBlockMetadataSerializedSize {
		return 0, 0, 0, 0, 0, 0, 0, errors.Errorf("len of metadata %d smaller than expected size %d", len(metadata), assemblerBlockMetadataSerializedSize)
	}

	primary = types.PartyID(binary.BigEndian.Uint16(metadata[0:2]))
	shard = types.ShardID(binary.BigEndian.Uint16(metadata[2:4]))
	seq = types.BatchSequence(binary.BigEndian.Uint64(metadata[4:12]))

	num = types.DecisionNum(binary.BigEndian.Uint64(metadata[12:20]))
	batchIndex = (binary.BigEndian.Uint32(metadata[20:24]))
	batchCount = (binary.BigEndian.Uint32(metadata[24:28]))
	transactionCount = (binary.BigEndian.Uint64(metadata[28:36]))

	return
}

// AssemblerBatchIdOrderingInfoAndTxCountFromBlock returns the BatchID, the OrderingInformation and the transactions count that are encoded in the metadata
// and header of the block.
func AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block *common.Block) (types.BatchID, *state.OrderingInformation, uint64, error) {
	if block == nil {
		return nil, nil, 0, errors.Errorf("nil block")
	}

	if block.Header == nil {
		return nil, nil, 0, errors.Errorf("nil block header")
	}

	if block.Metadata == nil || len(block.Metadata.Metadata) == 0 {
		return nil, nil, 0, errors.Errorf("missing block metadata")
	}

	if len(block.Metadata.Metadata) < int(common.BlockMetadataIndex_ORDERER)+1 {
		return nil, nil, 0, errors.Errorf("missing block ORDERER metadata")
	}

	pr, sh, seq, num, bI, bC, tC, err := AssemblerBlockMetadataFromBytes(block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER])
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "failed to extract AssemblerBlockMetadata")
	}

	mdSigs, err := protoutil.GetMetadataFromBlock(block, common.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "failed to extract signatures")
	}

	var bftSigs []smartbft_types.Signature
	for _, sig := range mdSigs.Signatures {
		identifierHeader, err := protoutil.UnmarshalIdentifierHeader(sig.IdentifierHeader)
		if err != nil {
			return nil, nil, 0, errors.Wrap(err, "failed to extract signature identifier")
		}
		bftSigs = append(bftSigs, smartbft_types.Signature{
			ID:    uint64(identifierHeader.GetIdentifier()),
			Value: sig.Signature,
		})
	}

	ab := state.NewAvailableBatch(pr, sh, seq, block.GetHeader().GetDataHash())
	oi := &state.OrderingInformation{
		BlockHeader: &state.BlockHeader{
			Number:   block.GetHeader().GetNumber(),
			PrevHash: block.GetHeader().GetPreviousHash(),
			Digest:   block.GetHeader().GetDataHash(),
		},
		Signatures:  bftSigs,
		DecisionNum: num,
		BatchIndex:  int(bI),
		BatchCount:  int(bC),
	}
	return ab, oi, tC, nil
}

func BatchFrontierToString(frontier BatchFrontier) string {
	// discover shards and parties for consistent iteration order, thus consistent string output
	shardIDs := []types.ShardID{}
	partySet := make(map[types.PartyID]bool)
	for shard, shardMap := range frontier {
		shardIDs = append(shardIDs, shard)
		for party := range shardMap {
			partySet[party] = true
		}
	}
	sort.Slice(shardIDs, func(i, j int) bool {
		return int(shardIDs[i]) < int(shardIDs[j])
	})

	partyIDs := []types.PartyID{}
	for party := range partySet {
		partyIDs = append(partyIDs, party)
	}
	sort.Slice(partyIDs, func(i, j int) bool {
		return int(partyIDs[i]) < int(partyIDs[j])
	})

	sb := strings.Builder{}
	sb.WriteString("{")

	nShards := len(shardIDs)
	for _, shard := range shardIDs {
		shardMap := frontier[shard]
		sb.WriteString(fmt.Sprintf("Sh: %d, {", shard))

		nParties := len(shardMap)
		for _, party := range partyIDs {
			seq, ok := shardMap[party]
			if !ok {
				continue
			}

			sb.WriteString(fmt.Sprintf("<Pr: %d, Sq: %d>", party, seq))
			nParties--
			if nParties > 0 {
				sb.WriteString(", ")
			}
		}

		nShards--
		sb.WriteString("}")
		if nShards > 0 {
			sb.WriteString("; ")
		}
	}
	sb.WriteString("}")

	return sb.String()
}
