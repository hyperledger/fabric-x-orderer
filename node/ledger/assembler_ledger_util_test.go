/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestAssemblerBlockMetadataToFromBytes(t *testing.T) {
	batchedRequests := types.BatchedRequests{
		[]byte("tx1-1"), []byte("tx2"),
	}

	fb := node_ledger.NewFabricBatchFromRequests(2, 1, 3, batchedRequests, 0, []byte("bogus"))
	assert.NotNil(t, fb)

	oi := &state.OrderingInformation{
		BlockHeader: &state.BlockHeader{
			Number:   0,
			PrevHash: []byte{},
			Digest:   fb.Digest(),
		},
		Signatures: []smartbft_types.Signature{{
			ID:    1,
			Value: []byte("sig1"),
		}, {
			ID:    2,
			Value: []byte("sig2"),
		}},
		DecisionNum: 4,
		BatchIndex:  5,
		BatchCount:  6,
	}

	expectedTransactionCount := uint64(len(batchedRequests))

	metadata, err := node_ledger.AssemblerBlockMetadataToBytes(fb, oi, expectedTransactionCount)
	assert.NoError(t, err)
	primary, shard, seq, num, batchIndex, batchCount, transactionCount, err := node_ledger.AssemblerBlockMetadataFromBytes(metadata)
	assert.NoError(t, err)
	assert.Equal(t, types.PartyID(1), primary)
	assert.Equal(t, types.ShardID(2), shard)
	assert.Equal(t, types.BatchSequence(3), seq)
	assert.Equal(t, oi.DecisionNum, num)
	assert.Equal(t, uint32(oi.BatchIndex), batchIndex)
	assert.Equal(t, uint32(oi.BatchCount), batchCount)
	assert.Equal(t, expectedTransactionCount, transactionCount)
}

func TestBatchFrontierToString(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		bf := make(node_ledger.BatchFrontier)
		assert.Equal(t, "{}", node_ledger.BatchFrontierToString(bf))
	})

	t.Run("partial map", func(t *testing.T) {
		bf := make(node_ledger.BatchFrontier)
		seq := types.BatchSequence(101)
		for shardID := types.ShardID(1); shardID < 4; shardID++ {
			bf[shardID] = make(node_ledger.PartySequenceMap)

			for partyID := types.PartyID(11); partyID < 15; partyID++ {
				if shardID == 3 {
					continue
				}
				if partyID == types.PartyID(12) {
					continue
				}

				bf[shardID][partyID] = seq
				seq++
			}
		}
		assert.Equal(t, "{Sh: 1, {<Pr: 11, Sq: 101>, <Pr: 13, Sq: 102>, <Pr: 14, Sq: 103>}; Sh: 2, {<Pr: 11, Sq: 104>, <Pr: 13, Sq: 105>, <Pr: 14, Sq: 106>}; Sh: 3, {}}",
			node_ledger.BatchFrontierToString(bf))
	})

	t.Run("full map", func(t *testing.T) {
		bf := make(node_ledger.BatchFrontier)
		seq := types.BatchSequence(101)
		for shardID := types.ShardID(1); shardID < 3; shardID++ {
			bf[shardID] = make(node_ledger.PartySequenceMap)

			for partyID := types.PartyID(11); partyID < 15; partyID++ {
				bf[shardID][partyID] = seq
				seq++
			}
		}
		assert.Equal(t, "{Sh: 1, {<Pr: 11, Sq: 101>, <Pr: 12, Sq: 102>, <Pr: 13, Sq: 103>, <Pr: 14, Sq: 104>}; Sh: 2, {<Pr: 11, Sq: 105>, <Pr: 12, Sq: 106>, <Pr: 13, Sq: 107>, <Pr: 14, Sq: 108>}}",
			node_ledger.BatchFrontierToString(bf))
	})
}
