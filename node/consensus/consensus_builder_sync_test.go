/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/stretchr/testify/require"
)

// makeConfigBlockWithDecisionNum builds a config block whose ORDERER metadata encodes the given
// decision number, so that ledger.AssemblerBatchIdOrderingInfoAndTxCountFromBlock can recover it.
func makeConfigBlockWithDecisionNum(t *testing.T, blockNumber uint64, decisionNum types.DecisionNum) *common.Block {
	t.Helper()

	batchedRequests := types.BatchedRequests{[]byte("tx1"), []byte("tx2")}
	fb := ledger.NewFabricBatchFromRequests(1, 1, 1, batchedRequests, batchedRequests.Digest(), 0, []byte("prev"), nil)
	require.NotNil(t, fb)

	oi := &state.OrderingInformation{
		CommonBlock: &common.Block{
			Header: &common.BlockHeader{Number: blockNumber, DataHash: fb.Digest()},
		},
		DecisionNum: decisionNum,
		BatchIndex:  0,
		BatchCount:  1,
	}

	mdBytes, err := ledger.AssemblerBlockMetadataToBytes(fb, oi, uint64(len(batchedRequests)))
	require.NoError(t, err)

	block := &common.Block{
		Header: &common.BlockHeader{Number: blockNumber, DataHash: fb.Digest()},
		Data:   &common.BlockData{Data: [][]byte{[]byte("data")}},
		Metadata: &common.BlockMetadata{
			Metadata: make([][]byte, common.BlockMetadataIndex_ORDERER+1),
		},
	}
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = mdBytes
	return block
}

func TestShouldSyncOnStart(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	t.Run("fresh join with empty ledger and non-genesis config block requires sync", func(t *testing.T) {
		// A brand-new node joins from a join config block (number > 0) with an empty ledger.
		joinConfigBlock := makeConfigBlockWithDecisionNum(t, 5, 5)
		require.True(t, shouldSyncOnStart(logger, joinConfigBlock, 0))
	})

	t.Run("rejoin from a more-advanced config block requires sync", func(t *testing.T) {
		// Party rejoins with a non-empty stale ledger (height 14) but boots from a join config
		// block whose decision number (24) is ahead of the ledger height. This is the flaky
		// TestConsensusFullReplacement scenario: without sync the node would resume as a stale
		// leader of its old view and never catch up.
		joinConfigBlock := makeConfigBlockWithDecisionNum(t, 12, 24)
		require.True(t, shouldSyncOnStart(logger, joinConfigBlock, 14))
	})

	t.Run("rejoin behind by exactly one decision requires sync", func(t *testing.T) {
		// The ledger height is 14, meaning the node has committed decisions up to number 13
		// (height == last committed decision + 1). A join config block at decision 14 is the very
		// next decision the node has NOT yet committed, so it is behind and must sync. This is the
		// boundary between "behind" and "current".
		joinConfigBlock := makeConfigBlockWithDecisionNum(t, 12, 14)
		require.True(t, shouldSyncOnStart(logger, joinConfigBlock, 14))
	})

	t.Run("up-to-date node recovering from its own ledger does not sync", func(t *testing.T) {
		// The node has committed decisions up to 13 (height 14). The join config block's decision
		// number (13) is one the node already has, so it is current and must not force a sync.
		joinConfigBlock := makeConfigBlockWithDecisionNum(t, 12, 13)
		require.False(t, shouldSyncOnStart(logger, joinConfigBlock, 14))
	})

	t.Run("genesis start does not sync", func(t *testing.T) {
		// Starting from the genesis config block (number 0) with an empty ledger: no sync.
		genesisBlock := &common.Block{Header: &common.BlockHeader{Number: 0}}
		require.False(t, shouldSyncOnStart(logger, genesisBlock, 0))
	})

	t.Run("panics when the join config block ordering info cannot be read", func(t *testing.T) {
		// A non-genesis config block whose ordering metadata is missing/corrupt is unexpected and
		// must not be silently ignored.
		corruptBlock := &common.Block{Header: &common.BlockHeader{Number: 12}}
		require.Panics(t, func() {
			shouldSyncOnStart(logger, corruptBlock, 14)
		})
	})
}
