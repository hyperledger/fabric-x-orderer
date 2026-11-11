/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
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

// appendDecisions appends count chained decision blocks to consLedger. Each decision names
// decisionNumOfLastConfigBlock as the decision of the last config block, or itself if it comes
// before that decision, as the genesis decision does.
func appendDecisions(t *testing.T, consLedger *ledger.ConsensusLedger, count uint64, decisionNumOfLastConfigBlock types.DecisionNum) {
	t.Helper()

	var prevHash []byte
	for num := uint64(0); num < count; num++ {
		lastConfig := min(types.DecisionNum(num), decisionNumOfLastConfigBlock)
		header := &state.Header{
			Num:                          types.DecisionNum(num),
			DecisionNumOfLastConfigBlock: lastConfig,
		}
		proposal := smartbft_types.Proposal{Header: header.Serialize()}
		block := state.CreateBlockToAppendFromDecision(num, proposal, nil, prevHash, uint64(lastConfig))
		consLedger.Append(block)
		prevHash = protoutil.BlockHeaderHash(block.Header)
	}
}

func TestGetInitialStateAndMetadataDecisionNumOfLastConfigBlock(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	for _, tc := range []struct {
		name string
		// The ledger holds decisions 0 to ledgerHeight-1, and its last config block is in
		// decision ledgerConfigDecision.
		ledgerHeight         uint64
		ledgerConfigDecision types.DecisionNum
		lastConfigBlock      *common.Block
		expected             types.DecisionNum
	}{
		{
			// The node was down across a config change and starts from the new config block, which
			// is in decision 24. Its ledger still points to decision 1.
			name:                 "rejoin from a config block ahead of the ledger",
			ledgerHeight:         3,
			ledgerConfigDecision: 1,
			lastConfigBlock:      makeConfigBlockWithDecisionNum(t, 12, 24),
			expected:             24,
		},
		{
			name:                 "rejoin from a config block in the next decision",
			ledgerHeight:         3,
			ledgerConfigDecision: 1,
			lastConfigBlock:      makeConfigBlockWithDecisionNum(t, 12, 3),
			expected:             3,
		},
		{
			name:                 "restart from a config block in the last decision of the ledger",
			ledgerHeight:         3,
			ledgerConfigDecision: 2,
			lastConfigBlock:      makeConfigBlockWithDecisionNum(t, 5, 2),
			expected:             2,
		},
		{
			name:                 "restart from an earlier config block in the ledger",
			ledgerHeight:         3,
			ledgerConfigDecision: 1,
			lastConfigBlock:      makeConfigBlockWithDecisionNum(t, 5, 1),
			expected:             1,
		},
		{
			name:                 "restart from the genesis block",
			ledgerHeight:         3,
			ledgerConfigDecision: 0,
			lastConfigBlock:      &common.Block{Header: &common.BlockHeader{Number: 0}},
			expected:             0,
		},
		{
			name:            "join with an empty ledger",
			ledgerHeight:    0,
			lastConfigBlock: makeConfigBlockWithDecisionNum(t, 12, 24),
			expected:        24,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			consLedger, err := ledger.NewConsensusLedger(t.TempDir())
			require.NoError(t, err)
			defer consLedger.Close()
			appendDecisions(t, consLedger, tc.ledgerHeight, tc.ledgerConfigDecision)

			_, _, _, _, decisionNumOfLastConfigBlock, _ := getInitialStateAndMetadata(logger, &node_config.ConsenterNodeConfig{}, tc.lastConfigBlock, consLedger)
			require.Equal(t, tc.expected, decisionNumOfLastConfigBlock)
		})
	}
}
