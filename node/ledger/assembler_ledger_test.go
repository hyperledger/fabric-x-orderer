/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger_test

import (
	"testing"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssemblerLedger_Create(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al := createAssemblerLedger(t, tmpDir, logger)
	defer al.Close()

	count := al.GetTxCount()
	assert.Equal(t, uint64(0), count)
}

// TestAssemblerLedger_Append append two blocks
func TestAssemblerLedger_Append(t *testing.T) {
	t.Run("three batches", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: utils.EmptyGenesisBlock("arma"),
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})
		assert.Equal(t, uint64(1), al.GetTxCount())
		assert.Equal(t, uint64(1), al.Ledger.Height())

		batches, ordInfos := createBatchesAndOrdInfo(t, 3)

		al.Append(batches[0], ordInfos[0])
		assert.Equal(t, uint64(3), al.GetTxCount())
		assert.Equal(t, uint64(2), al.Ledger.Height())

		al.Append(batches[1], ordInfos[1])
		assert.Equal(t, uint64(5), al.GetTxCount())
		assert.Equal(t, uint64(3), al.Ledger.Height())

		al.Append(batches[2], ordInfos[2])
		assert.Equal(t, uint64(7), al.GetTxCount())
		assert.Equal(t, uint64(4), al.Ledger.Height())
	})

	t.Run("duplicate block", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: utils.EmptyGenesisBlock("arma"),
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})
		assert.Equal(t, uint64(1), al.GetTxCount())
		assert.Equal(t, uint64(1), al.Ledger.Height())

		batches, ordInfos := createBatchesAndOrdInfo(t, 1)

		al.Append(batches[0], ordInfos[0])
		assert.Equal(t, uint64(3), al.GetTxCount())
		assert.Equal(t, uint64(2), al.Ledger.Height())

		// append the same batch again, should panic
		assert.Panics(t, func() { al.Append(batches[0], ordInfos[0]) })

		assert.Equal(t, uint64(3), al.GetTxCount())
		assert.Equal(t, uint64(2), al.Ledger.Height())
	})
}

func TestAssemblerLedger_ReadAndParse(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al := createAssemblerLedger(t, tmpDir, logger)
	defer al.Close()

	al.AppendConfig(&state.OrderingInformation{
		CommonBlock: utils.EmptyGenesisBlock("arma"),
		DecisionNum: 0,
		BatchIndex:  0,
		BatchCount:  1,
	})
	assert.Equal(t, uint64(1), al.GetTxCount())
	assert.Equal(t, uint64(1), al.Ledger.Height())

	batches, ordInfos := createBatchesAndOrdInfo(t, 2)

	al.Append(batches[0], ordInfos[0])
	al.Append(batches[1], ordInfos[1])
	assert.Equal(t, uint64(5), al.GetTxCount())
	assert.Equal(t, uint64(3), al.Ledger.Height())
	expectedTransactionCount := []int{1 + len(batches[0].Requests()), 1 + len(batches[0].Requests()) + len(batches[1].Requests())}

	for bn := uint64(0); bn < 3; bn++ {
		block, err := al.Ledger.RetrieveBlockByNumber(bn)
		assert.NoError(t, err)
		batchID, ordInfo, transactionCount, err := node_ledger.AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)
		assert.NoError(t, err)

		t.Logf("batchID: %v", batchID)
		t.Logf("ordInfo: %v", ordInfo)

		isConf := protoutil.IsConfigBlock(block)
		if bn == 0 {
			require.True(t, isConf)
			assert.Equal(t, types.ShardIDConsensus, batchID.Shard())
			assert.Equal(t, types.DecisionNum(0), ordInfo.DecisionNum)
			assert.Equal(t, uint64(1), transactionCount)
			continue
		} else {
			require.False(t, isConf)
		}

		n := bn - 1
		assert.Equal(t, batches[n].Digest(), batchID.Digest())
		assert.Equal(t, batches[n].Shard(), batchID.Shard())
		assert.Equal(t, batches[n].Seq(), batchID.Seq())
		assert.Equal(t, batches[n].Primary(), batchID.Primary())

		assert.Equal(t, protoutil.BlockHeaderHash(ordInfos[n].CommonBlock.Header), protoutil.BlockHeaderHash(ordInfo.CommonBlock.Header))
		assert.Equal(t, ordInfos[n].DecisionNum, ordInfo.DecisionNum)
		assert.Equal(t, ordInfos[n].BatchIndex, ordInfo.BatchIndex)
		assert.Equal(t, ordInfos[n].BatchCount, ordInfo.BatchCount)
		assert.Equal(t, ordInfos[n].Signatures, ordInfo.Signatures)
		assert.Equal(t, uint64(expectedTransactionCount[n]), transactionCount)
	}
}

func TestAssemblerLedger_LastOrderingInfo(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al := createAssemblerLedger(t, tmpDir, logger)
	defer al.Close()

	al.AppendConfig(&state.OrderingInformation{
		CommonBlock: utils.EmptyGenesisBlock("arma"),
		DecisionNum: 0,
		BatchIndex:  0,
		BatchCount:  1,
	})
	ordInfo, err := al.LastOrderingInfo()
	require.NoError(t, err)
	require.NotNil(t, ordInfo)
	assert.Equal(t, types.DecisionNum(0), ordInfo.DecisionNum)

	batches, ordInfos := createBatchesAndOrdInfo(t, 2)
	al.Append(batches[0], ordInfos[0])
	al.Append(batches[1], ordInfos[1])
	assert.Equal(t, uint64(5), al.GetTxCount())
	assert.Equal(t, uint64(3), al.Ledger.Height())

	ordInfo, err = al.LastOrderingInfo()
	require.NoError(t, err)

	assert.Equal(t, protoutil.BlockHeaderHash(ordInfos[1].CommonBlock.Header), protoutil.BlockHeaderHash(ordInfo.CommonBlock.Header))
	assert.Equal(t, ordInfos[1].DecisionNum, ordInfo.DecisionNum)
	assert.Equal(t, ordInfos[1].BatchIndex, ordInfo.BatchIndex)
	assert.Equal(t, ordInfos[1].BatchCount, ordInfo.BatchCount)
	assert.Equal(t, ordInfos[1].Signatures, ordInfo.Signatures)
}

func TestAssemblerLedger_BatchFrontier(t *testing.T) {
	t.Run("covers all shards and parties", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: utils.EmptyGenesisBlock("arma"),
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})

		num := 128
		batches, ordInfos := createBatchesAndOrdInfo(t, num)

		for n := 0; n < num; n++ {
			al.Append(batches[n], ordInfos[n])
		}

		assert.Equal(t, uint64(1+num*2), al.GetTxCount())
		assert.Equal(t, uint64(1+num), al.Ledger.Height())

		bf, err := al.BatchFrontier([]types.ShardID{1, 2, 3, 4, 5, 6, 7, 8}, []types.PartyID{1, 2, 3, 4}, time.Hour)
		assert.NoError(t, err)
		assert.Len(t, bf, 8) // every shard
		for _, bfs := range bf {
			assert.Len(t, bfs, 4) // every party
			for _, seq := range bfs {
				assert.Equal(t, types.BatchSequence(3), seq)
			}
		}
	})

	t.Run("empty ledger", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: utils.EmptyGenesisBlock("arma"),
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})

		assert.Equal(t, uint64(1), al.GetTxCount())
		assert.Equal(t, uint64(1), al.Ledger.Height())

		bf, err := al.BatchFrontier([]types.ShardID{1, 2, 3, 4, 5, 6, 7, 8}, []types.PartyID{1, 2, 3, 4}, time.Hour)
		assert.NoError(t, err)
		assert.Len(t, bf, 0)
	})

	t.Run("stops at block 0", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: utils.EmptyGenesisBlock("arma"),
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})

		num := 8
		batches, ordInfos := createBatchesAndOrdInfo(t, num)

		for n := 0; n < num; n++ {
			al.Append(batches[n], ordInfos[n])
		}
		assert.Equal(t, uint64(9), al.Ledger.Height())

		bf, err := al.BatchFrontier([]types.ShardID{1, 2, 3, 4, 5, 6, 7, 8}, []types.PartyID{1, 2, 3, 4}, time.Hour)
		assert.NoError(t, err)
		assert.Len(t, bf, 8) // every shard
		for _, bfs := range bf {
			assert.Len(t, bfs, 1) // only one party
			for _, seq := range bfs {
				assert.Equal(t, types.BatchSequence(0), seq)
			}
		}
	})

	t.Run("respects the timeout", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: utils.EmptyGenesisBlock("arma"),
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})

		num := 10
		batches, ordInfos := createBatchesAndOrdInfo(t, num)

		for n := 0; n < num; n++ {
			al.Append(batches[n], ordInfos[n])
		}
		assert.Equal(t, uint64(1+num), al.Ledger.Height())

		bf, err := al.BatchFrontier([]types.ShardID{1, 2, 3, 4, 5, 6, 7, 8}, []types.PartyID{1, 2, 3, 4}, time.Nanosecond)
		assert.NoError(t, err)
		assert.Len(t, bf, 1) // just one block before the deadline
		for _, bfs := range bf {
			assert.Len(t, bfs, 1) // only one party
		}
	})
}

func TestAssemblerLedger_LastConfig(t *testing.T) {
	t.Run("last config after AppendConfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		genBlock := utils.EmptyGenesisBlock("arma")
		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: genBlock,
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})

		idx, err := node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(0), idx)

		confBlock1 := prepareConfigBlock(1, genBlock)
		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: confBlock1,
			DecisionNum: 1,
			BatchIndex:  0,
			BatchCount:  1,
		})
		idx, err = node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(1), idx)

		confBlock2 := prepareConfigBlock(2, confBlock1)
		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: confBlock2,
			DecisionNum: 2,
			BatchIndex:  0,
			BatchCount:  1,
		})
		idx, err = node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(2), idx)
	})

	t.Run("last config after Append", func(t *testing.T) {
		tmpDir := t.TempDir()
		logger := flogging.MustGetLogger("arma-assembler")

		al := createAssemblerLedger(t, tmpDir, logger)
		defer al.Close()

		genBlock := utils.EmptyGenesisBlock("arma")
		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: genBlock,
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		})

		idx, err := node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(0), idx)
		block1 := prepareBlockWithLastConfig(1, 0, genBlock)

		ordInfo1 := &state.OrderingInformation{
			CommonBlock: block1,
		}
		batch1 := node_ledger.NewFabricBatchFromRequests(2, 1, 3, types.BatchedRequests{
			[]byte("tx1"), []byte("tx2"),
		}, 0, []byte(""))

		al.Append(batch1, ordInfo1)
		idx, err = node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(0), idx)

		confBlock1 := prepareConfigBlock(2, block1)
		al.AppendConfig(&state.OrderingInformation{
			CommonBlock: confBlock1,
			DecisionNum: 2,
			BatchIndex:  0,
			BatchCount:  1,
		})
		idx, err = node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(2), idx)

		block2 := prepareBlockWithLastConfig(3, 2, confBlock1)
		ordInfo2 := &state.OrderingInformation{
			CommonBlock: block2,
		}
		batch2 := node_ledger.NewFabricBatchFromRequests(2, 1, 3, types.BatchedRequests{
			[]byte("tx1"), []byte("tx2"),
		}, 0, []byte(""))

		al.Append(batch2, ordInfo2)
		idx, err = node_ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)
		require.Equal(t, uint64(2), idx)
	})
}

func createAssemblerLedger(t *testing.T, tmpDir string, logger *flogging.FabricLogger) *node_ledger.AssemblerLedger {
	al, err := node_ledger.NewAssemblerLedger(logger, tmpDir)
	require.NoError(t, err)
	require.NotNil(t, al)
	al.Metrics().NewAssemblerLedgerMetrics(monitoring.NewMonitor(monitoring.Endpoint{Host: "127.0.0.1", Port: 0}, "TestAssemblerWithLastConfigBlock").Provider, "party1", logger)
	return al
}

// createBatchesAndOrdInfo creates a series of batches and their corresponding ordering information, emulating the
// output of consensus and including the batches retrieved from the batchers by the assembler.
// When generating batches, we try to cover every <shard, primary> with a batches.
// Assuming 4 parties (1-4) and 8 shards (1-8).
func createBatchesAndOrdInfo(t *testing.T, num int) ([]types.Batch, []*state.OrderingInformation) {
	var batches []types.Batch
	var ordInfos []*state.OrderingInformation
	transactionCount := 0

	// this 2D matrix holds the last batch-sequence of every [shard][primary]
	seqArray := make([][]uint64, 8)
	for s := 0; s < 8; s++ {
		seqArray[s] = make([]uint64, 4)
	}

	for n := uint64(0); n < uint64(num); n++ {
		batchedRequests := types.BatchedRequests{
			[]byte{1, 2, 3, 4, byte(n)}, []byte{5, 6, 7, 8, byte(n)},
		}

		// deal a batch on every shard
		sIdx := n % 8
		shard := types.ShardID(sIdx + 1)
		// every |shards| change primary
		pIdx := (n / 8) % 4
		party := types.PartyID(pIdx + 1)
		// on each <shard,primary> the sequence increases by +1 increments
		seq := seqArray[sIdx][pIdx]
		seqArray[sIdx][pIdx] = seq + 1

		fb := node_ledger.NewFabricBatchFromRequests(shard, party, types.BatchSequence(seq), batchedRequests, 0, nil)
		require.NotNil(t, fb)

		transactionCount += len(batchedRequests)
		ordInfo := &state.OrderingInformation{
			CommonBlock: &common.Block{Header: &common.BlockHeader{Number: n + 1, DataHash: fb.Digest()}},
			Signatures: []smartbft_types.Signature{{
				ID:    1,
				Value: []byte("sig1"),
			}, {
				ID:    2,
				Value: []byte("sig2"),
			}},
			DecisionNum: types.DecisionNum(3 + n),
			BatchIndex:  0,
			BatchCount:  1,
		}
		protoutil.InitBlockMetadata(ordInfo.CommonBlock)

		ordererBlockMetadata, err := node_ledger.AssemblerBlockMetadataToBytes(fb, ordInfo, uint64(transactionCount+len(fb.Requests())))
		require.NoError(t, err)
		ordInfo.CommonBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = ordererBlockMetadata

		if n > 0 {
			ordInfo.CommonBlock.Header.PreviousHash = protoutil.BlockHeaderHash(ordInfos[n-1].CommonBlock.Header)
		} else {
			genesis := utils.EmptyGenesisBlock("arma")
			ordInfo.CommonBlock.Header.PreviousHash = protoutil.BlockHeaderHash(genesis.Header)
		}

		batches = append(batches, fb)
		ordInfos = append(ordInfos, ordInfo)
	}

	return batches, ordInfos
}

func prepareConfigBlock(blockNumber uint64, prevBlock *common.Block) *common.Block {
	configBlock := tx.CreateConfigBlock(blockNumber, []byte{1})
	protoutil.InitBlockMetadata(configBlock)
	configBlock.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: configBlock.Header.Number}),
	})

	if prevBlock != nil {
		configBlock.Header.PreviousHash = protoutil.BlockHeaderHash(prevBlock.Header)
	}
	return configBlock
}

func prepareBlockWithLastConfig(blockNumber uint64, lastConfigIndex uint64, prevBlock *common.Block) *common.Block {
	block := &common.Block{Header: &common.BlockHeader{Number: blockNumber}}
	protoutil.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: lastConfigIndex}),
	})
	if prevBlock != nil {
		block.Header.PreviousHash = protoutil.BlockHeaderHash(prevBlock.Header)
	}
	return block
}
