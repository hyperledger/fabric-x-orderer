/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	assembler_mocks "github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

type naiveOrderedBatchAttestationReplicator chan *state.AvailableBatchOrdered

func (n naiveOrderedBatchAttestationReplicator) Replicate() <-chan *state.AvailableBatchOrdered {
	return n
}

type naiveIndex struct {
	sync.Map
}

func (n *naiveIndex) Put(batch types.Batch) error {
	n.Store(string(batch.Digest()), batch)
	return nil
}

func (n *naiveIndex) PopOrWait(batchId types.BatchID) (types.Batch, error) {
	for {
		val, exists := n.Load(string(batchId.Digest()))

		if !exists {
			time.Sleep(time.Millisecond)
			continue
		}
		defer func() {
			n.Delete(string(batchId.Digest()))
		}()
		return val.(types.Batch), nil
	}
}

func (n *naiveIndex) Stop() {}

func TestCollator_Batches(t *testing.T) {
	shardCount := 4
	batchNum := 20
	primaryID := types.PartyID(1)

	// create test batches from all shards
	digestsSet, batches := createTestBatches(t, shardCount, batchNum, primaryID)
	// create test setup
	index, ledger, ordBARep, collator := createCollator(t, shardCount, &assembler_mocks.FakeAssemblerRestarter{})

	collator.Run()

	totalOrder := make(chan *state.AvailableBatchOrdered)

	genBlock, err := ledger.LedgerReader().RetrieveBlockByNumber(0)
	prevBlock := genBlock
	require.NoError(t, err)
	require.True(t, protoutil.IsConfigBlock(prevBlock), "ledger includes an empty genesis block")
	decNum := types.DecisionNum(1)

	simulateDecisions(t, batches, index, prevBlock, decNum, totalOrder)

	go func() {
		for ordererBatchAttestation := range totalOrder {
			ordBARep <- ordererBatchAttestation
		}
	}()

	require.Eventually(t, func() bool {
		return ledger.LedgerReader().Height() == uint64(shardCount)*uint64(batchNum)+1
	}, 10*time.Second, 100*time.Millisecond)

	// verify all batches received
	for blockNUm := uint64(0); blockNUm <= uint64(batchNum*shardCount); blockNUm++ {
		block, err := ledger.LedgerReader().RetrieveBlockByNumber(blockNUm)
		require.NoError(t, err)
		if blockNUm == 0 {
			require.True(t, protoutil.IsConfigBlock(block), "ledger includes an empty genesis block")
			continue
		}

		batchID, ordInfo, _, _ := node_ledger.AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)

		expectedBatch := batches[batchID.Shard()][batchID.Seq()]
		require.Equal(t, types.BatchIDToString(expectedBatch), types.BatchIDToString(batchID))
		require.Contains(t, ordInfo.String(), fmt.Sprintf("DecisionNum: %d, BatchIndex: 0, BatchCount: 1; No. Sigs: 0, Common Block: Number: %d", blockNUm, blockNUm), ordInfo.String())

		delete(digestsSet, string(batchID.Digest()))
	}

	require.Len(t, digestsSet, 0)
}

func TestCollator_Config(t *testing.T) {
	shardCount := 4
	batchNum := 3
	primaryID := types.PartyID(1)

	// create test batches from all shards
	_, batches := createTestBatches(t, shardCount, batchNum, primaryID)
	// create test setup
	restarter := &assembler_mocks.FakeAssemblerRestarter{}
	index, ledger, ordBARep, collator := createCollator(t, shardCount, restarter)

	collator.Run()

	totalOrder := make(chan *state.AvailableBatchOrdered)

	genBlock, err := ledger.LedgerReader().RetrieveBlockByNumber(0)
	prevBlock := genBlock
	require.NoError(t, err)
	require.True(t, protoutil.IsConfigBlock(prevBlock), "ledger includes an empty genesis block")
	decNum := types.DecisionNum(1)

	simulateDecisions(t, batches, index, prevBlock, decNum, totalOrder)

	go func() {
		for ordererBatchAttestation := range totalOrder {
			ordBARep <- ordererBatchAttestation
		}
	}()

	require.Eventually(t, func() bool {
		return ledger.LedgerReader().Height() == uint64(shardCount)*uint64(batchNum)+1
	}, 10*time.Second, 100*time.Millisecond)

	// consensus emits a config decision
	lastBlock, err := ledger.LedgerReader().RetrieveBlockByNumber(ledger.LedgerReader().Height() - 1)
	require.NoError(t, err)
	configBlock := protoutil.UnmarshalBlockOrPanic(protoutil.MarshalOrPanic(genBlock)) // clone the block
	configBlock.Header.PreviousHash = protoutil.BlockHeaderHash(lastBlock.Header)
	configBlock.Header.Number = lastBlock.Header.Number + 1

	configABO := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(0, types.ShardIDConsensus, 0, []byte{}),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: configBlock,
			DecisionNum: decNum,
			BatchIndex:  0,
			BatchCount:  1,
		},
	}
	totalOrder <- configABO

	require.Eventually(t, func() bool {
		return ledger.LedgerReader().Height() == uint64(shardCount)*uint64(batchNum)+2
	}, 10*time.Second, 100*time.Millisecond)

	lastBlock, err = ledger.LedgerReader().RetrieveBlockByNumber(ledger.LedgerReader().Height() - 1)
	require.NoError(t, err)
	require.True(t, protoutil.IsConfigBlock(lastBlock))

	require.Eventually(t, func() bool {
		return restarter.SoftStopCallCount() == 1
	}, 10*time.Second, 100*time.Millisecond)
}

func simulateDecisions(
	t *testing.T,
	batches [][]types.Batch,
	index *naiveIndex,
	prevBlock *common.Block,
	decNum types.DecisionNum,
	totalOrder chan *state.AvailableBatchOrdered,
) {
	go func() {
		for shardID := 0; shardID < len(batches); shardID++ {
			for _, batch := range batches[shardID] {

				index.Put(batch)

				ab := state.NewAvailableBatch(batch.Primary(), batch.Shard(), batch.Seq(), batch.Digest())
				block := protoutil.NewBlock(prevBlock.Header.Number+1, protoutil.BlockHeaderHash(prevBlock.Header))
				block.Header.DataHash = ab.Digest()

				abo := &state.AvailableBatchOrdered{
					AvailableBatch: ab,
					OrderingInformation: &state.OrderingInformation{
						CommonBlock: block,
						DecisionNum: decNum,
						BatchIndex:  0,
						BatchCount:  1,
					},
				}
				totalOrder <- abo

				prevBlock = block
				decNum++
			}
		}

		t.Logf("Simulated %d decisions", decNum)
	}()
}

func createTestBatches(t *testing.T, shardCount int, batchNum int, primaryID types.PartyID) (map[string]bool, [][]types.Batch) {
	digestsSet := make(map[string]bool)
	var batches [][]types.Batch
	for shardID := types.ShardID(0); shardID < types.ShardID(shardCount); shardID++ {
		var batchesForShard []types.Batch
		for seq := types.BatchSequence(0); seq < types.BatchSequence(batchNum); seq++ {
			buff := generateRandomBytes(t, 1024)
			batch := types.NewSimpleBatch(shardID, primaryID, seq, [][]byte{buff}, 0)
			digestsSet[string(batch.Digest())] = true
			batchesForShard = append(batchesForShard, batch)
		}
		batches = append(batches, batchesForShard)
	}

	return digestsSet, batches
}

func createCollator(t *testing.T, shardCount int, AssemblerRestarter assembler.AssemblerRestarter) (*naiveIndex, node_ledger.AssemblerLedgerReaderWriter, naiveOrderedBatchAttestationReplicator, *assembler.Collator) {
	tempDir := t.TempDir()

	logger := testutil.CreateLogger(t, 0)

	index := &naiveIndex{}

	var shards []types.ShardID
	for i := 0; i < shardCount; i++ {
		shards = append(shards, types.ShardID(i))
	}

	ledgerFactory := &node_ledger.DefaultAssemblerLedgerFactory{}
	ledger, err := ledgerFactory.Create(logger, tempDir)
	require.NoError(t, err)

	ledger.Metrics().NewAssemblerLedgerMetrics(monitoring.NewMonitor(monitoring.Endpoint{Host: "127.0.0.1", Port: 0}, t.Name()).Provider, "test_party", testutil.CreateLogger(t, 0))

	ledger.AppendConfig(&state.OrderingInformation{
		CommonBlock: utils.EmptyGenesisBlock("test"),
		DecisionNum: 0,
		BatchIndex:  0,
		BatchCount:  1,
	})

	ordBARep := make(naiveOrderedBatchAttestationReplicator)

	collator := &assembler.Collator{
		Shards:                            shards,
		Logger:                            testutil.CreateLogger(t, 0),
		Ledger:                            ledger,
		ShardCount:                        shardCount,
		OrderedBatchAttestationReplicator: ordBARep,
		Index:                             index,
		AssemblerRestarter:                AssemblerRestarter,
	}
	return index, ledger, ordBARep, collator
}
