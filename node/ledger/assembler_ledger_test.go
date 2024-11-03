package ledger_test

import (
	"testing"

	"arma/common/types"
	"arma/core"
	"arma/node/consensus/state"
	node_ledger "arma/node/ledger"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssemblerLedge_Create(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al, err := createAssemblerLedger(tmpDir, logger)
	require.NoError(t, err)
	go al.TrackThroughput()

	count := al.GetTxCount()
	assert.Equal(t, uint64(0), count)
}

// TestAssemblerLedge_Append append two blocks
func TestAssemblerLedge_Append(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al, err := createAssemblerLedger(tmpDir, logger)
	require.NoError(t, err)
	go al.TrackThroughput()

	t.Run("three batches", func(t *testing.T) {
		batches, ordInfos := createBatchesAndOrdInfo(t, 3)

		al.Append(batches[0], ordInfos[0])
		assert.Equal(t, uint64(2), al.GetTxCount())
		assert.Equal(t, uint64(1), al.Ledger.Height())

		al.Append(batches[1], ordInfos[1])
		assert.Equal(t, uint64(4), al.GetTxCount())
		assert.Equal(t, uint64(2), al.Ledger.Height())

		al.Append(batches[2], ordInfos[2])
		assert.Equal(t, uint64(6), al.GetTxCount())
		assert.Equal(t, uint64(3), al.Ledger.Height())
	})
}

func TestAssemblerLedge_ReadAndParse(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al, err := createAssemblerLedger(tmpDir, logger)
	require.NoError(t, err)
	go al.TrackThroughput()

	batches, ordInfos := createBatchesAndOrdInfo(t, 2)

	al.Append(batches[0], ordInfos[0])
	al.Append(batches[1], ordInfos[1])
	assert.Equal(t, uint64(4), al.GetTxCount())
	assert.Equal(t, uint64(2), al.Ledger.Height())

	for n := uint64(0); n < 2; n++ {
		block, err := al.Ledger.RetrieveBlockByNumber(n)
		assert.NoError(t, err)
		batchID, ordInfo, err := node_ledger.AssemblerBatchIdOrderingInfoFromBlock(block)
		assert.NoError(t, err)

		assert.Equal(t, batches[n].Digest(), batchID.Digest())
		assert.Equal(t, batches[n].Shard(), batchID.Shard())
		assert.Equal(t, batches[n].Seq(), batchID.Seq())
		assert.Equal(t, batches[n].Primary(), batchID.Primary())

		assert.Equal(t, ordInfos[n].Hash(), ordInfo.Hash())
		assert.Equal(t, ordInfos[n].DecisionNum, ordInfo.DecisionNum)
		assert.Equal(t, ordInfos[n].BatchIndex, ordInfo.BatchIndex)
		assert.Equal(t, ordInfos[n].BatchCount, ordInfo.BatchCount)
		assert.Equal(t, ordInfos[n].Signatures, ordInfo.Signatures)
	}
}

func TestAssemblerLedger_LastOrderingInfo(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al, err := createAssemblerLedger(tmpDir, logger)
	require.NoError(t, err)
	go al.TrackThroughput()

	batches, ordInfos := createBatchesAndOrdInfo(t, 2)

	al.Append(batches[0], ordInfos[0])
	al.Append(batches[1], ordInfos[1])
	assert.Equal(t, uint64(4), al.GetTxCount())
	assert.Equal(t, uint64(2), al.Ledger.Height())

	ordInfo, err := al.LastOrderingInfo()
	require.NoError(t, err)

	assert.Equal(t, ordInfos[1].Hash(), ordInfo.Hash())
	assert.Equal(t, ordInfos[1].DecisionNum, ordInfo.DecisionNum)
	assert.Equal(t, ordInfos[1].BatchIndex, ordInfo.BatchIndex)
	assert.Equal(t, ordInfos[1].BatchCount, ordInfo.BatchCount)
	assert.Equal(t, ordInfos[1].Signatures, ordInfo.Signatures)
}

func createAssemblerLedger(tmpDir string, logger *flogging.FabricLogger) (*node_ledger.AssemblerLedger, error) {
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(tmpDir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		return nil, err
	}

	armaBlockStore, err := provider.Open("arma")
	if err != nil {
		return nil, err
	}

	armaFileLedger := fileledger.NewFileLedger(armaBlockStore)

	al := &node_ledger.AssemblerLedger{Ledger: armaFileLedger, Logger: logger}
	return al, nil
}

func createBatchesAndOrdInfo(t *testing.T, num int) ([]core.Batch, []*state.OrderingInformation) {
	var batches []core.Batch
	var ordInfos []*state.OrderingInformation

	for n := uint64(0); n < uint64(num); n++ {
		batchedRequests := types.BatchedRequests{
			[]byte{1, 2, 3, 4, byte(n)}, []byte{5, 6, 7, 8, byte(n)},
		}
		batchBytes := batchedRequests.Serialize()

		fb, err := node_ledger.NewFabricBatchFromRaw(1, 2, n+3, batchBytes, nil)
		require.NoError(t, err)

		oi := &state.OrderingInformation{
			BlockHeader: &state.BlockHeader{
				Number:   n,
				PrevHash: nil,
				Digest:   fb.Digest(),
			},
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
		if n > 0 {
			oi.BlockHeader.PrevHash = ordInfos[n-1].Hash()
		}

		batches = append(batches, fb)
		ordInfos = append(ordInfos, oi)
	}

	return batches, ordInfos
}
