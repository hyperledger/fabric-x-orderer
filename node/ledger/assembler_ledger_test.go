package ledger_test

import (
	"testing"

	"arma/common/types"
	"arma/node/consensus/state"
	node_ledger "arma/node/ledger"

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

func TestAssemblerLedge_Append(t *testing.T) {
	tmpDir := t.TempDir()
	logger := flogging.MustGetLogger("arma-assembler")

	al, err := createAssemblerLedger(tmpDir, logger)
	require.NoError(t, err)
	go al.TrackThroughput()

	count := al.GetTxCount()
	assert.Equal(t, uint64(0), count)

	batchedRequests := types.BatchedRequests{
		[]byte("tx1-1"), []byte("tx2"),
	}
	batchBytes := batchedRequests.Serialize()

	fb, err := node_ledger.NewFabricBatchFromRaw(1, 2, 3, batchBytes, []byte("prev-hash"))
	assert.NoError(t, err)
	ba := state.NewAvailableBatch(1, 2, 3, fb.Digest())

	al.Append(0, fb, &ba)
	count = al.GetTxCount()
	assert.Equal(t, uint64(2), count)
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
