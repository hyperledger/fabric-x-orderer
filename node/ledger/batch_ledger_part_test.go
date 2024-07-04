package ledger

import (
	arma "arma/pkg"
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/stretchr/testify/require"
)

func TestBatchLedgerPart(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(dir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	require.NoError(t, err)

	part, err := newBatchLedgerPart(provider, 1, 1, 2, logger)
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, uint64(0), part.Height())
	require.Nil(t, part.RetrieveBatchByNumber(0))

	for seq := uint64(0); seq < 10; seq++ {
		batchedRequests := arma.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		part.Append(seq, batchedRequests.ToBytes())
		require.Equal(t, seq+1, part.Height())
		batch := part.RetrieveBatchByNumber(seq)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, arma.PartyID(2), batch.Party())
		require.NotNil(t, batch.Digest())
	}
	require.Nil(t, part.RetrieveBatchByNumber(100))
}

func TestBatchLedgerPart_Iterator(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(dir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	require.NoError(t, err)

	part, err := newBatchLedgerPart(provider, 1, 1, 2, logger)
	require.NoError(t, err)
	require.NotNil(t, part)

	for seq := uint64(0); seq < 10; seq++ {
		batchedRequests := arma.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		part.Append(seq, batchedRequests.ToBytes())
	}

	ledger := part.Ledger()
	require.NotNil(t, ledger)

	pos := &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: 5}}}
	it, seq := ledger.Iterator(pos)
	require.NotNil(t, it)
	require.Equal(t, uint64(5), seq)
	defer it.Close()

	block, _ := it.Next()
	require.Equal(t, uint64(5), block.GetHeader().GetNumber())
	block, _ = it.Next()
	require.Equal(t, uint64(6), block.GetHeader().GetNumber())
}
