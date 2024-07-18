package ledger

import (
	"fmt"
	"testing"

	arma "arma/pkg"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/stretchr/testify/require"
)

func TestNewBatchLedgerArray(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []arma.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	list, err := a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1", "shard1party2", "shard1party3", "shard1party4"}, list)

	a.Close()
}

func TestBatchLedgerArray(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	t.Log("Open, write & read")
	parties := []arma.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 3, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	numBatches := uint64(10)
	var batchedRequests arma.BatchedRequests
	for _, pID := range parties {
		for seq := uint64(0); seq < numBatches; seq++ {
			batchedRequests = arma.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, seq, batchedRequests.ToBytes())
			require.Equal(t, seq+1, a.Height(pID))
			batch := a.RetrieveBatchByNumber(pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Party())
			require.NotNil(t, batch.Digest())
		}
	}

	t.Log("Close, reopen write and read")
	a.Close()
	a, err = NewBatchLedgerArray(1, 3, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	for _, pID := range parties {
		require.Equal(t, numBatches, a.Height(pID))
		batch := a.RetrieveBatchByNumber(pID, numBatches-1)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, pID, batch.Party())
		require.NotNil(t, batch.Digest())
	}

	for _, pID := range parties {
		for seq := numBatches; seq < 2*numBatches; seq++ {
			batchedRequests = arma.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, seq, batchedRequests.ToBytes())
			require.Equal(t, seq+1, a.Height(pID))
			batch := a.RetrieveBatchByNumber(pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Party())
			require.NotNil(t, batch.Digest())
		}
	}
}

func TestBatchLedgerArrayPart(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []arma.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	batchedRequests := arma.BatchedRequests{[]byte("tx1"), []byte("tx2")}
	for _, pID := range parties {
		part := a.Part(pID)
		for seq := uint64(0); seq < 10; seq++ {
			part.Append(seq, batchedRequests.ToBytes())
			require.Equal(t, seq+1, part.Height())
			batch := part.RetrieveBatchByNumber(seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Party())
			require.NotNil(t, batch.Digest())
		}
	}
}
