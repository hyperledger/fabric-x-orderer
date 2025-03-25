package ledger

import (
	"fmt"
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/common/types"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/stretchr/testify/require"
)

func TestNewBatchLedgerArray(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
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
	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 3, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	numBatches := uint64(10)
	var batchedRequests types.BatchedRequests
	for _, pID := range parties {
		for seq := uint64(0); seq < numBatches; seq++ {
			batchedRequests = types.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, types.BatchSequence(seq), batchedRequests)
			require.Equal(t, seq+1, a.Height(pID))
			batch := a.RetrieveBatchByNumber(pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
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
		require.Equal(t, pID, batch.Primary())
		require.NotNil(t, batch.Digest())
	}

	for _, pID := range parties {
		for seq := numBatches; seq < 2*numBatches; seq++ {
			batchedRequests = types.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, types.BatchSequence(seq), batchedRequests)
			require.Equal(t, seq+1, a.Height(pID))
			batch := a.RetrieveBatchByNumber(pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}
}

func TestBatchLedgerArrayPart(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	batchedRequests := types.BatchedRequests{[]byte("tx1"), []byte("tx2")}
	for _, pID := range parties {
		part := a.Part(pID)
		for seq := uint64(0); seq < 10; seq++ {
			part.Append(types.BatchSequence(seq), batchedRequests)
			require.Equal(t, seq+1, part.Height())
			batch := part.RetrieveBatchByNumber(seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}
}
