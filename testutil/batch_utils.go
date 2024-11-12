package testutil

import (
	"crypto/sha256"
	"testing"

	"arma/common/types"
	types_mocks "arma/common/types/mocks"
	core_mocks "arma/core/mocks"

	"github.com/stretchr/testify/require"
)

func CreateEmptyMockBatch(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest []byte) *core_mocks.FakeBatch {
	batch := &core_mocks.FakeBatch{}
	batch.ShardReturns(shard)
	batch.PrimaryReturns(primary)
	batch.SeqReturns(seq)
	batch.DigestReturns(digest)
	return batch
}

// CreateMockBatch creates a mock batch
//
// Example:
//
//	// Returns a mock batch with:
//	// shard id = 1
//	// primary party id = 2
//	// sequence = 3
//	// digest = calculated according to the generated requests
//	// 2 requests, the first of size 4 bytes, the second with 5 bytes
//	b := CreateMockBatch(1, 2, 3, []int{4, 5})
func CreateMockBatch(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, requestsBytesSize []int) *core_mocks.FakeBatch {
	batch := &core_mocks.FakeBatch{}
	batch.ShardReturns(shard)
	batch.PrimaryReturns(primary)
	batch.SeqReturns(seq)
	requests := types.BatchedRequests{}
	for _, requestSize := range requestsBytesSize {
		requests = append(requests, make([]byte, requestSize))
	}
	digest := sha256.Sum256(requests.Serialize())
	batch.DigestReturns(digest[:])
	batch.RequestsReturns(requests)
	return batch
}

func CreateMockBatchId(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest []byte) *types_mocks.FakeBatchID {
	batchId := &types_mocks.FakeBatchID{}
	batchId.ShardReturns(shard)
	batchId.PrimaryReturns(primary)
	batchId.SeqReturns(seq)
	batchId.DigestReturns(digest)
	return batchId
}

func AssertBatchIdsEquals(t *testing.T, batchId types.BatchID, otherBatchId types.BatchID) {
	require.Equal(t, batchId.Shard(), otherBatchId.Shard())
	require.Equal(t, batchId.Primary(), otherBatchId.Primary())
	require.Equal(t, batchId.Seq(), otherBatchId.Seq())
	require.Equal(t, batchId.Digest(), otherBatchId.Digest())
}
