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
	requests := types.BatchedRequests{}
	for _, requestSize := range requestsBytesSize {
		requests = append(requests, make([]byte, requestSize))
	}
	return CreateMockBatchWithRequests(shard, primary, seq, requests)
}

func CalculateDigest(requests types.BatchedRequests) []byte {
	digest := sha256.Sum256(requests.Serialize())
	return digest[:]
}

func CreateMockBatchWithRequests(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, requests types.BatchedRequests) *core_mocks.FakeBatch {
	batch := &core_mocks.FakeBatch{}
	batch.ShardReturns(shard)
	batch.PrimaryReturns(primary)
	batch.SeqReturns(seq)
	batch.DigestReturns(CalculateDigest(requests))
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

func AssertBatchIdsEquals(t *testing.T, expectedBatchId, actualBatchId types.BatchID) {
	require.Equal(t, expectedBatchId.Shard(), actualBatchId.Shard())
	require.Equal(t, expectedBatchId.Primary(), actualBatchId.Primary())
	require.Equal(t, expectedBatchId.Seq(), actualBatchId.Seq())
	require.Equal(t, expectedBatchId.Digest(), actualBatchId.Digest())
}
