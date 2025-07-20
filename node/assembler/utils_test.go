/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/require"
)

type TestBatch struct {
	seq      types.BatchSequence
	shard    types.ShardID
	primary  types.PartyID
	requests types.BatchedRequests
	digest   []byte
}

func (tb *TestBatch) Digest() []byte                  { return tb.digest }
func (tb *TestBatch) Requests() types.BatchedRequests { return tb.requests }
func (tb *TestBatch) Primary() types.PartyID          { return tb.primary }
func (tb *TestBatch) Shard() types.ShardID            { return tb.shard }
func (tb *TestBatch) Seq() types.BatchSequence        { return tb.seq }

func createEmptyTestBatch(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest []byte) types.Batch {
	return &TestBatch{seq: seq, shard: shard, primary: primary, digest: digest}
}

// createTestBatch creates a mock batch
//
// Example:
//
//	// Returns a mock batch with:
//	// shard id = 1
//	// primary party id = 2
//	// sequence = 3
//	// digest = calculated according to the generated requests
//	// 2 requests, the first of size 4 bytes, the second with 5 bytes
//	b := createTestBatch(1, 2, 3, []int{4, 5})
func createTestBatch(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, requestsBytesSize []int) types.Batch {
	requests := types.BatchedRequests{}
	for _, requestSize := range requestsBytesSize {
		requests = append(requests, make([]byte, requestSize))
	}
	return &TestBatch{seq: seq, shard: shard, primary: primary, requests: requests, digest: requests.Digest()}
}

func createTestBatchId(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest []byte) types.BatchID {
	return &TestBatch{seq: seq, shard: shard, primary: primary, digest: digest}
}

func assertBatchIdsEquals(t *testing.T, expectedBatchId, actualBatchId types.BatchID) {
	require.Equal(t, expectedBatchId.Shard(), actualBatchId.Shard())
	require.Equal(t, expectedBatchId.Primary(), actualBatchId.Primary())
	require.Equal(t, expectedBatchId.Seq(), actualBatchId.Seq())
	require.Equal(t, expectedBatchId.Digest(), actualBatchId.Digest())
}
