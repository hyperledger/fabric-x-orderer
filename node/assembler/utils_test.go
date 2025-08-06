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

func createTestBatch(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, req []byte) types.Batch {
	return types.NewSimpleBatch(seq, shard, primary, types.BatchedRequests{req})
}

// createTestBatchWithSize creates a simple batch including requests with given size
//
// Example:
//
//	// Returns a simple batch with:
//	// shard id = 1
//	// primary party id = 2
//	// sequence = 3
//	// digest = calculated according to the generated requests
//	// 2 requests, the first of size 4 bytes, the second with 5 bytes
//	b := createTestBatchWithSize(1, 2, 3, []int{4, 5})
func createTestBatchWithSize(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, requestsBytesSize []int) types.Batch {
	requests := types.BatchedRequests{}
	for _, requestSize := range requestsBytesSize {
		requests = append(requests, make([]byte, requestSize))
	}
	return types.NewSimpleBatch(seq, shard, primary, requests)
}

func createTestBatchId(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, req []byte) types.BatchID {
	return types.NewSimpleBatch(seq, shard, primary, types.BatchedRequests{req})
}

func assertBatchIdsEquals(t *testing.T, expectedBatchId, actualBatchId types.BatchID) {
	require.True(t, types.BatchIDEqual(expectedBatchId, actualBatchId))
}
