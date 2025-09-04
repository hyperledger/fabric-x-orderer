/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package prefetch_benchmark_test

import (
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type batchGenerator struct {
	emptyRequests         types.BatchedRequests
	emptyRequestsDigest   []byte
	specialRequests       types.BatchedRequests
	specialRequestsDigest []byte
}

func newBatchGenerator(txInBatch, txSize int) *batchGenerator {
	emptyRequests := types.BatchedRequests{}
	for i := 0; i < txInBatch; i++ {
		emptyRequests = append(emptyRequests, make([]byte, txSize))
	}
	specialRequests := types.BatchedRequests{}
	for i := 0; i < txInBatch; i++ {
		request := make([]byte, txSize)
		request[0] = 1
		specialRequests = append(specialRequests, request)
	}
	return &batchGenerator{
		emptyRequests:         emptyRequests,
		emptyRequestsDigest:   emptyRequests.Digest(),
		specialRequests:       specialRequests,
		specialRequestsDigest: specialRequests.Digest(),
	}
}

func (bg *batchGenerator) GenerateBatch(shardId types.ShardID, primaryId types.PartyID, seq types.BatchSequence, regular bool) types.Batch {
	requests := bg.specialRequests
	if !regular {
		requests = bg.emptyRequests
	}
	return types.NewSimpleBatch(shardId, primaryId, seq, requests, 0)
}
