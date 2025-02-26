package prefetch_benchmark_test

import (
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
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
		emptyRequestsDigest:   testutil.CalculateDigest(emptyRequests),
		specialRequests:       specialRequests,
		specialRequestsDigest: testutil.CalculateDigest(specialRequests),
	}
}

func (bg *batchGenerator) GenerateBatch(shardId types.ShardID, primaryId types.PartyID, seq types.BatchSequence, regularDigest bool) core.Batch {
	requests := bg.specialRequests
	digest := bg.specialRequestsDigest
	if !regularDigest {
		requests = bg.emptyRequests
		digest = bg.emptyRequestsDigest
	}
	return types.NewSimpleBatch(seq, shardId, primaryId, requests, digest)
}
