/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime"

	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/request_verifier.go . RequestVerifier

// RequestVerifier verifies a single request (format and signatures)
type RequestVerifier interface {
	Verify(req *comm.Request) error
}

type RequestsInspectorVerifier struct {
	requestMaxBytes uint64
	batchMaxBytes   uint32
	batchMaxSize    uint32
	shards          []types.ShardID
	shardID         types.ShardID
	logger          types.Logger
	mapper          router.ShardMapper
	requestVerifier RequestVerifier
}

func NewRequestsInspectorVerifier(logger types.Logger, config *config.BatcherNodeConfig, requestVerifier RequestVerifier) *RequestsInspectorVerifier {
	riv := &RequestsInspectorVerifier{
		logger:          logger,
		shardID:         config.ShardId,
		shards:          config.GetShardsIDs(),
		batchMaxSize:    config.BatchMaxSize,
		batchMaxBytes:   config.BatchMaxBytes,
		requestMaxBytes: config.RequestMaxBytes,
	}
	if requestVerifier != nil {
		riv.requestVerifier = requestVerifier
	} else {
		riv.requestVerifier = createBatcherRulesVerifier(config)
	}
	riv.mapper = router.MapperCRC64{Logger: riv.logger, ShardCount: uint16(len(riv.shards))}
	return riv
}

func createBatcherRulesVerifier(config *config.BatcherNodeConfig) *requestfilter.RulesVerifier {
	rv := requestfilter.NewRulesVerifier(nil)
	rv.AddRule(requestfilter.PayloadNotEmptyRule{})
	rv.AddRule(requestfilter.NewMaxSizeFilter(config))
	rv.AddRule(requestfilter.NewSigFilter(config, policies.ChannelWriters))
	return rv
}

func (r *RequestsInspectorVerifier) VerifyBatchedRequests(reqs types.BatchedRequests) error {
	if len(reqs) == 0 {
		return errors.New("empty batch")
	}

	if len(reqs) > int(r.batchMaxSize) {
		return errors.Errorf("batch is too big; has %d requests", len(reqs))
	}

	size := 0
	for _, req := range reqs {
		size += len(req)
	}

	if size > int(r.batchMaxBytes) {
		return errors.Errorf("batch is too big; size in bytes is %d", size)
	}

	g, ctx := errgroup.WithContext(context.Background())
	numWorkers := runtime.NumCPU()

	for i := 0; i < numWorkers; i++ {
		workerID := i
		g.Go(func() error {
			for j := 0; j < len(reqs); j++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if workerID != j%numWorkers {
					continue
				}
				if err := r.VerifyRequest(reqs[j]); err != nil {
					return errors.Errorf("failed verifying request in index %d; req ID: %s; err: %v", j, r.RequestID(reqs[j]), err)
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Errorf("failed verifying batch; err: %v", err)
	}

	return nil
}

func (r *RequestsInspectorVerifier) VerifyRequestShard(req *comm.Request) error {
	if len(r.shards) != 1 {
		shardIndex, _ := r.mapper.Map(req.Payload)
		if r.shardID != r.shards[shardIndex] {
			rawReq, _ := proto.Marshal(req)
			return errors.Errorf("request maps to shard %d but our shard is %d; request ID %s", r.shards[shardIndex], r.shardID, r.RequestID(rawReq))
		}
	}
	return nil
}

// VerifyRequestFromRouter verifies a request that comes from the router in the batcher's party.
func (r *RequestsInspectorVerifier) VerifyRequestFromRouter(req *comm.Request) error {
	// TODO - check config sequence, and verify again if needed.
	return nil
}

func (r *RequestsInspectorVerifier) VerifyRequest(req []byte) error {
	var request comm.Request
	if err := proto.Unmarshal(req, &request); err != nil {
		return errors.Errorf("failed to unmarshall req")
	}

	if err := r.VerifyRequestShard(&request); err != nil {
		return errors.Errorf("failed verifying request with id: %s; err: %v", r.RequestID(req), err)
	}

	if err := r.requestVerifier.Verify(&request); err != nil {
		return errors.Errorf("failed verifying request with id: %s; err: %v", r.RequestID(req), err)
	}

	return nil
}

func (r *RequestsInspectorVerifier) RequestID(req []byte) string {
	if len(req) == 0 {
		return ""
	}
	// TODO maybe calculate the request ID differently
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}
