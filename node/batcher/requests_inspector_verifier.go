package batcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime"

	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// RequestsVerifier verifies requests

type ClientRequestVerifier interface {
	VerifyClientRequest(req []byte) error
}

type NoopClientRequestVerifier struct{}

func (v *NoopClientRequestVerifier) VerifyClientRequest(req []byte) error {
	return nil
}

type RequestsVerifier interface {
	VerifyRequest(req []byte) error
	core.BatchedRequestsVerifier
}

type RequestsInspectorVerifier struct {
	requestMaxBytes uint64
	batchMaxBytes   uint32
	batchMaxSize    uint32
	shards          []types.ShardID
	shardID         types.ShardID
	logger          types.Logger
	mapper          *core.Router // TODO make this into an interface?
	requestVerifier ClientRequestVerifier
}

func NewRequestsInspectorVerifier(logger types.Logger, shardID types.ShardID, shards []types.ShardID, batchMaxSize uint32, batchMaxBytes uint32, requestMaxBytes uint64, requestVerifier ClientRequestVerifier) *RequestsInspectorVerifier {
	riv := &RequestsInspectorVerifier{
		logger:          logger,
		shardID:         shardID,
		shards:          shards,
		batchMaxSize:    batchMaxSize,
		batchMaxBytes:   batchMaxBytes,
		requestMaxBytes: requestMaxBytes,
		requestVerifier: requestVerifier,
	}
	riv.mapper = &core.Router{Logger: riv.logger, ShardCount: uint16(len(riv.shards))}
	return riv
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

func (r *RequestsInspectorVerifier) VerifyRequest(req []byte) error {
	reqID := r.RequestID(req)
	if reqID == "" {
		return errors.Errorf("empty req")
	}
	if uint64(len(req)) > r.requestMaxBytes {
		return errors.Errorf("request size (%d) is bigger than request max bytes (%d); request ID %s", len(req), r.requestMaxBytes, reqID)
	}
	if len(r.shards) != 1 {
		var request comm.Request
		err := proto.Unmarshal(req, &request) // TODO avoid unmarshaling here by unifying the batcher and router behavior
		if err != nil {
			return errors.Errorf("failed to unmarshal req")
		}
		shardIndex, _ := r.mapper.Map(request.Payload)
		if r.shardID != r.shards[shardIndex] {
			return errors.Errorf("request maps to shard %d but our shard is %d; request ID %s", r.shards[shardIndex], r.shardID, reqID)
		}
	}
	if err := r.requestVerifier.VerifyClientRequest(req); err != nil { // TODO actually verify the request (for example client's signature)
		return errors.Errorf("failed verifying request with id: %s; err: %v", reqID, err)
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
