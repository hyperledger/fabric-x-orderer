package batcher_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher/mocks"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
	"google.golang.org/protobuf/proto"
)

func TestRequestsInspectAndVerify(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)
	config := &config.BatcherNodeConfig{
		ShardId:         1,
		Shards:          []config.ShardInfo{{ShardId: 1}, {ShardId: 2}},
		BatchMaxSize:    3,
		BatchMaxBytes:   10,
		RequestMaxBytes: 10,
	}
	verifier := batcher.NewRequestsInspectorVerifier(logger, config, &batcher.NoopClientRequestSigVerifier{}, nil)

	t.Run("empty request ID", func(t *testing.T) {
		emptyReq := []byte{}
		require.Equal(t, "", verifier.RequestID(emptyReq))
		require.Equal(t, "", verifier.RequestID(nil))
	})

	t.Run("verify empty request", func(t *testing.T) {
		rawReq0, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		require.NoError(t, verifier.VerifyRequest(rawReq0))
		rawReq1, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		require.NoError(t, verifier.VerifyRequest(rawReq1))
		rawReq2, err := proto.Marshal(&protos.Request{Payload: []byte{}})
		require.NoError(t, err)
		require.ErrorContains(t, verifier.VerifyRequest(rawReq2), "empty")
	})

	t.Run("verify request max bytes", func(t *testing.T) {
		largeReq := make([]byte, 11)
		rawReq, err := proto.Marshal(&protos.Request{Payload: largeReq})
		require.NoError(t, err)
		require.ErrorContains(t, verifier.VerifyRequest(rawReq), "bigger than request max bytes")
	})

	t.Run("verify batched requests with max batch bytes", func(t *testing.T) {
		largeReq := make([]byte, 11)
		rawReq, err := proto.Marshal(&protos.Request{Payload: largeReq})
		require.NoError(t, err)
		reqs := make([][]byte, 3)
		reqs[0] = rawReq
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "too big")
	})

	t.Run("verify request with mapping", func(t *testing.T) {
		rawReq0, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		require.NoError(t, verifier.VerifyRequest(rawReq0))
		rawReq1, err := proto.Marshal(&protos.Request{Payload: []byte{5}})
		require.NoError(t, err)
		require.ErrorContains(t, verifier.VerifyRequest(rawReq1), "map")
	})

	t.Run("verify request with unmarshal", func(t *testing.T) {
		require.ErrorContains(t, verifier.VerifyRequest([]byte{1}), "unmarshal")
	})

	t.Run("verify batched requests with empty batch", func(t *testing.T) {
		reqs := make([][]byte, 0)
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "empty batch")
	})

	t.Run("verify batched requests with empty request", func(t *testing.T) {
		rawReq0, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		rawReq1, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		rawReq2, err := proto.Marshal(&protos.Request{Payload: []byte{}})
		require.NoError(t, err)

		reqs := make([][]byte, 3)
		reqs[0] = rawReq0
		reqs[1] = rawReq1
		reqs[2] = rawReq2
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "empty")

		reqs[2] = rawReq0
		require.NoError(t, verifier.VerifyBatchedRequests(reqs))
	})

	t.Run("verify batched requests with too many requests", func(t *testing.T) {
		rawReq0, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		rawReq1, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)

		reqs := make([][]byte, 3)
		reqs[0] = rawReq0
		reqs[1] = rawReq1
		reqs[2] = rawReq0
		require.NoError(t, verifier.VerifyBatchedRequests(reqs))

		reqs = append(reqs, rawReq1)
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "too big")
	})
}

func TestRequestVerificationStopEarly(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)
	config := &config.BatcherNodeConfig{
		ShardId:         1,
		Shards:          []config.ShardInfo{{ShardId: 1}, {ShardId: 2}},
		BatchMaxSize:    500,
		BatchMaxBytes:   1000,
		RequestMaxBytes: 1000,
	}
	reqVerifier := &mocks.FakeRequestVerifier{}

	verifier := batcher.NewRequestsInspectorVerifier(logger, config, &batcher.NoopClientRequestSigVerifier{}, reqVerifier)

	reqs := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		rawReq, err := proto.Marshal(&protos.Request{Payload: []byte{1}})
		require.NoError(t, err)
		reqs[i] = rawReq
	}

	reqVerifier.VerifyRequestReturns(nil)

	require.NoError(t, verifier.VerifyBatchedRequests(reqs))

	require.Equal(t, 100, reqVerifier.VerifyRequestCallCount())

	reqVerifier.VerifyRequestReturns(errors.New("error"))

	require.Error(t, verifier.VerifyBatchedRequests(reqs))

	require.Less(t, reqVerifier.VerifyRequestCallCount(), 200)
	t.Log(reqVerifier.VerifyRequestCallCount())
}
