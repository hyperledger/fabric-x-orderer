/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"testing"

	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/batcher/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRequestsInspectAndVerify(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	bundle.ConfigtxValidatorReturns(configtxValidator)

	config := &config.BatcherNodeConfig{
		ShardId:         1,
		Shards:          []config.ShardInfo{{ShardId: types.ShardID(1)}, {ShardId: types.ShardID(2)}},
		BatchMaxSize:    3,
		BatchMaxBytes:   180,
		RequestMaxBytes: 60,
		Bundle:          bundle,
	}
	verifier := batcher.NewRequestsInspectorVerifier(logger, config, nil)

	req1, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{1}))             // should map to shard 1
	req2, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{11}))            // should map to shard 2
	largeReq, _ := proto.Marshal(tx.CreateStructuredRequest(make([]byte, 101))) // large request, mapped to shard 1

	t.Run("empty request ID", func(t *testing.T) {
		emptyReq := []byte{}
		require.Equal(t, "", verifier.RequestID(emptyReq))
		require.Equal(t, "", verifier.RequestID(nil))
	})

	t.Run("verify empty request", func(t *testing.T) {
		// non-empty request
		require.NoError(t, verifier.VerifyRequest(req1))

		// request with empty payload
		emptyPayloadReq, err := proto.Marshal(&protos.Request{Payload: nil})
		require.NoError(t, err)
		require.ErrorContains(t, verifier.VerifyRequest(emptyPayloadReq), "empty")
	})

	t.Run("verify request max bytes", func(t *testing.T) {
		require.ErrorContains(t, verifier.VerifyRequest(largeReq), "request's size exceeds the maximum size")
	})

	t.Run("verify batched requests with max batch bytes", func(t *testing.T) {
		reqs := make([][]byte, 3)
		reqs[0] = largeReq
		reqs[1] = largeReq
		reqs[2] = largeReq
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "too big")
	})

	t.Run("verify request with mapping", func(t *testing.T) {
		req := tx.CreateStructuredRequest([]byte("123345"))
		_ = req
		// req1 - should be mapped to shard 1
		require.NoError(t, verifier.VerifyRequest(req1))
		// req2 - should be mapped to shard 2
		require.ErrorContains(t, verifier.VerifyRequest(req2), "map")
	})

	t.Run("verify request with unmarshal", func(t *testing.T) {
		require.ErrorContains(t, verifier.VerifyRequest([]byte{1}), "unmarshal")
	})

	t.Run("verify batched requests with empty batch", func(t *testing.T) {
		reqs := make([][]byte, 0)
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "empty batch")
	})

	t.Run("verify batched requests with empty request", func(t *testing.T) {
		rawEmptyReq, err := proto.Marshal(&protos.Request{Payload: nil})
		require.NoError(t, err)

		reqs := make([][]byte, 3)
		reqs[0] = req1
		reqs[1] = req1
		reqs[2] = rawEmptyReq
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "empty")

		reqs[2] = req1
		require.NoError(t, verifier.VerifyBatchedRequests(reqs))
	})

	t.Run("verify batched requests with too many requests", func(t *testing.T) {
		reqs := make([][]byte, 3)
		reqs[0] = req1
		reqs[1] = req1
		reqs[2] = req1
		require.NoError(t, verifier.VerifyBatchedRequests(reqs))

		reqs = append(reqs, req1)
		require.ErrorContains(t, verifier.VerifyBatchedRequests(reqs), "too big")
	})
}

func TestRequestVerificationStopEarly(t *testing.T) {
	req1, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{1})) // should map to shard 1

	logger := testutil.CreateLogger(t, 1)
	config := &config.BatcherNodeConfig{
		ShardId:         1,
		Shards:          []config.ShardInfo{{ShardId: 1}, {ShardId: 2}},
		BatchMaxSize:    500,
		BatchMaxBytes:   10000,
		RequestMaxBytes: 1000,
	}
	reqVerifier := &mocks.FakeRequestVerifier{}

	verifier := batcher.NewRequestsInspectorVerifier(logger, config, reqVerifier)

	reqs := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		reqs[i] = req1
	}

	reqVerifier.VerifyReturns(nil)

	require.NoError(t, verifier.VerifyBatchedRequests(reqs))

	require.Equal(t, 100, reqVerifier.VerifyCallCount())

	reqVerifier.VerifyReturns(errors.New("error"))

	require.Error(t, verifier.VerifyBatchedRequests(reqs))

	require.Less(t, reqVerifier.VerifyCallCount(), 200)
	t.Log(reqVerifier.VerifyCallCount())
}
