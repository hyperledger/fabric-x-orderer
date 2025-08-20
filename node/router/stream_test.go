/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	commMocks "github.com/hyperledger/fabric-x-orderer/node/protos/comm/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFaultyStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{ctx: ctx, cancelFunc: cancel}
	require.False(t, s.faulty())
	s.cancel()
	require.True(t, s.faulty())
}

func TestRegisterReply(t *testing.T) {
	traceID := []byte("request123")
	responseChan := make(chan Response, 1)

	s := &stream{
		requestTraceIdToResponseChannel: make(map[string]chan Response),
	}

	s.registerReply(traceID, responseChan)
	respChan, exists := s.isRequestRegistered(traceID)
	require.NotNil(t, respChan)
	require.True(t, exists)
	require.Equal(t, responseChan, respChan)
}

func TestSendRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.SendReturns(nil)
	fakeSubmitStreamClient.ContextReturns(ctx)
	logger := testutil.CreateLogger(t, 0)
	verifier := createTestVerifier()
	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   make(chan *TrackedRequest, 10),
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
		srReconnectChan:                   make(chan reconnectReq, 20),
		verifier:                          verifier,
	}

	go s.sendRequests()

	req1 := createTestTrackedRequestFromTrace([]byte("request1"))
	req2 := createTestTrackedRequestFromTrace([]byte("request2"))

	s.requestsChannel <- req1
	s.requestsChannel <- req2

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.SendCallCount() == 2
	}, time.Second, 10*time.Millisecond)

	sentReq1 := fakeSubmitStreamClient.SendArgsForCall(0)
	require.Equal(t, req1.request, sentReq1)

	sentReq2 := fakeSubmitStreamClient.SendArgsForCall(1)
	require.Equal(t, req2.request, sentReq2)

	s.close()
}

func TestSendRequestsReturnsWithError(t *testing.T) {
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.SendReturns(fmt.Errorf("error"))
	ctx, cancel := context.WithCancel(context.Background())
	logger := testutil.CreateLogger(t, 1)
	verifier := createTestVerifier()

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   make(chan *TrackedRequest, 10),
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
		srReconnectChan:                   make(chan reconnectReq, 20),
		verifier:                          verifier,
	}

	go s.sendRequests()

	req := createTestTrackedRequestFromTrace([]byte("request"))
	s.requestsChannel <- req

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.SendCallCount() == 1 && s.faulty()
	}, time.Second, 10*time.Millisecond)

	sentReq := fakeSubmitStreamClient.SendArgsForCall(0)
	require.Equal(t, req.request, sentReq)
}

func TestReadResponses(t *testing.T) {
	reqID := []byte{123}
	traceID := []byte("request123")

	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.RecvStub = func() (*protos.SubmitResponse, error) {
		if fakeSubmitStreamClient.RecvCallCount() == 1 {
			return &protos.SubmitResponse{
				Error:   "",
				ReqID:   reqID,
				TraceId: traceID,
			}, nil
		}
		return nil, fmt.Errorf("rpc error: service unavailable")
	}

	logger := testutil.CreateLogger(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	verifier := createTestVerifier()

	responseChan := make(chan Response, 1)

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   make(chan *TrackedRequest, 10),
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
		srReconnectChan:                   make(chan reconnectReq, 20),
		verifier:                          verifier,
	}

	s.registerReply(traceID, responseChan)
	respChan, exists := s.isRequestRegistered(traceID)
	require.NotNil(t, respChan)
	require.True(t, exists)

	go s.readResponses()

	resp := <-responseChan
	require.Equal(t, resp.SubmitResponse.Error, "")
	require.Equal(t, resp.SubmitResponse.ReqID, reqID)
	require.Equal(t, resp.SubmitResponse.TraceId, traceID)

	respChan, exists = s.isRequestRegistered(traceID)
	require.Nil(t, respChan)
	require.False(t, exists)
}

func TestReadResponsesReturnsWithError(t *testing.T) {
	reqID := []byte{123}
	traceID := []byte("request123")

	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.RecvReturns(&protos.SubmitResponse{
		Error:   "SERVICE_UNAVAILABLE",
		ReqID:   reqID,
		TraceId: traceID,
	}, fmt.Errorf("rpc error: service unavailable"))
	logger := testutil.CreateLogger(t, 3)
	verifier := createTestVerifier()

	ctx, cancel := context.WithCancel(context.Background())

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   make(chan *TrackedRequest, 10),
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
		srReconnectChan:                   make(chan reconnectReq, 20),
		verifier:                          verifier,
	}

	go s.readResponses()

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.RecvCallCount() == 1 && s.faulty()
	}, time.Second, 10*time.Millisecond)
}

// scenario: when error is received, check that the requests in the request channel get a response.
func TestErrorRequestChannel(t *testing.T) {
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.RecvReturns(&protos.SubmitResponse{Error: "SERVICE_UNAVAILABLE"}, fmt.Errorf("rpc error: service unavailable"))
	logger := testutil.CreateLogger(t, 3)
	verifier := createTestVerifier()

	ctx, cancel := context.WithCancel(context.Background())

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   make(chan *TrackedRequest, 10),
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
		srReconnectChan:                   make(chan reconnectReq, 20),
		verifier:                          verifier,
	}

	responseChan := make(chan Response, 1)
	trace := []byte("traceID")
	reqID := []byte("ID")
	req := CreateTrackedRequest(&protos.Request{TraceId: trace}, responseChan, reqID, trace)
	s.requestsChannel <- req

	go s.readResponses()

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.RecvCallCount() == 1 && s.faulty()
	}, time.Second, 10*time.Millisecond)

	res := <-responseChan
	require.ErrorContains(t, res.err, "server error: could not establish connection between router and batcher")
	require.True(t, bytes.Equal(res.reqID, reqID))
}

func TestRenewStreamSuccess(t *testing.T) {
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}

	ctx, cancel := context.WithCancel(context.Background())
	fakeSubmitStreamClient.ContextReturns(ctx)

	logger := testutil.CreateLogger(t, 4)

	// prepare requests and map
	requests := make(chan *TrackedRequest, 10)
	requestTraceIdToResponseChannel := make(map[string]chan Response)

	req1 := createTestTrackedRequestFromTrace([]byte{1})
	requests <- req1
	requestTraceIdToResponseChannel[string(req1.trace)] = make(chan Response, 100)

	req2 := createTestTrackedRequestFromTrace([]byte{2})
	requests <- req2
	requestTraceIdToResponseChannel[string(req2.trace)] = make(chan Response, 100)
	verifier := createTestVerifier()

	faultyStream := &stream{
		endpoint:                          "127.0.0.1:7015",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   requests,
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   requestTraceIdToResponseChannel,
		srReconnectChan:                   make(chan reconnectReq, 20),
		verifier:                          verifier,
	}

	faultyStream.cancel()
	assert.Eventually(t, func() bool {
		return faultyStream.faulty()
	}, time.Second, 10*time.Millisecond)

	client := &commMocks.FakeRequestTransmitClient{}
	newFakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	newFakeSubmitStreamClient.RecvStub = func() (*protos.SubmitResponse, error) {
		if newFakeSubmitStreamClient.RecvCallCount() == 1 {
			return &protos.SubmitResponse{
				Error:   "",
				TraceId: req1.trace,
			}, nil
		}
		if newFakeSubmitStreamClient.RecvCallCount() == 2 {
			return &protos.SubmitResponse{
				Error:   "",
				TraceId: req2.trace,
			}, nil
		}
		return &protos.SubmitResponse{
			Error:   "",
			TraceId: []byte{3},
		}, nil
	}

	var reqPool safeReqPool
	newFakeSubmitStreamClient.SendStub = func(request *protos.Request) error {
		reqPool.append(request)
		return nil
	}

	ctx, cancel = context.WithCancel(context.Background())
	newFakeSubmitStreamClient.ContextReturns(ctx)
	client.SubmitStreamReturns(newFakeSubmitStreamClient, nil)
	newStream, err := faultyStream.renewStream(client, faultyStream.endpoint, faultyStream.logger, ctx, cancel)
	require.NoError(t, err)
	require.NotNil(t, newStream)
	require.False(t, newStream.faulty())

	assert.Eventually(t, func() bool {
		reqCond := reqPool.length() == 2 && bytes.Equal(reqPool.getElement(0).TraceId, req1.trace) && bytes.Equal(reqPool.getElement(1).TraceId, req2.trace)
		return reqCond && newFakeSubmitStreamClient.SendCallCount() >= 2 && newFakeSubmitStreamClient.RecvCallCount() >= 2
	}, 30*time.Second, 10*time.Millisecond)

	newStream.close()
}

// Scenario:
// A request is sent, but Send() returns an error
// The stream is then expected to be reported to the reconnection goroutine in SR
func TestReconnectRequest(t *testing.T) {
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.SendReturns(fmt.Errorf("error"))
	ctx, cancel := context.WithCancel(context.Background())
	logger := testutil.CreateLogger(t, 1)
	verifier := createTestVerifier()

	connectionNumber := 2
	streamNumber := 3

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestsChannel:                   make(chan *TrackedRequest, 10),
		doneChannel:                       make(chan bool),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
		srReconnectChan:                   make(chan reconnectReq, 20),
		connNum:                           connectionNumber,
		streamNum:                         streamNumber,
		verifier:                          verifier,
	}

	go s.sendRequests()

	req := createTestTrackedRequestFromTrace([]byte("request"))
	s.requestsChannel <- req

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.SendCallCount() == 1 && s.faulty()
	}, time.Second, 10*time.Millisecond)

	reconnectRequest := <-s.srReconnectChan

	require.Equal(t, connectionNumber, reconnectRequest.connNumber)
	require.Equal(t, streamNumber, reconnectRequest.streamInConn)
}

type safeReqPool struct {
	mu      sync.Mutex
	reqPool []*protos.Request
}

func (srp *safeReqPool) append(request *protos.Request) {
	srp.mu.Lock()
	defer srp.mu.Unlock()
	srp.reqPool = append(srp.reqPool, request)
}

func (srp *safeReqPool) length() int {
	srp.mu.Lock()
	defer srp.mu.Unlock()
	return len(srp.reqPool)
}

func (srp *safeReqPool) getElement(i int) *protos.Request {
	srp.mu.Lock()
	defer srp.mu.Unlock()
	return srp.reqPool[i]
}

func createTestVerifier() *requestfilter.RulesVerifier {
	rv := requestfilter.NewRulesVerifier(nil)
	rv.AddRule(requestfilter.AcceptRule{})
	return rv
}

func createTestTrackedRequestFromTrace(trace []byte) *TrackedRequest {
	return CreateTrackedRequest(&protos.Request{TraceId: trace}, make(chan Response, 10), nil, trace)
}
