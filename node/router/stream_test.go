/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFaultyStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{ctx: ctx, cancelFunc: cancel}
	require.False(t, s.faulty())
	s.cancel()
	require.True(t, s.faulty())
}

// func TestRegisterReply(t *testing.T) {
// 	traceID := []byte("request123")
// 	responseChan := make(chan Response, 1)

// 	s := &stream{
// 		requestTraceIdToResponseChannel: make(map[string]chan Response),
// 	}

// 	s.registerReply(traceID, responseChan)
// 	respChan, exists := s.isRequestRegistered(traceID)
// 	require.NotNil(t, respChan)
// 	require.True(t, exists)
// 	require.Equal(t, responseChan, respChan)
// }

// func TestSendRequests(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
// 	fakeSubmitStreamClient.SendReturns(nil)
// 	fakeSubmitStreamClient.ContextReturns(ctx)
// 	logger := testutil.CreateLogger(t, 0)

// 	s := &stream{
// 		endpoint:                          "127.0.0.1:5017",
// 		logger:                            logger,
// 		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
// 		ctx:                               ctx,
// 		cancelFunc:                        cancel,
// 		requestsChannel:                   make(chan *protos.Request, 10),
// 		doneChannel:                       make(chan bool),
// 		requestTraceIdToResponseChannel:   make(map[string]chan Response),
// 		srReconnectChan:                   make(chan reconnectReq, 20),
// 	}

// 	go s.sendRequests()

// 	req1 := &protos.Request{TraceId: []byte("request1")}
// 	req2 := &protos.Request{TraceId: []byte("request2")}

// 	s.requestsChannel <- req1
// 	s.requestsChannel <- req2

// 	assert.Eventually(t, func() bool {
// 		return fakeSubmitStreamClient.SendCallCount() == 2
// 	}, time.Second, 10*time.Millisecond)

// 	sentReq1 := fakeSubmitStreamClient.SendArgsForCall(0)
// 	require.Equal(t, req1, sentReq1)

// 	sentReq2 := fakeSubmitStreamClient.SendArgsForCall(1)
// 	require.Equal(t, req2, sentReq2)

// 	s.close()
// }

// func TestSendRequestsReturnsWithError(t *testing.T) {
// 	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
// 	fakeSubmitStreamClient.SendReturns(fmt.Errorf("error"))
// 	ctx, cancel := context.WithCancel(context.Background())
// 	logger := testutil.CreateLogger(t, 1)

// 	s := &stream{
// 		endpoint:                          "127.0.0.1:5017",
// 		logger:                            logger,
// 		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
// 		ctx:                               ctx,
// 		cancelFunc:                        cancel,
// 		requestsChannel:                   make(chan *protos.Request, 10),
// 		doneChannel:                       make(chan bool),
// 		requestTraceIdToResponseChannel:   make(map[string]chan Response),
// 		srReconnectChan:                   make(chan reconnectReq, 20),
// 	}

// 	go s.sendRequests()

// 	req := &protos.Request{TraceId: []byte("request")}
// 	s.requestsChannel <- req

// 	assert.Eventually(t, func() bool {
// 		return fakeSubmitStreamClient.SendCallCount() == 1 && s.faulty()
// 	}, time.Second, 10*time.Millisecond)

// 	sentReq := fakeSubmitStreamClient.SendArgsForCall(0)
// 	require.Equal(t, req, sentReq)
// }

// func TestReadResponses(t *testing.T) {
// 	reqID := []byte{123}
// 	traceID := []byte("request123")

// 	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
// 	fakeSubmitStreamClient.RecvStub = func() (*protos.SubmitResponse, error) {
// 		if fakeSubmitStreamClient.RecvCallCount() == 1 {
// 			return &protos.SubmitResponse{
// 				Error:   "",
// 				ReqID:   reqID,
// 				TraceId: traceID,
// 			}, nil
// 		}
// 		return nil, fmt.Errorf("rpc error: service unavailable")
// 	}

// 	logger := testutil.CreateLogger(t, 2)
// 	ctx, cancel := context.WithCancel(context.Background())

// 	responseChan := make(chan Response, 1)

// 	s := &stream{
// 		endpoint:                          "127.0.0.1:5017",
// 		logger:                            logger,
// 		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
// 		ctx:                               ctx,
// 		cancelFunc:                        cancel,
// 		requestsChannel:                   make(chan *protos.Request, 10),
// 		doneChannel:                       make(chan bool),
// 		requestTraceIdToResponseChannel:   make(map[string]chan Response),
// 		srReconnectChan:                   make(chan reconnectReq, 20),
// 	}

// 	s.registerReply(traceID, responseChan)
// 	respChan, exists := s.isRequestRegistered(traceID)
// 	require.NotNil(t, respChan)
// 	require.True(t, exists)

// 	go s.readResponses()

// 	resp := <-responseChan
// 	require.Equal(t, resp.SubmitResponse.Error, "")
// 	require.Equal(t, resp.SubmitResponse.ReqID, reqID)
// 	require.Equal(t, resp.SubmitResponse.TraceId, traceID)

// 	respChan, exists = s.isRequestRegistered(traceID)
// 	require.Nil(t, respChan)
// 	require.False(t, exists)
// }

// func TestReadResponsesReturnsWithError(t *testing.T) {
// 	reqID := []byte{123}
// 	traceID := []byte("request123")

// 	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
// 	fakeSubmitStreamClient.RecvReturns(&protos.SubmitResponse{
// 		Error:   "SERVICE_UNAVAILABLE",
// 		ReqID:   reqID,
// 		TraceId: traceID,
// 	}, fmt.Errorf("rpc error: service unavailable"))
// 	logger := testutil.CreateLogger(t, 3)

// 	ctx, cancel := context.WithCancel(context.Background())

// 	s := &stream{
// 		endpoint:                          "127.0.0.1:5017",
// 		logger:                            logger,
// 		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
// 		ctx:                               ctx,
// 		cancelFunc:                        cancel,
// 		requestsChannel:                   make(chan *protos.Request, 10),
// 		doneChannel:                       make(chan bool),
// 		requestTraceIdToResponseChannel:   make(map[string]chan Response),
// 		srReconnectChan:                   make(chan reconnectReq, 20),
// 	}

// 	go s.readResponses()

// 	assert.Eventually(t, func() bool {
// 		return fakeSubmitStreamClient.RecvCallCount() == 1 && s.faulty()
// 	}, time.Second, 10*time.Millisecond)
// }

// func TestRenewStreamSuccess(t *testing.T) {
// 	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}

// 	ctx, cancel := context.WithCancel(context.Background())
// 	fakeSubmitStreamClient.ContextReturns(ctx)

// 	logger := testutil.CreateLogger(t, 4)

// 	// prepare requests and map
// 	requests := make(chan *protos.Request, 10)
// 	requestTraceIdToResponseChannel := make(map[string]chan Response)

// 	req1 := &protos.Request{
// 		Payload: []byte{0o123},
// 		TraceId: []byte{1},
// 	}
// 	requests <- req1
// 	requestTraceIdToResponseChannel[string(req1.TraceId)] = make(chan Response, 100)

// 	req2 := &protos.Request{
// 		Payload: []byte{123},
// 		TraceId: []byte{2},
// 	}
// 	requests <- req2
// 	requestTraceIdToResponseChannel[string(req2.TraceId)] = make(chan Response, 100)

// 	faultyStream := &stream{
// 		endpoint:                          "127.0.0.1:7015",
// 		logger:                            logger,
// 		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
// 		ctx:                               ctx,
// 		cancelFunc:                        cancel,
// 		requestsChannel:                   requests,
// 		doneChannel:                       make(chan bool),
// 		requestTraceIdToResponseChannel:   requestTraceIdToResponseChannel,
// 		srReconnectChan:                   make(chan reconnectReq, 20),
// 	}

// 	faultyStream.cancel()
// 	assert.Eventually(t, func() bool {
// 		return faultyStream.faulty()
// 	}, time.Second, 10*time.Millisecond)

// 	client := &commMocks.FakeRequestTransmitClient{}
// 	newFakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
// 	newFakeSubmitStreamClient.RecvStub = func() (*protos.SubmitResponse, error) {
// 		if newFakeSubmitStreamClient.RecvCallCount() == 1 {
// 			return &protos.SubmitResponse{
// 				Error:   "",
// 				TraceId: req1.TraceId,
// 			}, nil
// 		}
// 		if newFakeSubmitStreamClient.RecvCallCount() == 2 {
// 			return &protos.SubmitResponse{
// 				Error:   "",
// 				TraceId: req2.TraceId,
// 			}, nil
// 		}
// 		return &protos.SubmitResponse{
// 			Error:   "",
// 			TraceId: []byte{3},
// 		}, nil
// 	}

// 	var reqPool safeReqPool
// 	newFakeSubmitStreamClient.SendStub = func(request *protos.Request) error {
// 		reqPool.append(request)
// 		return nil
// 	}

// 	ctx, cancel = context.WithCancel(context.Background())
// 	newFakeSubmitStreamClient.ContextReturns(ctx)
// 	client.SubmitStreamReturns(newFakeSubmitStreamClient, nil)
// 	newStream, err := faultyStream.renewStream(client, faultyStream.endpoint, faultyStream.logger, ctx, cancel)
// 	require.NoError(t, err)
// 	require.NotNil(t, newStream)
// 	require.False(t, newStream.faulty())

// 	assert.Eventually(t, func() bool {
// 		reqCond := reqPool.length() == 2 && bytes.Equal(reqPool.getElement(0).TraceId, req1.TraceId) && bytes.Equal(reqPool.getElement(1).TraceId, req2.TraceId)
// 		return reqCond && newFakeSubmitStreamClient.SendCallCount() >= 2 && newFakeSubmitStreamClient.RecvCallCount() >= 2
// 	}, 30*time.Second, 10*time.Millisecond)

// 	newStream.close()
// }

// // Scenario:
// // A request is sent, but Send() returns an error
// // The stream is then expected to be reported to the reconnection goroutine in SR
// func TestReconnectRequest(t *testing.T) {
// 	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
// 	fakeSubmitStreamClient.SendReturns(fmt.Errorf("error"))
// 	ctx, cancel := context.WithCancel(context.Background())
// 	logger := testutil.CreateLogger(t, 1)

// 	connectionNumber := 2
// 	streamNumber := 3

// 	s := &stream{
// 		endpoint:                          "127.0.0.1:5017",
// 		logger:                            logger,
// 		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
// 		ctx:                               ctx,
// 		cancelFunc:                        cancel,
// 		requestsChannel:                   make(chan *protos.Request, 10),
// 		doneChannel:                       make(chan bool),
// 		requestTraceIdToResponseChannel:   make(map[string]chan Response),
// 		srReconnectChan:                   make(chan reconnectReq, 20),
// 		connNum:                           connectionNumber,
// 		streamNum:                         streamNumber,
// 	}

// 	go s.sendRequests()

// 	req := &protos.Request{TraceId: []byte("request")}
// 	s.requestsChannel <- req

// 	assert.Eventually(t, func() bool {
// 		return fakeSubmitStreamClient.SendCallCount() == 1 && s.faulty()
// 	}, time.Second, 10*time.Millisecond)

// 	reconnectRequest := <-s.srReconnectChan

// 	require.Equal(t, connectionNumber, reconnectRequest.connNumber)
// 	require.Equal(t, streamNumber, reconnectRequest.streamInConn)
// }

// type safeReqPool struct {
// 	mu      sync.Mutex
// 	reqPool []*protos.Request
// }

// func (srp *safeReqPool) append(request *protos.Request) {
// 	srp.mu.Lock()
// 	defer srp.mu.Unlock()
// 	srp.reqPool = append(srp.reqPool, request)
// }

// func (srp *safeReqPool) length() int {
// 	srp.mu.Lock()
// 	defer srp.mu.Unlock()
// 	return len(srp.reqPool)
// }

// func (srp *safeReqPool) getElement(i int) *protos.Request {
// 	srp.mu.Lock()
// 	defer srp.mu.Unlock()
// 	return srp.reqPool[i]
// }
