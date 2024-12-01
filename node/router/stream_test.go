package router

import (
	"context"
	"fmt"
	"testing"
	"time"

	"arma/testutil"

	protos "arma/node/protos/comm"
	commMocks "arma/node/protos/comm/mocks"

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
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.SendReturns(nil)
	logger := testutil.CreateLogger(t, 0)

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		requests:                          make(chan *protos.Request, 10),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
	}

	go s.sendRequests()

	req1 := &protos.Request{TraceId: []byte("request1")}
	req2 := &protos.Request{TraceId: []byte("request2")}

	s.requests <- req1
	s.requests <- req2

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.SendCallCount() == 2
	}, time.Second, 10*time.Millisecond)

	sentReq1 := fakeSubmitStreamClient.SendArgsForCall(0)
	require.Equal(t, req1, sentReq1)

	sentReq2 := fakeSubmitStreamClient.SendArgsForCall(1)
	require.Equal(t, req2, sentReq2)
}

func TestSendRequestsReturnsWithError(t *testing.T) {
	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.SendReturns(fmt.Errorf("error"))
	ctx, cancel := context.WithCancel(context.Background())
	logger := testutil.CreateLogger(t, 1)

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requests:                          make(chan *protos.Request, 10),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
	}

	go s.sendRequests()

	req := &protos.Request{TraceId: []byte("request")}
	s.requests <- req

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.SendCallCount() == 1 && s.faulty()
	}, time.Second, 10*time.Millisecond)

	sentReq := fakeSubmitStreamClient.SendArgsForCall(0)
	require.Equal(t, req, sentReq)
}

func TestReadResponses(t *testing.T) {
	reqID := []byte{123}
	traceID := []byte("request123")

	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.RecvReturns(&protos.SubmitResponse{
		Error:   "",
		ReqID:   reqID,
		TraceId: traceID,
	}, nil)
	logger := testutil.CreateLogger(t, 2)

	responseChan := make(chan Response, 1)

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		requests:                          make(chan *protos.Request, 10),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
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

	ctx, cancel := context.WithCancel(context.Background())

	s := &stream{
		endpoint:                          "127.0.0.1:5017",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requests:                          make(chan *protos.Request, 10),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
	}

	go s.readResponses()

	assert.Eventually(t, func() bool {
		return fakeSubmitStreamClient.RecvCallCount() == 1 && s.faulty()
	}, time.Second, 10*time.Millisecond)
}

func TestRenewStreamSuccess(t *testing.T) {
	reqID := []byte{123}
	traceID := []byte("request123")

	fakeSubmitStreamClient := &commMocks.FakeRequestTransmit_SubmitStreamClient{}
	fakeSubmitStreamClient.RecvReturns(&protos.SubmitResponse{
		Error:   "SERVICE_UNAVAILABLE",
		ReqID:   reqID,
		TraceId: traceID,
	}, fmt.Errorf("rpc error: service unavailable"))
	logger := testutil.CreateLogger(t, 4)
	ctx, cancel := context.WithCancel(context.Background())

	faultyStream := &stream{
		endpoint:                          "127.0.0.1:7015",
		logger:                            logger,
		requestTransmitSubmitStreamClient: fakeSubmitStreamClient,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requests:                          make(chan *protos.Request, 10),
		requestTraceIdToResponseChannel:   make(map[string]chan Response),
	}

	faultyStream.cancel()

	client := &commMocks.FakeRequestTransmitClient{}
	client.SubmitStreamReturns(fakeSubmitStreamClient, nil)
	newStream, err := faultyStream.renewStream(client)
	require.NoError(t, err)
	require.NotNil(t, newStream)
	require.Equal(t, faultyStream.getRequests(), newStream.getRequests())
	require.Equal(t, faultyStream.getMap(), newStream.getMap())
}
