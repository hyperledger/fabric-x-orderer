/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type stream struct {
	endpoint                          string
	logger                            types.Logger
	requestTransmitSubmitStreamClient protos.RequestTransmit_SubmitStreamClient
	ctx                               context.Context
	cancelOnce                        sync.Once
	cancelFunc                        func()
	requestsChannel                   chan *TrackedRequest
	doneChannel                       chan bool
	doneOnce                          sync.Once
	lock                              sync.Mutex
	requestTraceIdToResponseChannel   map[string]chan Response
	connNum                           int
	streamNum                         int
	srReconnectChan                   chan reconnectReq
	notifiedReconnect                 bool
	verifier                          *requestfilter.RulesVerifier
	configSubmitter                   ConfigurationSubmitter
}

// readResponses listens for responses from the batcher.
// If the batcher is alive and a response is received then the request is removed from the map and a response is sent to the shard router.
// If the batcher is not alive, Recv return an error and the cancellation method is applied to mark the stream as faulty.
func (s *stream) readResponses() {
	for {
		select {
		case <-s.doneChannel:
			s.logger.Debugf("the stream has been closed, readResponses goroutine is exiting, it was connected to batcher %s ", s.endpoint)
			return
		default:
			resp, err := s.requestTransmitSubmitStreamClient.Recv()
			if err != nil {
				s.logger.Debugf("failed receiving response from batcher %s", s.endpoint)
				s.cancelOnServerError()
				return
			}
			s.logger.Debugf("read response from batcher %s on request with trace id %x", s.endpoint, resp.TraceId)
			err = s.forwardResponseToClient(resp)
			if err != nil {
				s.logger.Debugf("received a response from batcher %s for a request with trace id %x, which does not exist in the map, dropping response", s.endpoint, resp.TraceId)
			}
		}
	}
}

// sendRequests sends requests to the batcher.
func (s *stream) sendRequests() {
	for {
		select {
		case <-s.doneChannel:
			s.logger.Debugf("the stream has been closed, sendRequests goroutine is exiting, it was connected to batcher %s", s.endpoint)
			return
		case tr, ok := <-s.requestsChannel:
			if !ok {
				s.logger.Debugf("request channel to batcher %s have been closed", s.endpoint)
				s.cancelOnServerError()
				return
			}
			// verify the request
			if requestType, err := s.verifier.Verify(tr.request); err != nil {
				s.logger.Debugf("request is invalid: %s", err)
				// send error to the client
				s.responseToClientWithError(tr, fmt.Errorf("request verification error: %s", err))
			} else {
				switch requestType {
				case "data":
					s.logger.Debugf("received request with type %s, forwarding to batcher %s", requestType, s.endpoint)
					err := s.requestTransmitSubmitStreamClient.Send(tr.request)
					if err != nil {
						s.logger.Errorf("Failed sending request to batcher %s", s.endpoint)
						if tr.trace == nil {
							// send error to client, in case request is not traced.
							tr.responses <- Response{err: fmt.Errorf("server error: could not establish connection between router and batcher %s", s.endpoint)}
						}
						s.cancelOnServerError()
						return
					}
					// fast response to client
					if tr.trace == nil {
						// request is untraced, send no-error to client.
						tr.responses <- Response{err: nil}
					}
				case "config_update":
					s.logger.Debugf("received request with type %s, forwarding to consenter", requestType)
					s.configSubmitter.Forward(tr)
				default:
					s.logger.Errorf("received request with unsupported type %s", requestType)
					s.responseToClientWithError(tr, fmt.Errorf("received request with unsupported type: %s", requestType))
				}
			}
		}
	}
}

func (s *stream) forwardResponseToClient(response *protos.SubmitResponse) error {
	traceID := response.TraceId
	s.lock.Lock()
	ch, exists := s.requestTraceIdToResponseChannel[string(traceID)]
	delete(s.requestTraceIdToResponseChannel, string(traceID))
	s.lock.Unlock()
	if exists {
		s.logger.Debugf("registration for request with trace id %x was removed upon receiving a response", traceID)
		ch <- Response{
			SubmitResponse: response,
		}
		return nil
	} else {
		return fmt.Errorf("request with traceID %x is not in map", traceID)
	}
}

func (s *stream) responseToClientWithError(rr *TrackedRequest, err error) {
	traceID := rr.trace
	if traceID == nil {
		// request is untraced, send a response
		rr.responses <- Response{err: err}
	} else {
		// request is traced, and there was an error
		s.lock.Lock()
		ch, exists := s.requestTraceIdToResponseChannel[string(traceID)]
		delete(s.requestTraceIdToResponseChannel, string(traceID))
		s.lock.Unlock()
		if exists {
			s.logger.Debugf("responding to request with trace id %x, and removing registration from map", traceID)
			ch <- Response{
				SubmitResponse: &protos.SubmitResponse{Error: err.Error(), TraceId: traceID},
			}
		} else {
			s.logger.Debugf("request with traceID %x is not in map", traceID)
		}
	}
}

func (s *stream) cancelOnServerError() {
	s.cancel()
	s.sendResponseToAllClientsOnError(fmt.Errorf("server error: could not establish connection between router and batcher %s", s.endpoint))
	s.notifyReconnectRoutine()
}

func (s *stream) cancel() {
	s.cancelOnce.Do(s.cancelFunc)
}

// Send an error to all clients that are still waiting for a response
func (s *stream) sendResponseToAllClientsOnError(e error) {
	s.lock.Lock()
	s.logger.Debugf("Sending error %s to all response channels registerd in stream ", e)
	for _, respChan := range s.requestTraceIdToResponseChannel {
		respChan <- Response{
			err: e,
		}
	}
	clear(s.requestTraceIdToResponseChannel)

	// Drain the requests channel. it could block another reader (none are expected)
DrainChannelLoop:
	for {
		select {
		case rr := <-s.requestsChannel:
			rr.responses <- Response{err: e, reqID: rr.reqID}
		default:
			break DrainChannelLoop
		}
	}

	s.lock.Unlock()
}

// Here we notify the reconnect goroutine in the shard router that this stream need to be reconnected. However, we do it
// only when its context is marked done, to avoid a race in the reconnection routine where s.faulty() check could be faule.
func (s *stream) notifyReconnectRoutine() {
	<-s.ctx.Done()
	s.lock.Lock()
	if !s.notifiedReconnect {
		s.logger.Debugf("Reporting stream cIndex: %d, sIndex: %d to reconnection goroutine in shard-router", s.connNum, s.streamNum)
		s.srReconnectChan <- reconnectReq{s.connNum, s.streamNum}
		s.notifiedReconnect = true
	}
	s.lock.Unlock()
}

// faulty returns true if the stream is faulty, else return false.
// The stream is marked as faulty by applying the ctx cancellation function when the batcher is unresponsive.
func (s *stream) faulty() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *stream) registerReply(traceID []byte, responses chan Response) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.requestTraceIdToResponseChannel[string(traceID)] = responses
}

// renewStream creates a new stream that inherits the map and the requests channel
func (s *stream) renewStream(client protos.RequestTransmitClient, endpoint string, logger types.Logger, ctx context.Context, cancel context.CancelFunc) (*stream, error) {
	newGRPCStream, err := client.SubmitStream(ctx)
	if err != nil {
		s.logger.Errorf("Failed establishing a new stream")
		cancel()
		return nil, fmt.Errorf("failed establishing a new stream: %w", err)
	}

	newStreamRequests := make(chan *TrackedRequest, 1000)
	newRequestTraceIdToResponseChannelMap := make(map[string]chan Response)

	// close the old stream. This should stop the sendRequests and readResponses goroutines.
	s.close()

	// Move requests from the old channel to the new channel. There could be another reader (readResponses)
	// so it is done with a select-loop, and not by counting and moving.
CopyChannelLoop:
	for {
		select {
		case req := <-s.requestsChannel:
			newStreamRequests <- req
		default:
			break CopyChannelLoop
		}
	}

	s.lock.Lock()
	// copy the response-Channels map
	maps.Copy(newRequestTraceIdToResponseChannelMap, s.requestTraceIdToResponseChannel)

	newStream := &stream{
		endpoint:                          endpoint,
		logger:                            logger,
		requestTransmitSubmitStreamClient: newGRPCStream,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestTraceIdToResponseChannel:   newRequestTraceIdToResponseChannelMap,
		requestsChannel:                   newStreamRequests,
		doneChannel:                       make(chan bool),
		connNum:                           s.connNum,
		streamNum:                         s.streamNum,
		srReconnectChan:                   s.srReconnectChan,
		notifiedReconnect:                 false,
		verifier:                          s.verifier,
		configSubmitter:                   s.configSubmitter,
	}
	s.lock.Unlock()

	go newStream.sendRequests()
	go newStream.readResponses()

	return newStream, nil
}

// isRequestRegistered is only used for testing to validate that a request has been recorded in the map
func (s *stream) isRequestRegistered(traceID []byte) (chan Response, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	respChan, exists := s.requestTraceIdToResponseChannel[string(traceID)]
	return respChan, exists
}

// close closes the read and send channels
func (s *stream) close() {
	s.doneOnce.Do(func() {
		s.cancel()
		close(s.doneChannel)
	})
}
