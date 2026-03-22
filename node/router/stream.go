/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type stream struct {
	endpoint                          string
	logger                            *flogging.FabricLogger
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
	reconnectBackoffInterval          time.Duration
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
				s.logger.Infof("failed receiving response from batcher %s, error: %v", s.endpoint, err)
				withBackoff := strings.Contains(err.Error(), "batcher is stopped")
				s.cancelOnServerError(withBackoff)
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
				// withBackoff is set to false because it is not the case of a stopped batcher.
				s.cancelOnServerError(false)
				return
			}

			reqType, err := s.verifyAndClassifyRequest(tr.request)
			if err != nil {
				s.responseToClientWithError(tr, err)
				continue
			}

			if reqType == common.HeaderType_CONFIG_UPDATE {
				s.logger.Infof("received request with type %s, forwarding to consenter", reqType)
				// forward to consenter. configSubmitter will handle sending response to client.
				s.configSubmitter.Forward(tr)
			} else {
				s.logger.Debugf("received request with type %s, forwarding to batcher %s", reqType, s.endpoint)
				err = s.requestTransmitSubmitStreamClient.Send(tr.request)
				if err != nil {
					s.logger.Errorf("Failed sending request to batcher %s", s.endpoint)
					if tr.trace == nil {
						// send error to client, in case request is not traced.
						tr.responses <- Response{err: fmt.Errorf("server error: could not establish connection between router and batcher %s", s.endpoint)}
					}
					// withBackoff is set to false because it is not the case of a stopped batcher.
					s.cancelOnServerError(false)
					return
				}

				// send fast response to client for untraced requests.
				// traced requests get their response from readResponses goroutine.
				if tr.trace == nil {
					tr.responses <- Response{err: nil}
				}
			}
		}
	}
}

// verifyAndClassifyRequest first verifies the request with the RulesVerifier verify method, and then it validates the request structure and type.
func (s *stream) verifyAndClassifyRequest(request *protos.Request) (common.HeaderType, error) {
	var reqType common.HeaderType

	err := s.verifier.Verify(request)
	if err != nil {
		s.logger.Debugf("request is invalid: %s", err)
		return reqType, fmt.Errorf("request verification error: %s", err)
	}

	// verify the request structure and clasify the request
	reqType, err = s.verifier.VerifyStructureAndClassify(request)
	if err != nil {
		s.logger.Debugf("request structure is invalid: %s", err)
		return reqType, fmt.Errorf("request structure verification error: %s", err)
	}

	if int(reqType) >= len(common.HeaderType_value) || reqType == common.HeaderType_CONFIG || reqType == common.HeaderType_ORDERER_TRANSACTION || reqType == common.HeaderType_DELIVER_SEEK_INFO {
		s.logger.Debugf("request has unsupported type: %s", reqType)
		return reqType, fmt.Errorf("request structure verification error: request has unsupported type %s", reqType)
	}

	return reqType, nil
}

func (s *stream) forwardResponseToClient(response *protos.SubmitResponse) error {
	traceID := response.TraceId
	s.lock.Lock()
	ch, exists := s.requestTraceIdToResponseChannel[string(traceID)]
	delete(s.requestTraceIdToResponseChannel, string(traceID))
	s.reconnectBackoffInterval = minRetryInterval
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

// cancelOnServerError is called when send or receive from the batcher returns an error.
// withBackoff parameter is set to true when the error is due to batcher being stopped,
// which requires a backoff before trying to reconnect.
func (s *stream) cancelOnServerError(withBackoff bool) {
	s.cancel()
	s.sendResponseToAllClientsOnError(fmt.Errorf("server error: could not establish connection between router and batcher %s", s.endpoint))
	s.notifyReconnectRoutine(withBackoff)
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
func (s *stream) notifyReconnectRoutine(withBackoff bool) {
	<-s.ctx.Done()
	if withBackoff {
		s.lock.Lock()
		timeToWait := s.reconnectBackoffInterval
		s.reconnectBackoffInterval = min(maxRetryInterval, timeToWait*2)
		s.lock.Unlock()
		s.logger.Debugf("waiting for %s before notifying the reconnection routine about stream with index %d, sIndex: %d", timeToWait, s.connNum, s.streamNum)
		select {
		case <-time.After(timeToWait):
		case <-s.doneChannel:
			s.logger.Debugf("the stream has been closed, reconnection notification is cancelled")
			return
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// if a we got an error that does not require backoff, we reset the backoff interval to the minimum
	if !withBackoff {
		s.reconnectBackoffInterval = minRetryInterval
	}
	if !s.notifiedReconnect {
		s.logger.Debugf("Reporting stream cIndex: %d, sIndex: %d to reconnection goroutine in shard-router", s.connNum, s.streamNum)
		// report the stream to the reconnect routine.
		s.srReconnectChan <- reconnectReq{s.connNum, s.streamNum}
		s.notifiedReconnect = true
	}
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
func (s *stream) renewStream(client protos.RequestTransmitClient, endpoint string, logger *flogging.FabricLogger, ctx context.Context, cancel context.CancelFunc) (*stream, error) {
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
		reconnectBackoffInterval:          s.reconnectBackoffInterval,
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
