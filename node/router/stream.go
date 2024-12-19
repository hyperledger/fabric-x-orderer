package router

import (
	"context"
	"fmt"
	"sync"

	"arma/common/types"
	protos "arma/node/protos/comm"
)

type stream struct {
	endpoint                          string
	logger                            types.Logger
	requestTransmitSubmitStreamClient protos.RequestTransmit_SubmitStreamClient
	ctx                               context.Context
	cancelOnce                        sync.Once
	cancelFunc                        func()
	requestsChannel                   chan *protos.Request
	doneChannel                       chan bool
	doneOnce                          sync.Once
	lock                              sync.Mutex
	requestTraceIdToResponseChannel   map[string]chan Response
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
				s.cancel()
				return
			}

			s.lock.Lock()
			ch, exists := s.requestTraceIdToResponseChannel[string(resp.TraceId)]
			delete(s.requestTraceIdToResponseChannel, string(resp.TraceId))
			s.lock.Unlock()
			if exists {
				s.logger.Debugf("read response from batcher %s on request with trace id %x", s.endpoint, resp.TraceId)
				s.logger.Debugf("registration for request with trace id %x was removed upon receiving a response", resp.TraceId)
				ch <- Response{
					SubmitResponse: resp,
				}
			} else {
				s.logger.Debugf("received a response from batcher %v for a request with trace id %x, which does not exist in the map, dropping response", s.endpoint, resp.TraceId)
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
		case msg, ok := <-s.requestsChannel:
			if !ok {
				s.logger.Debugf("request channel to batcher %s have been closed", s.endpoint)
				s.cancel()
				return
			}
			s.logger.Debugf("send request with trace id %x to batcher %s", msg.TraceId, s.endpoint)
			err := s.requestTransmitSubmitStreamClient.Send(msg)
			if err != nil {
				s.logger.Errorf("Failed sending request to batcher %s", s.endpoint)
				s.cancel()
				return
			}
		}
	}
}

func (s *stream) cancel() {
	s.cancelOnce.Do(s.cancelFunc)
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
	s.lock.Lock()
	defer s.lock.Unlock()

	newGRPCStream, err := client.SubmitStream(ctx)
	if err != nil {
		s.logger.Errorf("Failed establishing a new stream")
		cancel()
		return nil, fmt.Errorf("failed establishing a new stream: %w", err)
	}

	newStreamRequests := make(chan *protos.Request, 1000)
	newRequestTraceIdToResponseChannelMap := make(map[string]chan Response)

	elem := len(s.requestsChannel)
	for i := 0; i < elem; i++ {
		req := <-s.requestsChannel
		newStreamRequests <- req
	}
	s.close() // close the old stream

	for traceId, respChan := range s.requestTraceIdToResponseChannel {
		newRequestTraceIdToResponseChannelMap[traceId] = respChan
	}

	newStream := &stream{
		endpoint:                          endpoint,
		logger:                            logger,
		requestTransmitSubmitStreamClient: newGRPCStream,
		ctx:                               ctx,
		cancelFunc:                        cancel,
		requestTraceIdToResponseChannel:   newRequestTraceIdToResponseChannelMap,
		requestsChannel:                   newStreamRequests,
		doneChannel:                       make(chan bool),
	}

	newStream.logger.Infof("renew stream")
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

// getMap is only used for testing
func (s *stream) getMap() map[string]chan Response {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.requestTraceIdToResponseChannel
}

// getRequests is only used for testing
func (s *stream) getRequests() chan *protos.Request {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.requestsChannel
}
