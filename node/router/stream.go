package router

import (
	"context"
	"sync"

	"arma/common/types"
	protos "arma/node/protos/comm"
)

type stream struct {
	endpoint string
	logger   types.Logger
	protos.RequestTransmit_SubmitStreamClient
	ctx                             context.Context
	once                            sync.Once
	cancelThisStream                func()
	requests                        chan *protos.Request
	lock                            sync.Mutex
	requestTraceIdToResponseChannel map[string]chan Response
}

// readResponses listens for responses from the batcher.
// If the batcher is alive and a response is received then the request is removed from the map and a response is sent to the shard router.
// If the batcher is not alive, Recv return an error and the cancellation method is applied to mark the stream as faulty.
func (s *stream) readResponses() {
	for {
		resp, err := s.Recv()
		if err != nil {
			s.cancel()
			return
		}

		s.lock.Lock()
		ch, exists := s.requestTraceIdToResponseChannel[string(resp.TraceId)]
		delete(s.requestTraceIdToResponseChannel, string(resp.TraceId))
		s.lock.Unlock()
		if exists {
			ch <- Response{
				SubmitResponse: resp,
			}
		}
	}
}

// sendRequests sends requests to the batcher.
func (s *stream) sendRequests() {
	for {
		msg := <-s.requests
		err := s.Send(msg)
		if err != nil {
			s.logger.Errorf("Failed sending to %s", s.endpoint)
			return
		}
	}
}

func (s *stream) cancel() {
	s.once.Do(s.cancelThisStream)
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
