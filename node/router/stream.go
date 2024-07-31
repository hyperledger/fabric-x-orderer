package router

import (
	"context"
	"sync"

	arma "arma/core"
	protos "arma/node/protos/comm"
)

type stream struct {
	endpoint string
	logger   arma.Logger
	protos.RequestTransmit_SubmitStreamClient
	ctx              context.Context
	once             sync.Once
	cancelThisStream func()
	requests         chan *protos.Request
	lock             sync.Mutex
	m                map[string]chan response
}

func (s *stream) readResponses() {
	for {
		resp, err := s.Recv()
		if err != nil {
			s.cancel()
			return
		}

		s.lock.Lock()
		ch, exists := s.m[string(resp.TraceId)]
		delete(s.m, string(resp.TraceId))
		s.lock.Unlock()
		if exists {
			ch <- response{
				SubmitResponse: resp,
			}
		}
	}
}

func (s *stream) sendRequests() {
	for {
		msg := <-s.requests
		err := s.Send(msg)
		if err != nil {
			s.logger.Errorf("Failed sending to %s", s.endpoint)
		}
	}
}

func (s *stream) cancel() {
	s.once.Do(s.cancelThisStream)
}

func (s *stream) faulty() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *stream) registerReply(traceID []byte, responses chan response) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.m[string(traceID)] = responses
}
