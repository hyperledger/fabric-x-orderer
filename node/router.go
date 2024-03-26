package node

import (
	arma "arma/pkg"
	"context"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"io"
)

type Router struct {
	r arma.Router
}

func NewRouter(l arma.Logger, shardCount int) *Router {
	return &Router{
		r: arma.Router{
			RequestToShard: arma.CRC32RequestToShard(uint16(shardCount)),
			Logger:         l,
			Forward: func(shard uint16, request []byte) (arma.BackendError, error) {
				panic("this should be implemented by the caller")
				return nil, nil
			},
		},
	}
}

func (r *Router) SubmitStream(stream protos.Router_SubmitStreamServer) error {
	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	errors := make(chan error, 10)
	go r.sendErrors(stream, exit, errors)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = r.r.Submit(req.Payload)
		errors <- err
	}
}

func (r *Router) sendErrors(stream protos.Router_SubmitStreamServer, exit chan struct{}, errors chan error) {
	for {
		select {
		case <-exit:
			return
		case err := <-errors:
			resp := &protos.SubmitResponse{}
			if err != nil {
				resp.Error = err.Error()
			}
			stream.Send(resp)
		}
	}
}

func (r *Router) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	err := r.r.Submit(request.Payload)
	if err == nil {
		return &protos.SubmitResponse{}, nil
	}
	return &protos.SubmitResponse{Error: err.Error()}, nil
}
