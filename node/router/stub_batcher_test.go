package router_test

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"arma/common/types"
	"arma/testutil"

	"arma/node/comm"
	"arma/node/comm/tlsgen"
	protos "arma/node/protos/comm"

	"github.com/stretchr/testify/require"
)

type stubBatcher struct {
	ca      tlsgen.CA        // Certificate authority that issues a certificate for the batcher
	server  *comm.GRPCServer // GRPCServer instance represents the batcher
	txs     uint32           // Number of txs received from router
	partyID types.PartyID
	shardID types.ShardID
	logger  types.Logger
}

func NewStubBatcher(t *testing.T, ca tlsgen.CA, partyID types.PartyID, shardID types.ShardID) stubBatcher {
	// create a (cert,key) pair for the batcher
	certKeyPair, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	// create a GRPC Server which will listen for incoming connections on some available port
	server, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: certKeyPair.Cert,
			Key:         certKeyPair.Key,
		},
	})
	require.NoError(t, err)

	// return a stub batcher that includes all server setup
	stubBatcher := stubBatcher{
		ca:      ca,
		server:  server,
		partyID: partyID,
		shardID: shardID,
		logger:  testutil.CreateLogger(t, int(shardID)),
	}
	return stubBatcher
}

func (sb *stubBatcher) Start() {
	protos.RegisterRequestTransmitServer(sb.server.Server(), sb)
	go func() {
		if err := sb.server.Start(); err != nil {
			panic(err)
		}
	}()
}

func (sb *stubBatcher) Stop() {
	sb.server.Stop()
}

func (sb *stubBatcher) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	resp := &protos.SubmitResponse{
		Error:   "",
		ReqID:   request.Identity,
		TraceId: request.TraceId,
	}
	atomic.AddUint32(&sb.txs, 1)
	return resp, nil
}

func (sb *stubBatcher) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp := &protos.SubmitResponse{
			Error:   "",
			ReqID:   req.Identity,
			TraceId: req.TraceId,
		}

		atomic.AddUint32(&sb.txs, 1)

		err = stream.Send(resp)
		if err != nil {
			return err
		}
	}
}

func (sb *stubBatcher) ReceivedMessageCount() uint32 {
	sb.logger.Infof("stub batcher from party %v and shard %v received %v txs\n", sb.partyID, sb.shardID, sb.txs)
	return atomic.LoadUint32(&sb.txs)
}
