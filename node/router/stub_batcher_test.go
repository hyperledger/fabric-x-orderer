/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"

	"github.com/stretchr/testify/require"
)

type stubBatcher struct {
	ca           tlsgen.CA // Certificate authority that issues a certificate for the batcher
	certificate  []byte
	key          []byte
	server       *comm.GRPCServer // GRPCServer instance represents the batcher
	txs          uint32           // Number of txs received from router
	partyID      types.PartyID
	shardID      types.ShardID
	logger       types.Logger
	dropRequests bool
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
		ca:          ca,
		certificate: certKeyPair.Cert,
		key:         certKeyPair.Key,
		server:      server,
		partyID:     partyID,
		shardID:     shardID,
		logger:      testutil.CreateLogger(t, int(shardID)),
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

func (sb *stubBatcher) Restart() {
	// save the current server address
	addr := sb.server.Address()

	// create a new gRPC server with the same settings (same address and TLS options)
	server, err := comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: sb.certificate,
			Key:         sb.key,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sb.server = server

	// register the service again and start the new server
	protos.RegisterRequestTransmitServer(sb.server.Server(), sb)
	go func() {
		if err := sb.server.Start(); err != nil {
			panic(err)
		}
	}()
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

		if !sb.dropRequests {
			err = stream.Send(resp)
			if err != nil {
				return err
			}
		}
	}
}

func (sb *stubBatcher) ReceivedMessageCount() uint32 {
	receivedTxs := atomic.LoadUint32(&sb.txs)
	sb.logger.Infof("stub batcher from party %d and shard %d received %d txs\n", sb.partyID, sb.shardID, receivedTxs)
	return receivedTxs
}

func (sb *stubBatcher) GetBatcherEndpoint() string {
	return sb.server.Address()
}

func (sb *stubBatcher) SetDropRequests(dropRequests bool) {
	sb.dropRequests = dropRequests
}
