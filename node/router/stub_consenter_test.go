/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
)

type stubConsenter struct {
	ca          tlsgen.CA // Certificate authority that issues a certificate for the consenter
	certificate []byte
	key         []byte
	server      *comm.GRPCServer // GRPCServer instance represents the consenter
	txs         uint32           // Number of txs received from router
	partyID     types.PartyID
	logger      types.Logger
}

func NewStubConsenter(t *testing.T, ca tlsgen.CA, partyID types.PartyID, logger types.Logger) stubConsenter {
	// create a (cert,key) pair for the consenter
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

	// return a stub consenter that includes all server setup
	stubConsenter := stubConsenter{
		ca:          ca,
		certificate: certKeyPair.Cert,
		key:         certKeyPair.Key,
		server:      server,
		partyID:     partyID,
		logger:      logger,
	}
	return stubConsenter
}

func (sc *stubConsenter) Start() {
	protos.RegisterConsensusServer(sc.server.Server(), sc)
	go func() {
		if err := sc.server.Start(); err != nil {
			panic(err)
		}
	}()
}

func (sc *stubConsenter) Stop() {
	sc.server.Stop()
}

func (sc *stubConsenter) Restart() {
	// save the current server address
	addr := sc.server.Address()

	// create a new gRPC server with the same settings (same address and TLS options)
	server, err := comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: sc.certificate,
			Key:         sc.key,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sc.server = server

	// register the service again and start the new server
	protos.RegisterConsensusServer(sc.server.Server(), sc)
	go func() {
		if err := sc.server.Start(); err != nil {
			panic(err)
		}
	}()
}

func (sc *stubConsenter) ReceivedMessageCount() uint32 {
	receivedTxs := atomic.LoadUint32(&sc.txs)
	return receivedTxs
}

func (sc *stubConsenter) GetConsenterEndpoint() string {
	return sc.server.Address()
}

func (sc *stubConsenter) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	return fmt.Errorf("NotifyEvent not implemented")
}

func (sc *stubConsenter) SubmitConfig(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	resp := &protos.SubmitResponse{
		Error:   "dummy submit config",
		ReqID:   request.Identity,
		TraceId: request.TraceId,
	}
	atomic.AddUint32(&sc.txs, 1)
	return resp, nil
}
