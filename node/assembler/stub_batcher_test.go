/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
)

type stubBatcher struct {
	shardID     types.ShardID
	partyID     types.PartyID
	server      *comm.GRPCServer
	endpoint    string
	cert        []byte
	key         []byte
	batcherInfo config.BatcherInfo
	batches     chan *common.Block
}

func NewStubBatcher(t *testing.T, shardID types.ShardID, partyID types.PartyID, ca tlsgen.CA) *stubBatcher {
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

	batcherInfo := config.BatcherInfo{
		PartyID:    partyID,
		Endpoint:   server.Address(),
		TLSCert:    certKeyPair.Cert,
		TLSCACerts: []config.RawBytes{ca.CertBytes()},
	}

	stubBatcher := &stubBatcher{
		shardID:     shardID,
		partyID:     partyID,
		server:      server,
		endpoint:    server.Address(),
		cert:        certKeyPair.Cert,
		key:         certKeyPair.Key,
		batcherInfo: batcherInfo,
		batches:     make(chan *common.Block, 100),
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubBatcher)
	go func() {
		err := server.Start()
		require.NoError(t, err)
	}()

	return stubBatcher
}

func (sb *stubBatcher) Stop() {
	sb.server.Stop()
}

func (sb *stubBatcher) Shutdown() {
	close(sb.batches)
	sb.server.Stop()
}

func (sb *stubBatcher) Restart() {
	server, err := comm.NewGRPCServer(sb.endpoint, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: sb.cert,
			Key:         sb.key,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sb.server = server

	orderer.RegisterAtomicBroadcastServer(server.Server(), sb)

	go func() {
		if err := sb.server.Start(); err != nil {
			panic(err)
		}
	}()
}

func (sb *stubBatcher) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	for {
		select {
		case b := <-sb.batches:
			if b == nil {
				return nil
			}
			err := stream.Send(&orderer.DeliverResponse{
				Type: &orderer.DeliverResponse_Block{Block: b},
			})
			if err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (sb *stubBatcher) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sb *stubBatcher) SetNextBatch(batch core.Batch) {
	block, _ := ledger.NewFabricBatchFromRequests(sb.partyID, sb.shardID, batch.Seq(), batch.Requests(), []byte(""))
	sb.batches <- (*common.Block)(block)
}
