/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"net"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

const (
	localhost = "127.0.0.1"
)

type stubBatcher struct {
	shardID     types.ShardID
	partyID     types.PartyID
	server      *comm.GRPCServer
	endpoint    string
	cert        []byte
	key         []byte
	batcherInfo config.BatcherInfo
	logger      *flogging.FabricLogger

	deliveryService *batcher.BatcherDeliverService
}

func NewStubBatcher(t *testing.T, shardID types.ShardID, partyID types.PartyID, parties []types.PartyID, ca tlsgen.CA) *stubBatcher {
	certKeyPair, err := ca.NewServerCertKeyPair(localhost)
	require.NoError(t, err)

	// allocate a port using the shared port allocator
	port, listener := testutil.SharedTestPortAllocator().Allocate(t)
	listener.Close()

	// create a GRPC Server which will listen for incoming connections on the allocated port
	server, err := comm.NewGRPCServer(net.JoinHostPort(localhost, port), comm.ServerConfig{
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

	logger := flogging.MustGetLogger(fmt.Sprintf("stub-batcher-S%d-P%d", shardID, partyID))
	ledgerArray, err := node_ledger.NewBatchLedgerArray(shardID, partyID, parties, t.TempDir(), logger)
	if err != nil {
		logger.Panicf("Failed creating BatchLedgerArray: %s", err)
	}

	deliveryService := &batcher.BatcherDeliverService{
		LedgerArray: ledgerArray,
		Logger:      logger,
	}

	stubBatcher := &stubBatcher{
		shardID:         shardID,
		partyID:         partyID,
		server:          server,
		endpoint:        server.Address(),
		cert:            certKeyPair.Cert,
		key:             certKeyPair.Key,
		batcherInfo:     batcherInfo,
		deliveryService: deliveryService,
		logger:          logger,
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubBatcher.deliveryService)
	go func() {
		address := server.Address()
		logger.Infof("StubBatcher network service is starting on %s", address)
		err := server.Start()
		require.NoError(t, err)
		logger.Infof("StubBatcher network service on %s has been stopped", address)
	}()

	return stubBatcher
}

func (sb *stubBatcher) Stop() {
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

	orderer.RegisterAtomicBroadcastServer(server.Server(), sb.deliveryService)

	go func() {
		address := server.Address()
		sb.logger.Infof("StubBatcher network service is re-starting on %s", address)
		if err := sb.server.Start(); err != nil {
			panic(err)
		}
		sb.logger.Infof("StubBatcher network service on %s has been stopped", address)
	}()
}

func (sb *stubBatcher) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sb *stubBatcher) SetNextBatch(batch types.Batch) {
	sb.deliveryService.LedgerArray.Append(batch.Primary(), batch.Seq(), 0, batch.Requests())
}
