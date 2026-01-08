/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
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

	deliveryService *batcher.BatcherDeliverService
}

func NewStubBatcher(t *testing.T, shardID types.ShardID, partyID types.PartyID, parties []types.PartyID, ca tlsgen.CA) *stubBatcher {
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
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubBatcher.deliveryService)
	go func() {
		err := server.Start()
		require.NoError(t, err)
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
		if err := sb.server.Start(); err != nil {
			panic(err)
		}
	}()
}

func (sb *stubBatcher) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sb *stubBatcher) SetNextBatch(batch types.Batch) {
	sb.deliveryService.LedgerArray.Append(batch.Primary(), batch.Seq(), 0, batch.Requests())
}
