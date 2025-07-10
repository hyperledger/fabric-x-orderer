/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"sync"
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
	storedBatch *common.Block
	blockLock   sync.Mutex
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

func (sb *stubBatcher) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sb *stubBatcher) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	sb.blockLock.Lock()
	defer sb.blockLock.Unlock()
	return stream.Send(&orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Block{Block: sb.storedBatch},
	})
}

func (sb *stubBatcher) SetNextBatch(batch core.Batch) {
	block, _ := ledger.NewFabricBatchFromRequests(sb.partyID, sb.shardID, batch.Seq(), batch.Requests(), []byte(""))
	sb.blockLock.Lock()
	defer sb.blockLock.Unlock()
	sb.storedBatch = (*common.Block)(block)
}
