/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	localhost = "127.0.0.1"
)

type stubBatcher struct {
	shardID  types.ShardID
	partyID  types.PartyID
	server   *comm.GRPCServer
	endpoint string
	cert     []byte
	key      []byte
	caCert   []byte
	// clientRootCAs are the CAs that issue the client certificates the stub accepts.
	clientRootCAs [][]byte
	batcherInfo   config.BatcherInfo
	ledgerArray   *node_ledger.BatchLedgerArray
	logger        *flogging.FabricLogger

	// mutex guards deliveryService, which a configuration update replaces.
	mutex           sync.RWMutex
	deliveryService *batcher.BatcherDeliverService
}

// NewStubBatcher starts a batcher deliver service over an empty ledger, serving the nodes of bundle.
func NewStubBatcher(t *testing.T, shardID types.ShardID, partyID types.PartyID, parties []types.PartyID, ca tlsgen.CA, bundle channelconfig.Resources) *stubBatcher {
	certKeyPair, err := ca.NewServerCertKeyPair(localhost)
	require.NoError(t, err)

	clientRootCAs := clientRootCAs(t, ca.CertBytes(), bundle)

	// allocate a port using the shared port allocator
	port, listener := testutil.SharedTestPortAllocator().Allocate(t)
	listener.Close()

	// create a GRPC Server which will listen for incoming connections on the allocated port
	server, err := comm.NewGRPCServer(net.JoinHostPort(localhost, port), comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			Certificate:       certKeyPair.Cert,
			Key:               certKeyPair.Key,
			RequireClientCert: true,
			ClientRootCAs:     clientRootCAs,
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
	ledgerMetrics := &node_ledger.BatchLedgerMetrics{}
	ledgerMetrics.NewBatchLedgerMetrics(&disabled.Provider{}, fmt.Sprintf("%d", partyID), fmt.Sprintf("%d", shardID))
	ledgerArray, err := node_ledger.NewBatchLedgerArray(shardID, partyID, parties, "arma", t.TempDir(), logger, ledgerMetrics)
	if err != nil {
		logger.Panicf("Failed creating BatchLedgerArray: %s", err)
	}

	accessControl, err := deliver.NewBatcherDeliverVerifier(bundle, shardID)
	require.NoError(t, err)

	stubBatcher := &stubBatcher{
		shardID:       shardID,
		partyID:       partyID,
		server:        server,
		endpoint:      server.Address(),
		cert:          certKeyPair.Cert,
		key:           certKeyPair.Key,
		caCert:        ca.CertBytes(),
		clientRootCAs: clientRootCAs,
		batcherInfo:   batcherInfo,
		ledgerArray:   ledgerArray,
		logger:        logger,
		deliveryService: &batcher.BatcherDeliverService{
			LedgerArray:   ledgerArray,
			AccessControl: accessControl,
			Logger:        logger,
		},
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubBatcher)
	go func() {
		address := server.Address()
		logger.Infof("StubBatcher network service is starting on %s", address)
		err := server.Start()
		require.NoError(t, err)
		logger.Infof("StubBatcher network service on %s has been stopped", address)
	}()

	return stubBatcher
}

// clientRootCAs returns caCert with the TLS CA certificates of the parties of the shared configuration.
func clientRootCAs(t *testing.T, caCert []byte, bundle channelconfig.Resources) [][]byte {
	t.Helper()

	ordererConfig, exists := bundle.OrdererConfig()
	require.True(t, exists)

	sharedConfig := &ordererpb.SharedConfig{}
	require.NoError(t, proto.Unmarshal(ordererConfig.ConsensusMetadata(), sharedConfig))

	roots := [][]byte{caCert}
	for _, party := range sharedConfig.GetPartiesConfig() {
		roots = append(roots, party.GetTLSCACerts()...)
	}

	return roots
}

func (sb *stubBatcher) Stop() {
	sb.server.Stop()
}

func (sb *stubBatcher) Restart() {
	server, err := comm.NewGRPCServer(sb.endpoint, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			Certificate:       sb.cert,
			Key:               sb.key,
			RequireClientCert: true,
			ClientRootCAs:     sb.clientRootCAs,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sb.server = server

	orderer.RegisterAtomicBroadcastServer(server.Server(), sb)

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

func (sb *stubBatcher) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	sb.mutex.RLock()
	deliveryService := sb.deliveryService
	sb.mutex.RUnlock()

	return deliveryService.Deliver(stream)
}

// UpdateAccessControl rebuilds the access control of the stub from bundle, so that a test which adds
// a party can let its nodes pull.
func (sb *stubBatcher) UpdateAccessControl(t *testing.T, bundle channelconfig.Resources) {
	t.Helper()

	accessControl, err := deliver.NewBatcherDeliverVerifier(bundle, sb.shardID)
	require.NoError(t, err)

	sb.clientRootCAs = clientRootCAs(t, sb.caCert, bundle)
	require.NoError(t, sb.server.SetClientRootCAs(sb.clientRootCAs))

	sb.mutex.Lock()
	defer sb.mutex.Unlock()

	sb.deliveryService = &batcher.BatcherDeliverService{
		LedgerArray:   sb.ledgerArray,
		AccessControl: accessControl,
		Logger:        sb.logger,
	}
}

func (sb *stubBatcher) SetNextBatch(batch types.Batch) {
	sb.ledgerArray.Append(batch.Primary(), batch.Seq(), 0, batch.Requests(), batch.Digest(), batch.PrimarySignature())
}
