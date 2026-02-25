/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package stub

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"go.uber.org/zap"

	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"

	"github.com/stretchr/testify/require"
)

type StubBatcher struct {
	certificate  []byte
	key          []byte
	server       *comm.GRPCServer // GRPCServer instance represents the batcher
	txs          uint32           // Number of txs received from router
	partyID      types.PartyID
	shardID      types.ShardID
	logger       *flogging.FabricLogger
	dropRequests bool
}

func NewStubBatcher(t *testing.T, ca tlsgen.CA, partyID types.PartyID, shardID types.ShardID) StubBatcher {
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
	stubBatcher := StubBatcher{
		certificate: certKeyPair.Cert,
		key:         certKeyPair.Key,
		server:      server,
		partyID:     partyID,
		shardID:     shardID,
		logger:      testutil.CreateLogger(t, int(shardID)),
	}
	return stubBatcher
}

func NewStubBatcherFromConfig(t *testing.T, configStoreDir string, nodeConfigPath string, listener net.Listener) StubBatcher {
	listener.Close()

	localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
	require.NoError(t, err)

	localConfig.NodeLocalConfig.FileStore.Path = configStoreDir
	utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)

	config, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigBatcher", zap.DebugLevel))
	require.NoError(t, err)

	batcherConfig := config.ExtractBatcherConfig(lastConfigBlock)
	require.NotNil(t, batcherConfig)

	server := node.CreateGRPCBatcher(batcherConfig)

	// return a stub batcher that includes all server setup
	stubBatcher := StubBatcher{
		certificate: batcherConfig.TLSCertificateFile,
		key:         batcherConfig.TLSPrivateKeyFile,
		server:      server,
		partyID:     batcherConfig.PartyId,
		shardID:     batcherConfig.ShardId,
		logger:      testutil.CreateLogger(t, int(batcherConfig.ShardId)),
	}
	return stubBatcher
}

func NewStubBatchersAndInfos(t *testing.T, numParties int, shardIDs []types.ShardID) ([]nodeconfig.ShardInfo, func()) {
	var batchers []*StubBatcher
	var batcherInfos []nodeconfig.BatcherInfo
	shardInfos := make([]nodeconfig.ShardInfo, len(shardIDs))

	// prepare CA for each party
	var partiesCAs []tlsgen.CA
	for i := 0; i < numParties; i++ {
		certificateAuthority, err := tlsgen.NewCA()
		require.NoError(t, err)
		partiesCAs = append(partiesCAs, certificateAuthority)
	}

	for idx, shardID := range shardIDs {
		batcherInfos = []nodeconfig.BatcherInfo{}
		for i := 0; i < numParties; i++ {
			batcher := NewStubBatcher(t, partiesCAs[i], types.PartyID(i+1), shardID)
			batchers = append(batchers, &batcher)
			batcherInfo := nodeconfig.BatcherInfo{
				PartyID:    batcher.partyID,
				Endpoint:   batcher.server.Address(),
				TLSCACerts: []nodeconfig.RawBytes{partiesCAs[i].CertBytes()},
				PublicKey:  batcher.key,
				TLSCert:    batcher.certificate,
			}
			batcherInfos = append(batcherInfos, batcherInfo)
		}
		shardInfos[idx] = nodeconfig.ShardInfo{
			ShardId:  shardID,
			Batchers: batcherInfos,
		}
	}

	return shardInfos, func() {
		for _, b := range batchers {
			b.Stop()
		}
	}
}

func (sb *StubBatcher) Server() *comm.GRPCServer {
	return sb.server
}

func (sb *StubBatcher) Start() {
	protos.RegisterRequestTransmitServer(sb.server.Server(), sb)
	go func() {
		if err := sb.server.Start(); err != nil {
			panic(err)
		}
	}()
}

func (sb *StubBatcher) Stop() {
	sb.server.Stop()
}

func (sb *StubBatcher) Restart() {
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

func (sb *StubBatcher) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	resp := &protos.SubmitResponse{
		Error:   "",
		ReqID:   request.Identity,
		TraceId: request.TraceId,
	}
	atomic.AddUint32(&sb.txs, 1)
	return resp, nil
}

func (sb *StubBatcher) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
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

func (sb *StubBatcher) ReceivedMessageCount() uint32 {
	receivedTxs := atomic.LoadUint32(&sb.txs)
	sb.logger.Infof("stub batcher from party %d and shard %d received %d txs\n", sb.partyID, sb.shardID, receivedTxs)
	return receivedTxs
}

func (sb *StubBatcher) GetBatcherEndpoint() string {
	return sb.server.Address()
}

func (sb *StubBatcher) SetDropRequests(dropRequests bool) {
	sb.dropRequests = dropRequests
}
