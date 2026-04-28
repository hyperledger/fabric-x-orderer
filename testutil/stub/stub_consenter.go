/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package stub

import (
	"context"
	"encoding/asn1"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
)

type StubConsenter struct {
	certificate []byte
	key         []byte
	server      *comm.GRPCServer // GRPCServer instance represents the consenter
	txs         uint32           // Number of txs received from router
	partyID     types.PartyID
	decisions   chan *common.Block
	logger      *flogging.FabricLogger
}

func NewStubConsenter(t *testing.T, ca tlsgen.CA, partyID types.PartyID) StubConsenter {
	// create a (cert,key) pair for the consenter
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

	// return a stub consenter that includes all server setup
	stubConsenter := StubConsenter{
		certificate: certKeyPair.Cert,
		key:         certKeyPair.Key,
		server:      server,
		partyID:     partyID,
		decisions:   make(chan *common.Block, 100),
		logger:      testutil.CreateLogger(t, int(partyID)),
	}
	return stubConsenter
}

func NewStubConsenterFromConfig(t *testing.T, configStoreDir string, nodeConfigPath string, listener net.Listener) *StubConsenter {
	if listener != nil {
		listener.Close()
	}

	localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
	require.NoError(t, err)

	localConfig.NodeLocalConfig.FileStore.Path = configStoreDir
	utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)

	configContent, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
	require.NoError(t, err)

	consenterConfig := configContent.ExtractConsenterConfig(lastConfigBlock)
	require.NotNil(t, consenterConfig)

	server := node_utils.CreateGRPCConsensus(consenterConfig)

	stubConsenter := &StubConsenter{
		partyID:     consenterConfig.PartyId,
		server:      server,
		certificate: consenterConfig.TLSCertificateFile,
		key:         consenterConfig.TLSPrivateKeyFile,
		decisions:   make(chan *common.Block, 100),
		logger:      testutil.CreateLogger(t, int(consenterConfig.PartyId)),
	}

	return stubConsenter
}

func NewStubConsentersAndInfo(t *testing.T, numOfParties int) ([]node_config.ConsenterInfo, func()) {
	var consentersInfo []node_config.ConsenterInfo
	var consenters []StubConsenter
	for i := 0; i < numOfParties; i++ {
		certificateAuthority, err := tlsgen.NewCA()
		require.NoError(t, err)
		node := NewStubConsenter(t, certificateAuthority, types.PartyID(i+1))
		consenters = append(consenters, node)
		consentersInfo = append(consentersInfo, node_config.ConsenterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   node.server.Address(),
			TLSCACerts: []node_config.RawBytes{certificateAuthority.CertBytes()},
			PublicKey:  node.key,
		})
	}

	return consentersInfo, func() {
		for _, c := range consenters {
			c.Stop()
		}
	}
}

func (sc *StubConsenter) Server() *comm.GRPCServer {
	return sc.server
}

func (sc *StubConsenter) Start() {
	protos.RegisterConsensusServer(sc.server.Server(), sc)
	orderer.RegisterAtomicBroadcastServer(sc.server.Server(), sc)
	go func() {
		address := sc.server.Address()
		sc.logger.Infof("StubConsenter network service is starting on %s", address)
		if err := sc.server.Start(); err != nil {
			panic(err)
		}
		sc.logger.Infof("StubConsenter network service on %s has been stopped", address)
	}()
}

func (sc *StubConsenter) Stop() {
	sc.server.Stop()
}

func (sc *StubConsenter) Restart() {
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
	orderer.RegisterAtomicBroadcastServer(sc.server.Server(), sc)
	go func() {
		address := sc.server.Address()
		sc.logger.Infof("StubConsenter network service is re-starting on %s", address)
		if err := sc.server.Start(); err != nil {
			panic(err)
		}
		sc.logger.Infof("StubConsenter network service on %s has been stopped", address)
	}()
}

func (sc *StubConsenter) ReceivedMessageCount() uint32 {
	receivedTxs := atomic.LoadUint32(&sc.txs)
	return receivedTxs
}

func (sc *StubConsenter) GetConsenterEndpoint() string {
	return sc.server.Address()
}

func (sc *StubConsenter) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	return fmt.Errorf("NotifyEvent not implemented")
}

func (sc *StubConsenter) SubmitConfig(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	resp := &protos.SubmitResponse{
		Error:   "dummy submit config",
		ReqID:   request.Identity,
		TraceId: request.TraceId,
	}
	atomic.AddUint32(&sc.txs, 1)
	return resp, nil
}

func (sc *StubConsenter) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	for {
		select {
		case b := <-sc.decisions:
			if b == nil {
				return nil
			}
			err := server.Send(&orderer.DeliverResponse{
				Type: &orderer.DeliverResponse_Block{Block: b},
			})
			if err != nil {
				return err
			}
		case <-server.Context().Done():
			return server.Context().Err()
		}
	}
}

func (sc *StubConsenter) Broadcast(server orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sc *StubConsenter) DeliverDecisionFromHeader(header *state.Header) error {
	proposal := smartbft_types.Proposal{
		Header: header.Serialize(),
	}

	bytes := state.ProposalToBytes(proposal)

	sc.decisions <- &common.Block{
		Header: &common.BlockHeader{}, // dummy header - maybe fill if needed
		Data:   &common.BlockData{Data: [][]byte{bytes}},
	}
	return nil
}

func (sc *StubConsenter) DeliverConfigDecisionFromBA(ba *state.AvailableBatchOrdered, s *state.State) error {
	proposal := smartbft_types.Proposal{
		Header: (&state.Header{
			Num:                          ba.OrderingInformation.DecisionNum,
			DecisionNumOfLastConfigBlock: ba.OrderingInformation.DecisionNum,
			AvailableCommonBlocks:        []*common.Block{ba.OrderingInformation.CommonBlock},
			State:                        s,
		}).Serialize(),
	}

	// Dummy compound signatures
	sigs := [][]byte{{1}, {2}}
	sigBytes, err := asn1.Marshal(sigs)
	if err != nil {
		panic("failed to marshal fake signature: " + err.Error())
	}
	proposalMsg := &protoutil.MessageToSign{
		IdentifierHeader: protoutil.MarshalOrPanic(protoutil.NewIdentifierHeaderOrPanic(1)),
	}
	msg := &protoutil.MessageToSign{
		IdentifierHeader: protoutil.MarshalOrPanic(protoutil.NewIdentifierHeaderOrPanic(1)),
	}
	msgs := [][]byte{proposalMsg.ASN1MarshalOrPanic(), msg.ASN1MarshalOrPanic()}
	msgsBytes, err := asn1.Marshal(msgs)
	if err != nil {
		panic("failed to marshal fake signature msgs: " + err.Error())
	}
	signatures := []smartbft_types.Signature{{Value: sigBytes, Msg: msgsBytes}}
	bytes := state.ProposalToBytes(proposal)
	block := &common.Block{
		Header: ba.OrderingInformation.CommonBlock.Header,
		Data:   &common.BlockData{Data: [][]byte{bytes}},
	}
	protoutil.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)
	sc.decisions <- block
	return nil
}
