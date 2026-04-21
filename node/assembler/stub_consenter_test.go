/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"encoding/asn1"
	"fmt"
	"net"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
)

type stubConsenter struct {
	partyID        types.PartyID
	endpoint       string
	server         *comm.GRPCServer
	cert           []byte
	key            []byte
	consenterInfo  config.ConsenterInfo
	decisions      chan *common.Block
	decisionSentCh chan struct{} // decision sent signal
	logger         *flogging.FabricLogger
}

func NewStubConsenter(t *testing.T, partyID types.PartyID, ca tlsgen.CA) *stubConsenter {
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

	consenterInfo := config.ConsenterInfo{
		PartyID:    partyID,
		Endpoint:   server.Address(),
		TLSCACerts: []config.RawBytes{ca.CertBytes()},
	}

	stubConsenter := &stubConsenter{
		partyID:        partyID,
		server:         server,
		cert:           certKeyPair.Cert,
		key:            certKeyPair.Key,
		endpoint:       server.Address(),
		consenterInfo:  consenterInfo,
		decisions:      make(chan *common.Block, 100),
		decisionSentCh: make(chan struct{}, 1),
		logger:         flogging.MustGetLogger(fmt.Sprintf("stub-consenter-P%d", partyID)),
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubConsenter)

	go func() {
		address := server.Address()
		stubConsenter.logger.Infof("StubConsenter network service is starting on %s", address)
		err := server.Start()
		require.NoError(t, err)
		stubConsenter.logger.Infof("StubConsenter network service on %s has been stopped", address)
	}()

	return stubConsenter
}

func (sc *stubConsenter) Stop() {
	sc.server.Stop()
}

func (sc *stubConsenter) Restart() {
	server, err := comm.NewGRPCServer(sc.endpoint, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: sc.cert,
			Key:         sc.key,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to restart gRPC server: %v", err))
	}

	sc.server = server

	orderer.RegisterAtomicBroadcastServer(server.Server(), sc)

	go func() {
		address := server.Address()
		sc.logger.Infof("StubConsenter network service is re-starting on %s", address)
		if err := sc.server.Start(); err != nil {
			panic(err)
		}
		sc.logger.Infof("StubConsenter network service on %s has been stopped", address)
	}()
}

func (sc *stubConsenter) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	for {
		select {
		case b := <-sc.decisions:
			if b == nil {
				return nil
			}
			err := stream.Send(&orderer.DeliverResponse{
				Type: &orderer.DeliverResponse_Block{Block: b},
			})
			if err != nil {
				return err
			}

			sc.decisionSentCh <- struct{}{}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (sc *stubConsenter) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sc *stubConsenter) SetNextDecision(ba *state.AvailableBatchOrdered) {
	proposal := smartbft_types.Proposal{
		Header: (&state.Header{
			Num:                   ba.OrderingInformation.DecisionNum,
			AvailableCommonBlocks: []*common.Block{ba.OrderingInformation.CommonBlock},
		}).Serialize(),
	}

	// Dummy compound signatures
	sigs := [][]byte{{1}, {2}}
	sigBytes, err := asn1.Marshal(sigs)
	if err != nil {
		panic("failed to marshal fake signature: " + err.Error())
	}
	proposalMsg := &state.MessageToSign{
		IdentifierHeader: protoutil.MarshalOrPanic(state.NewIdentifierHeaderOrPanic(1)),
	}
	msg := &state.MessageToSign{
		IdentifierHeader: protoutil.MarshalOrPanic(state.NewIdentifierHeaderOrPanic(1)),
	}
	msgs := [][]byte{proposalMsg.Marshal(), msg.Marshal()}
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
}
