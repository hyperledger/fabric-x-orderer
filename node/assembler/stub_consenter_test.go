/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"encoding/asn1"
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"

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
}

func NewStubConsenter(t *testing.T, partyID types.PartyID, ca tlsgen.CA) *stubConsenter {
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
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubConsenter)

	go func() {
		err := server.Start()
		require.NoError(t, err)
	}()

	return stubConsenter
}

func (sc *stubConsenter) Stop() {
	sc.server.Stop()
}

func (sc *stubConsenter) Shutdown() {
	close(sc.decisions)
	close(sc.decisionSentCh)
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
		if err := sc.server.Start(); err != nil {
			panic(err)
		}
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
			Num: ba.OrderingInformation.DecisionNum,
			AvailableBlocks: []state.AvailableBlock{{
				Batch:  ba.AvailableBatch,
				Header: ba.OrderingInformation.BlockHeader,
			}},
		}).Serialize(),
	}

	// Dummy compound signatures
	sigs := [][]byte{{1}, {2}}
	sigBytes, err := asn1.Marshal(sigs)
	if err != nil {
		panic("failed to marshal fake signature: " + err.Error())
	}
	signatures := []smartbft_types.Signature{{Value: sigBytes}}
	bytes := state.DecisionToBytes(proposal, signatures)

	sc.decisions <- &common.Block{
		Header: &common.BlockHeader{
			Number:       uint64(ba.OrderingInformation.DecisionNum),
			PreviousHash: ba.OrderingInformation.PrevHash,
		},
		Data: &common.BlockData{Data: [][]byte{bytes}},
	}
}
