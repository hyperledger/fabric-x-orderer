/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"encoding/asn1"
	"fmt"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
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
	partyID       types.PartyID
	endpoint      string
	server        *comm.GRPCServer
	cert          []byte
	key           []byte
	consenterInfo config.ConsenterInfo
	storedBlock   *common.Block
	blockLock     sync.Mutex
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
		partyID:       partyID,
		server:        server,
		cert:          certKeyPair.Cert,
		key:           certKeyPair.Key,
		endpoint:      server.Address(),
		consenterInfo: consenterInfo,
	}

	orderer.RegisterAtomicBroadcastServer(server.Server(), stubConsenter)

	go func() {
		err := server.Start()
		require.NoError(t, err)
	}()

	return stubConsenter
}

func (sc *stubConsenter) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	sc.blockLock.Lock()
	defer sc.blockLock.Unlock()
	return stream.Send(&orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Block{Block: sc.storedBlock},
	})
}

func (sc *stubConsenter) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (sc *stubConsenter) Stop() {
	sc.server.Stop()
}

func (sc *stubConsenter) SetDecision(oba core.OrderedBatchAttestation) {
	ba := oba.(*state.AvailableBatchOrdered)

	proposal := smartbft_types.Proposal{
		Header: (&state.Header{
			Num: ba.OrderingInformation.DecisionNum,
			AvailableBlocks: []state.AvailableBlock{{
				Batch:  ba.AvailableBatch,
				Header: ba.OrderingInformation.BlockHeader,
			}},
		}).Serialize(),
		Payload:  []byte{},
		Metadata: []byte{},
	}

	// Dummy compound signatures
	sigs := [][]byte{{1}, {2}}
	sigBytes, err := asn1.Marshal(sigs)
	if err != nil {
		panic("failed to marshal fake signature: " + err.Error())
	}
	signatures := []smartbft_types.Signature{{Value: sigBytes}}
	bytes := state.DecisionToBytes(proposal, signatures)

	sc.blockLock.Lock()
	defer sc.blockLock.Unlock()
	sc.storedBlock = &common.Block{
		Data: &common.BlockData{Data: [][]byte{bytes}},
	}
}
