/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"

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

func (sc *stubConsenter) SendBlockFromOBA(oba core.OrderedBatchAttestation) {
	ba := oba.(*state.AvailableBatchOrdered)
	h := &state.Header{
		Num: types.DecisionNum(0),
		AvailableBlocks: []state.AvailableBlock{{
			Batch:  ba.AvailableBatch,
			Header: ba.OrderingInformation.BlockHeader,
		}},
	}

	data := createSerializedDecision(h.Serialize())
	sc.blockLock.Lock()
	defer sc.blockLock.Unlock()
	sc.storedBlock = &common.Block{
		Data: &common.BlockData{Data: [][]byte{data}},
	}
}

func createSerializedDecision(header []byte) []byte {
	buf := make([]byte, 12+len(header))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(header))) // header
	binary.BigEndian.PutUint32(buf[4:8], 0)                   // payload
	binary.BigEndian.PutUint32(buf[8:12], 0)                  // metadata
	copy(buf[12:], header)
	return buf
}
