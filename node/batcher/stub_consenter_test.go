/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
)

type stubConsenter struct {
	net                *comm.GRPCServer
	stateChan          chan *state.State
	key                []byte
	certificate        []byte
	logger             types.Logger
	complaints         int                  // number of complaints received
	bafs               int                  // number of BAFs received
	receivedEvents     []state.ControlEvent // received control events
	receivedEventsLock sync.RWMutex
}

func NewStubConsenter(t *testing.T, partyID types.PartyID, n *node) *stubConsenter {
	sc := &stubConsenter{
		complaints:  0,
		bafs:        0,
		logger:      testutil.CreateLogger(t, int(partyID)),
		net:         n.GRPCServer,
		key:         n.TLSKey,
		certificate: n.TLSCert,
		stateChan:   make(chan *state.State),
	}

	gRPCServer := n.GRPCServer.Server()
	protos.RegisterConsensusServer(gRPCServer, sc)

	go func() {
		if err := n.GRPCServer.Start(); err != nil {
			panic(err)
		}
	}()

	return sc
}

func (sc *stubConsenter) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var ce state.ControlEvent
		bafd := &state.BAFDeserialize{}
		ce.FromBytes(event.Payload, bafd.Deserialize)

		sc.receivedEventsLock.Lock()
		if ce.Complaint != nil {
			sc.complaints++
		} else {
			sc.bafs++
		}
		sc.receivedEvents = append(sc.receivedEvents, ce)
		sc.receivedEventsLock.Unlock()
	}
}

func (sc *stubConsenter) Stop() {
	// Stop() of stub consenter does nothing
	// use NetStop() to stop the stub consenter network
}

func (sc *stubConsenter) StopNet() {
	sc.net.Stop()
}

func (sc *stubConsenter) Restart() {
	sc.StopNet()
	addr := sc.net.Address()
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

	sc.net = server
	protos.RegisterConsensusServer(sc.net.Server(), sc)
	go func() {
		if err := sc.net.Start(); err != nil {
			panic(err)
		}
	}()
}

// Returns the last received control event
func (sc *stubConsenter) LastControlEvent() *state.ControlEvent {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	return &sc.receivedEvents[len(sc.receivedEvents)-1]
}

func (sc *stubConsenter) BAFCount() int {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	return sc.bafs
}

func (sc *stubConsenter) ComplaintCount() int {
	sc.receivedEventsLock.RLock()
	defer sc.receivedEventsLock.RUnlock()
	return sc.complaints
}

func (sc *stubConsenter) UpdateState(state *state.State) {
	sc.stateChan <- state
}

func (sc *stubConsenter) ReplicateState() <-chan *state.State {
	return sc.stateChan
}

func (sc *stubConsenter) CreateStateConsensusReplicator(conf *config.BatcherNodeConfig, logger types.Logger) batcher.StateReplicator {
	return sc
}
