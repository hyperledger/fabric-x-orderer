/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
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
	headerChan         chan *state.Header
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
		headerChan:  make(chan *state.Header),
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

func (sc *stubConsenter) SubmitConfig(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	return nil, fmt.Errorf("not implemented")
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

func (sc *stubConsenter) UpdateState(newState *state.State) {
	header := &state.Header{
		State: newState,
	}
	sc.headerChan <- header
}

func (sc *stubConsenter) UpdateStateHeaderWithConfigBlock(decisionNum types.DecisionNum, availableCommonBlocks []*common.Block, newState *state.State) {
	header := &state.Header{
		Num:                          decisionNum,
		AvailableCommonBlocks:        availableCommonBlocks,
		State:                        newState,
		DecisionNumOfLastConfigBlock: decisionNum,
	}
	sc.headerChan <- header
}

func (sc *stubConsenter) ReplicateDecision() <-chan *state.Header {
	return sc.headerChan
}

func (sc *stubConsenter) CreateDecisionConsensusReplicator(conf *config.BatcherNodeConfig, logger types.Logger, num types.DecisionNum) batcher.DecisionReplicator {
	return sc
}
