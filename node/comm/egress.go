/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"sync"

	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

//go:generate mockery -dir . -name RPC -case underscore -output mocks

// Egress implementation
type Egress struct {
	Channel  string
	RPC      *RPC
	Logger   *flogging.FabricLogger
	NodeList []uint64
	lock     sync.RWMutex
}

// Nodes returns nodes from the runtime config
func (e *Egress) Nodes() []uint64 {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.NodeList
}

// SendConsensus sends the BFT message to the cluster
func (e *Egress) SendConsensus(targetID uint64, m *protos.Message) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	err := e.RPC.SendConsensus(targetID, bftMsgToClusterMsg(m))
	if err != nil {
		e.Logger.Warnf("Failed sending to %d: %v", targetID, err)
	}
}

// SendTransaction sends the transaction to the cluster
func (e *Egress) SendTransaction(targetID uint64, request []byte) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	msg := &ab.SubmitRequest{
		Payload: &cb.Envelope{Payload: request},
	}

	report := func(err error) {
		if err != nil {
			e.Logger.Warnf("Failed sending transaction to %d: %v", targetID, err)
		}
	}
	e.RPC.SendSubmit(targetID, msg, report)
}

func bftMsgToClusterMsg(message *protos.Message) *ab.ConsensusRequest {
	return &ab.ConsensusRequest{
		Payload: protoutil.MarshalOrPanic(message),
	}
}

// Reconfigure updates the list of nodes and reconfigures the RPC
func (e *Egress) Reconfigure(nodes []uint64, comm *AuthCommMgr) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.RPC.Reconfigure(comm)
	e.NodeList = nodes
}
