/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	protos "github.com/SmartBFT-Go/consensus/v2/smartbftprotos"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/protoutil"
)

//go:generate mockery -dir . -name RPC -case underscore -output mocks

// Logger specifies the logger
type Logger interface {
	Debugf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

// Egress implementation
type Egress struct {
	Channel  string
	RPC      *RPC
	Logger   Logger
	NodeList []uint64
}

// Nodes returns nodes from the runtime config
func (e *Egress) Nodes() []uint64 {
	return e.NodeList
}

// SendConsensus sends the BFT message to the cluster
func (e *Egress) SendConsensus(targetID uint64, m *protos.Message) {
	err := e.RPC.SendConsensus(targetID, bftMsgToClusterMsg(m, e.Channel))
	if err != nil {
		e.Logger.Warnf("Failed sending to %d: %v", targetID, err)
	}
}

// SendTransaction sends the transaction to the cluster
func (e *Egress) SendTransaction(targetID uint64, request []byte) {
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

func bftMsgToClusterMsg(message *protos.Message, channel string) *ab.ConsensusRequest {
	return &ab.ConsensusRequest{
		Payload: protoutil.MarshalOrPanic(message),
	}
}
