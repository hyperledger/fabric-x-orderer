/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"strings"
	"time"

	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
)

//go:generate mockery -dir . -name RPC -case underscore -output mocks

// Egress implementation
type Egress struct {
	Channel  string
	RPC      *RPC
	Logger   *flogging.FabricLogger
	NodeList []uint64
}

// Nodes returns nodes from the runtime config
func (e *Egress) Nodes() []uint64 {
	return e.NodeList
}

// SendConsensus sends the BFT message to the cluster with retry logic for connection errors
func (e *Egress) SendConsensus(targetID uint64, m *protos.Message) {
	const maxRetries = 5
	const retryDelay = 50 * time.Millisecond

	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = e.RPC.SendConsensus(targetID, bftMsgToClusterMsg(m))
		if err == nil {
			return
		}

		// Only retry on connection errors (e.g., during node restarts/reconfigurations)
		if attempt < maxRetries && isConnectionError(err) {
			e.Logger.Debugf("Retry %d/%d sending consensus message to %d: %v", attempt+1, maxRetries, targetID, err)
			time.Sleep(retryDelay)
			continue
		}
		break
	}

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

func bftMsgToClusterMsg(message *protos.Message) *ab.ConsensusRequest {
	return &ab.ConsensusRequest{
		Payload: protoutil.MarshalOrPanic(message),
	}
}

// isConnectionError checks if the error is a connection-related error that should be retried
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection error") ||
		strings.Contains(errStr, "Unavailable")
}
