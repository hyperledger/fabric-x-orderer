/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configack

import (
	"context"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/types"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type ClientID struct {
	NodeType protos.NodeType
	Shard    types.ShardID
}

// ConfigAckHandler handle ConfigAck messages from party members (router, batchers, assembler)
type ConfigAckHandler struct {
	acks   map[ClientID]uint64
	logger *flogging.FabricLogger
	lock   sync.Mutex

	signalChan chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewConfigAckHandler creates a new config ack handler that collects config acknowledgments.
func NewConfigAckHandler(logger *flogging.FabricLogger, shards []types.ShardID) *ConfigAckHandler {
	ctx, cancel := context.WithCancel(context.Background())

	handler := &ConfigAckHandler{
		acks:       make(map[ClientID]uint64),
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		signalChan: make(chan struct{}, 100),
	}

	// fill the map
	handler.initClients(shards)

	return handler
}

func (ca *ConfigAckHandler) initClients(shards []types.ShardID) {
	// Router
	ca.acks[ClientID{
		NodeType: protos.NodeType_ROUTER,
		Shard:    0,
	}] = 0

	// Assembler
	ca.acks[ClientID{
		NodeType: protos.NodeType_ASSEMBLER,
		Shard:    0,
	}] = 0

	// Batchers
	for _, shardID := range shards {
		ca.acks[ClientID{
			NodeType: protos.NodeType_BATCHER,
			Shard:    shardID,
		}] = 0
	}
}

func (ca *ConfigAckHandler) Stop() {
	ca.logger.Infof("config ack handler is stopping")
	ca.cancel()
}

// AddAck records an acknowledgment from a party member.
func (ca *ConfigAckHandler) AddAck(request *protos.ConfigAck) error {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	clientID := ClientID{
		NodeType: request.NodeType,
		Shard:    types.ShardID(request.Shard),
	}

	if uint64(request.ConfigSeq) != ca.acks[clientID]+1 {
		ca.logger.Warnf("config ack has been received on sequence %d but the last acknowledged sequence is %d", request.ConfigSeq, ca.acks[clientID])
		return fmt.Errorf("config ack has been received on sequence %d but the last acknowledged sequence is %d", request.ConfigSeq, ca.acks[clientID])
	}

	ca.acks[clientID] = uint64(request.ConfigSeq)
	ca.logger.Infof("config ack has been received on sequence %d", request.ConfigSeq)

	ca.signalChan <- struct{}{}
	return nil
}

func (ca *ConfigAckHandler) ExpectAck(configSeq uint64) bool {
	for {
		select {
		case <-ca.ctx.Done():
			ca.logger.Infof("config ack handler is stopped")
			return false
		case <-ca.signalChan:
			ca.lock.Lock()
			res := ca.allAcksAtSeq(configSeq)
			ca.lock.Unlock()

			if res {
				return res
			}
		}
	}
}

func (ca *ConfigAckHandler) allAcksAtSeq(seq uint64) bool {
	if len(ca.acks) == 0 {
		return false
	}

	for _, ackSeq := range ca.acks {
		if ackSeq != seq {
			return false
		}
	}

	return true
}
