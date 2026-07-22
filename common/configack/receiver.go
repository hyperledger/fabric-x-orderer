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

type ClientRole struct {
	NodeType protos.NodeType
	Shard    types.ShardID
}

func (c *ClientRole) String() string {
	switch c.NodeType {
	case protos.NodeType_ROUTER:
		return "router"
	case protos.NodeType_ASSEMBLER:
		return "assembler"
	case protos.NodeType_BATCHER:
		return fmt.Sprintf("batcher, shard=%d", c.Shard)
	default:
		return fmt.Sprintf("node_type=%s shard=%d", c.NodeType.String(), c.Shard)
	}
}

// Receiver handle ConfigAck messages from party members (router, batchers, assembler)
type Receiver struct {
	lock   sync.Mutex
	acks   map[ClientRole]uint64 // acks maps each client role to the latest config sequence it has acknowledged
	logger *flogging.FabricLogger

	signalChan chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewReceiver creates a new config ack handler that collects config acknowledgments.
func NewReceiver(logger *flogging.FabricLogger, shards []types.ShardID) *Receiver {
	ctx, cancel := context.WithCancel(context.Background())

	receiver := &Receiver{
		acks:       make(map[ClientRole]uint64),
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		signalChan: make(chan struct{}, 100),
	}

	// fill the map
	receiver.initClients(shards)

	return receiver
}

func (r *Receiver) initClients(shards []types.ShardID) {
	// Router
	r.acks[ClientRole{
		NodeType: protos.NodeType_ROUTER,
		Shard:    0,
	}] = 0

	// Assembler
	r.acks[ClientRole{
		NodeType: protos.NodeType_ASSEMBLER,
		Shard:    0,
	}] = 0

	// Batchers
	for _, shardID := range shards {
		r.acks[ClientRole{
			NodeType: protos.NodeType_BATCHER,
			Shard:    shardID,
		}] = 0
	}
}

func (r *Receiver) Stop() {
	r.logger.Infof("config ack handler is stopping")
	r.cancel()
}

// AddAck records an acknowledgment from a party member.
func (r *Receiver) AddAck(request *protos.ConfigAck) error {
	if request == nil {
		return fmt.Errorf("config ack request is nil")
	}

	clientRole := ClientRole{
		NodeType: request.NodeType,
		Shard:    types.ShardID(request.Shard),
	}

	if clientRole.NodeType != protos.NodeType_ROUTER && clientRole.NodeType != protos.NodeType_BATCHER && clientRole.NodeType != protos.NodeType_ASSEMBLER {
		return fmt.Errorf("unknown node type: %s", clientRole.NodeType)
	}

	r.lock.Lock()
	lastSeq, exists := r.acks[clientRole]
	if !exists {
		r.lock.Unlock()
		r.logger.Warnf("config ack received from unknown client: %s", clientRole.String())
		return fmt.Errorf("config ack received from unknown client: %s", clientRole.String())
	}

	if uint64(request.ConfigSeq) <= lastSeq {
		r.lock.Unlock()
		r.logger.Warnf("config ack has been received from %s on sequence %d but the last acknowledged sequence is %d", clientRole.String(), request.ConfigSeq, r.acks[clientRole])
		return fmt.Errorf("config ack has been received from %s on sequence %d but the last acknowledged sequence is %d", clientRole.String(), request.ConfigSeq, r.acks[clientRole])
	}

	r.acks[clientRole] = uint64(request.ConfigSeq)
	r.logger.Infof("config ack has been received on sequence %d from %s", request.ConfigSeq, clientRole.String())
	r.lock.Unlock()

	r.signalChan <- struct{}{}
	return nil
}

func (r *Receiver) WaitForAllAcks(timeoutCtx context.Context, configSeq uint64) bool {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Infof("config ack handler is stopped")
			return false
		case <-timeoutCtx.Done():
			r.logger.Infof("waiting for acknowledgments from all nodes timed out")
			return false
		case <-r.signalChan:
			r.lock.Lock()
			res := r.allAcksAtSeq(configSeq)
			r.lock.Unlock()

			if res {
				return res
			}
		}
	}
}

func (r *Receiver) allAcksAtSeq(seq uint64) bool {
	if len(r.acks) == 0 {
		return false
	}

	for _, ackSeq := range r.acks {
		if ackSeq != seq {
			return false
		}
	}

	return true
}
