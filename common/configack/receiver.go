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

// Receiver handle ConfigAck messages from party members (router, batchers, assembler)
type Receiver struct {
	acks   map[ClientID]uint64
	logger *flogging.FabricLogger
	lock   sync.Mutex

	signalChan chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewReceiver creates a new config ack handler that collects config acknowledgments.
func NewReceiver(logger *flogging.FabricLogger, shards []types.ShardID) *Receiver {
	ctx, cancel := context.WithCancel(context.Background())

	receiver := &Receiver{
		acks:       make(map[ClientID]uint64),
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
	r.acks[ClientID{
		NodeType: protos.NodeType_ROUTER,
		Shard:    0,
	}] = 0

	// Assembler
	r.acks[ClientID{
		NodeType: protos.NodeType_ASSEMBLER,
		Shard:    0,
	}] = 0

	// Batchers
	for _, shardID := range shards {
		r.acks[ClientID{
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
	clientID := ClientID{
		NodeType: request.NodeType,
		Shard:    types.ShardID(request.Shard),
	}

	r.lock.Lock()
	if uint64(request.ConfigSeq) != r.acks[clientID]+1 {
		r.lock.Unlock()
		r.logger.Warnf("config ack has been received on sequence %d but the last acknowledged sequence is %d", request.ConfigSeq, r.acks[clientID])
		return fmt.Errorf("config ack has been received on sequence %d but the last acknowledged sequence is %d", request.ConfigSeq, r.acks[clientID])
	}

	r.acks[clientID] = uint64(request.ConfigSeq)
	r.logger.Infof("config ack has been received on sequence %d", request.ConfigSeq)
	r.lock.Unlock()

	r.signalChan <- struct{}{}
	return nil
}

func (r *Receiver) WaitForAllAcks(configSeq uint64) bool {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Infof("config ack handler is stopped")
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
