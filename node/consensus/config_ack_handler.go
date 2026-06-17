/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

// ConfigAckHandler handle ConfigAck messages from party members (router, batchers, assembler)
// for a specific config sequence number.
type ConfigAckHandler struct {
	expectedConfigSeq uint64
	routerAcked       bool
	assemblerAcked    bool
	batchersAcked     map[int]bool
	logger            *flogging.FabricLogger
	lock              sync.Mutex

	doneCh   chan struct{}
	doneOnce sync.Once
}

// NewConfigAckHandler creates a new tracker for a specific config sequence.
func NewConfigAckHandler(configSeq uint64, shards int, logger *flogging.FabricLogger) *ConfigAckHandler {
	tracker := &ConfigAckHandler{
		expectedConfigSeq: configSeq,
		routerAcked:       false,
		assemblerAcked:    false,
		batchersAcked:     make(map[int]bool),
		logger:            logger,
		doneCh:            make(chan struct{}),
	}

	for shardID := 1; shardID <= shards; shardID++ {
		tracker.batchersAcked[shardID] = false
	}

	return tracker
}

// RecordAck records an acknowledgment from a party member.
// Returns true if this was a new ack (not a duplicate), false otherwise.
func (t *ConfigAckHandler) RecordAck(request *protos.ConfigAck) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if request.ConfigSeq != t.expectedConfigSeq {
		t.logger.Warnf("%s send ConfigAck on sequence %d, but the next config sequence is: %d", request.NodeType, request.ConfigSeq, t.expectedConfigSeq)
		return fmt.Errorf("%s send ConfigAck on sequence %d, but the next config sequence is: %d", request.NodeType, request.ConfigSeq, t.expectedConfigSeq)
	}

	switch request.NodeType {
	case protos.NodeType_NODE_TYPE_ROUTER:
		if t.routerAcked {
			t.logger.Debugf("ConfigAck from router for sequence %d was already sent", t.expectedConfigSeq)
			return nil
		}
		t.routerAcked = true
		t.logger.Infof("Received ConfigAck from router for sequence %d", t.expectedConfigSeq)

	case protos.NodeType_NODE_TYPE_ASSEMBLER:
		if t.assemblerAcked {
			t.logger.Debugf("ConfigAck from assembler for sequence %d was already sent", t.expectedConfigSeq)
			return nil
		}
		t.assemblerAcked = true
		t.logger.Infof("Received ConfigAck from assembler for sequence %d", t.expectedConfigSeq)

	case protos.NodeType_NODE_TYPE_BATCHER:
		if t.batchersAcked[int(request.Shard)] {
			t.logger.Debugf("ConfigAck from batcher shard %d for sequence %d was already sent", request.Shard, t.expectedConfigSeq)
			return nil
		}
		t.batchersAcked[int(request.Shard)] = true
		t.logger.Infof("Received ConfigAck from batcher shard %d for sequence %d", int(request.Shard), t.expectedConfigSeq)

	default:
		return fmt.Errorf("unknown node type: %v", request.NodeType)
	}

	if t.isComplete() {
		t.doneOnce.Do(func() {
			close(t.doneCh)
		})
	}

	return nil
}

// IsComplete returns true if all expected acks have been received.
func (t *ConfigAckHandler) isComplete() bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.routerAcked {
		return false
	}

	if !t.assemblerAcked {
		return false
	}

	for _, ack := range t.batchersAcked {
		if !ack {
			return false
		}
	}

	return true
}

func (t *ConfigAckHandler) Done() <-chan struct{} {
	return t.doneCh
}
