/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/grpc"
)

type PrimaryAckConnector struct {
	logger               *flogging.FabricLogger
	config               *node_config.BatcherNodeConfig
	batchers             map[types.PartyID]*EndpointAndCerts
	shardID              types.ShardID
	primaryID            types.PartyID
	conn                 *grpc.ClientConn
	stream               protos.BatcherControlService_NotifyAckClient
	connectorCtx         context.Context // used to stop connector
	connectorCancelFunc  context.CancelFunc
	connectionCtx        context.Context // used to stop the current connection
	connectionCancelFunc context.CancelFunc
	lock                 sync.RWMutex

	minRetryInterval time.Duration
	maxRetryDelay    time.Duration
	sendTimeout      time.Duration
}

func (p *PrimaryAckConnector) SendAck(seq types.BatchSequence) {
	sendTimer := time.NewTimer(p.sendTimeout)
	var delay time.Duration
	for {
		if delay > 0 {
			delayTimer := time.NewTimer(delay)
			select {
			case <-p.connectorCtx.Done():
				delayTimer.Stop()
				return
			case <-delayTimer.C:
				delay *= 2
				if delay > p.maxRetryDelay {
					delay = p.maxRetryDelay
				}
			case <-sendTimer.C:
				delayTimer.Stop()
				return
			}
		}
		if delay == 0 {
			delay = p.minRetryInterval
		}
		stream, primaryID := p.currentStream()
		if stream == nil {
			p.ConnectToPrimary()
			stream, primaryID = p.currentStream()
			if stream == nil { // connecting to primary failed
				p.logger.Errorf("Failed sending ack to primary %d; failed to connect", primaryID)
				continue
			}
		}
		// The lock is not held across Send: a Send blocked on a full stream would otherwise keep
		// ResetConnection from taking the lock, so the connection could never be closed to unblock it.
		if err := stream.Send(&protos.Ack{Shard: uint32(p.shardID), Seq: uint64(seq)}); err != nil {
			p.logger.Errorf("Failed sending ack to primary %d; error: %v", primaryID, err)
			continue
		}
		return
	}
}

// currentStream returns the stream to the current primary, nil if there is none, and the primary.
func (p *PrimaryAckConnector) currentStream() (protos.BatcherControlService_NotifyAckClient, types.PartyID) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.stream, p.primaryID
}

func (p *PrimaryAckConnector) ConnectToNewPrimary(primaryID types.PartyID) {
	p.lock.Lock()
	p.primaryID = primaryID
	p.lock.Unlock()
	if p.config.PartyId == primaryID {
		p.ResetConnection()
		return
	}
	p.ConnectToPrimary()
}

func (p *PrimaryAckConnector) ConnectToPrimary() {
	p.ResetConnection()
	var primary types.PartyID
	var ctx context.Context
	p.lock.Lock()
	p.connectionCtx, p.connectionCancelFunc = context.WithCancel(p.connectorCtx)
	ctx = p.connectionCtx
	primary = p.primaryID
	p.lock.Unlock()

	primaryEndpoint := p.batchers[primary].Endpoint
	primaryTLSCACerts := p.batchers[primary].TLSCACerts

	if primaryEndpoint == "" {
		p.logger.Panicf("Could not find primaryID %d of shard %d within %v", primary, p.config.ShardId, p.batchers)
		return
	}

	cc := clientConfig(p.config.TLSPrivateKeyFile, p.config.TLSCertificateFile, primaryTLSCACerts)

	conn, err := cc.Dial(primaryEndpoint) // TODO create a new DialXXX(abortContext context.Context, address string) that accepts an abort context
	if err != nil {
		p.logger.Errorf("Failed connecting to primary %d (endpoint %s); error: %v", p.primaryID, primaryEndpoint, err)
		return
	}

	p.lock.Lock()
	p.conn = conn
	p.lock.Unlock()

	primaryClient := protos.NewBatcherControlServiceClient(conn)

	ackStream, err := primaryClient.NotifyAck(ctx)
	if err != nil {
		p.logger.Errorf("Failed creating ack stream to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
		return
	}

	p.lock.Lock()
	p.stream = ackStream
	p.lock.Unlock()

	p.logger.Debugf("Created streams to primary (party ID = %d)", primary)
}

func (p *PrimaryAckConnector) ResetConnection() {
	p.lock.Lock()
	defer p.lock.Unlock()
	// TODO abort dial
	if p.connectionCancelFunc != nil {
		p.connectionCancelFunc()
	}
	if p.conn != nil {
		p.conn.Close()
	}
	p.stream = nil
}

func (p *PrimaryAckConnector) Stop() {
	p.connectorCancelFunc()
	p.ResetConnection()
}

func CreatePrimaryAckConnector(primaryID types.PartyID, shardID types.ShardID, logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig, batchers map[types.PartyID]*EndpointAndCerts, ctx context.Context, timeout, minRetryInterval, maxRetryDelay time.Duration) *PrimaryAckConnector {
	connector := &PrimaryAckConnector{
		primaryID:        primaryID,
		shardID:          shardID,
		logger:           logger,
		config:           config,
		batchers:         batchers,
		sendTimeout:      timeout,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
	}
	connector.connectorCtx, connector.connectorCancelFunc = context.WithCancel(ctx)
	return connector
}
