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

type PrimaryReqConnector struct {
	logger               *flogging.FabricLogger
	config               *node_config.BatcherNodeConfig
	batchers             map[types.PartyID]*EndpointAndCerts
	primaryID            types.PartyID
	conn                 *grpc.ClientConn
	stream               protos.BatcherControlService_FwdRequestStreamClient
	connectorCtx         context.Context // used to stop connector
	connectorCancelFunc  context.CancelFunc
	connectionCtx        context.Context // used to stop the current connection
	connectionCancelFunc context.CancelFunc
	lock                 sync.RWMutex
	connectSlot          chan struct{} // lets one goroutine at a time replace the connection to the primary
	sendSlot             chan struct{} // lets one goroutine at a time send on the stream; gRPC does not allow concurrent sends

	minRetryInterval time.Duration
	maxRetryDelay    time.Duration
	sendTimeout      time.Duration
}

func (p *PrimaryReqConnector) SendReq(req []byte) {
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
			if !p.connectIfNeeded(sendTimer) {
				return
			}
			stream, primaryID = p.currentStream()
			if stream == nil { // connecting to primary failed
				p.logger.Errorf("Failed sending req to primary %d; failed to connect", primaryID)
				continue
			}
		}
		// gRPC does not allow concurrent sends on a stream, so the senders take turns. The turn is not
		// the connector lock: a Send blocked on a full stream would otherwise keep ResetConnection from
		// taking the lock, so the connection could never be closed to unblock it.
		if !p.acquireSend(sendTimer) {
			return
		}
		err := stream.Send(&protos.FwdRequest{Request: req})
		p.releaseSend()
		if err != nil {
			p.logger.Errorf("Failed sending req to primary %d; error: %v", primaryID, err)
			continue
		}
		return
	}
}

// currentStream returns the stream to the current primary, nil if there is none, and the primary.
func (p *PrimaryReqConnector) currentStream() (protos.BatcherControlService_FwdRequestStreamClient, types.PartyID) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.stream, p.primaryID
}

// connectIfNeeded connects to the primary unless there is a stream already. Only one connection attempt
// runs at a time, while callers can stop waiting when their send timeout expires.
func (p *PrimaryReqConnector) connectIfNeeded(sendTimer *time.Timer) bool {
	select {
	case p.connectSlot <- struct{}{}:
	case <-p.connectorCtx.Done():
		return false
	case <-sendTimer.C:
		return false
	}

	connected := make(chan struct{})
	go func() {
		defer func() {
			<-p.connectSlot
			close(connected)
		}()
		if stream, _ := p.currentStream(); stream == nil {
			p.connect()
		}
	}()

	select {
	case <-connected:
		return true
	case <-p.connectorCtx.Done():
		return false
	case <-sendTimer.C:
		return false
	}
}

// acquireSend waits for the turn to send on the stream. It returns false if the send timed out or the
// connector was stopped while waiting.
func (p *PrimaryReqConnector) acquireSend(sendTimer *time.Timer) bool {
	select {
	case p.sendSlot <- struct{}{}:
		return true
	case <-p.connectorCtx.Done():
		return false
	case <-sendTimer.C:
		return false
	}
}

func (p *PrimaryReqConnector) releaseSend() {
	<-p.sendSlot
}

func (p *PrimaryReqConnector) ConnectToNewPrimary(primaryID types.PartyID) {
	p.lock.Lock()
	p.primaryID = primaryID
	p.lock.Unlock()
	// Reset before waiting for the connect lock: it cancels a connect to the previous primary that is
	// in progress, so the switch does not wait for it to give up.
	p.ResetConnection()
	// Senders connect lazily so the decision loop is not blocked by an unavailable primary.
}

// ConnectToPrimary replaces the connection to the primary with a new one.
func (p *PrimaryReqConnector) ConnectToPrimary() {
	select {
	case p.connectSlot <- struct{}{}:
	case <-p.connectorCtx.Done():
		return
	}
	defer func() { <-p.connectSlot }()
	p.connect()
}

// connect replaces the connection to the primary with a new one. The caller holds connectSlot.
func (p *PrimaryReqConnector) connect() {
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
		p.logger.Errorf("Failed connecting to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
		return
	}

	p.lock.Lock()
	if ctx.Err() != nil { // the connection was reset, or the connector stopped, while dialing
		p.lock.Unlock()
		conn.Close()
		return
	}
	p.conn = conn
	p.lock.Unlock()

	primaryClient := protos.NewBatcherControlServiceClient(conn)

	reqStream, err := primaryClient.FwdRequestStream(ctx)
	if err != nil {
		p.logger.Errorf("Failed creating req stream to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
		return
	}

	p.lock.Lock()
	if ctx.Err() != nil { // reset while creating the stream; do not keep a stream of a closed connection
		p.lock.Unlock()
		return
	}
	p.stream = reqStream
	p.lock.Unlock()

	p.logger.Debugf("Created streams to primary (party ID = %d)", primary)
}

func (p *PrimaryReqConnector) ResetConnection() {
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

func (p *PrimaryReqConnector) Stop() {
	p.connectorCancelFunc()
	p.ResetConnection()
}

func CreatePrimaryReqConnector(primaryID types.PartyID, logger *flogging.FabricLogger, config *node_config.BatcherNodeConfig, batchers map[types.PartyID]*EndpointAndCerts, ctx context.Context, timeout, minRetryInterval, maxRetryDelay time.Duration) *PrimaryReqConnector {
	connector := &PrimaryReqConnector{
		primaryID:        primaryID,
		logger:           logger,
		config:           config,
		batchers:         batchers,
		sendSlot:         make(chan struct{}, 1),
		connectSlot:      make(chan struct{}, 1),
		sendTimeout:      timeout,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
	}
	connector.connectorCtx, connector.connectorCancelFunc = context.WithCancel(ctx)
	return connector
}
