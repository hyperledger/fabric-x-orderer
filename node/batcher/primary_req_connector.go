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
		p.lock.RLock()
		if p.stream == nil {
			p.lock.RUnlock()
			p.ConnectToPrimary()
			p.lock.RLock()
			if p.stream == nil { // connecting to primary failed
				p.logger.Errorf("Failed sending req to primary %d; failed to connect", p.primaryID)
				p.lock.RUnlock()
				continue
			}
		}
		if err := p.stream.Send(&protos.FwdRequest{Request: req}); err != nil {
			p.logger.Errorf("Failed sending req to primary %d; error: %v", p.primaryID, err)
			p.lock.RUnlock()
			continue
		}
		p.lock.RUnlock()
		return
	}
}

func (p *PrimaryReqConnector) ConnectToNewPrimary(primaryID types.PartyID) {
	p.lock.Lock()
	p.primaryID = primaryID
	p.lock.Unlock()
	p.ConnectToPrimary()
}

func (p *PrimaryReqConnector) ConnectToPrimary() {
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

	reqStream, err := primaryClient.FwdRequestStream(ctx)
	if err != nil {
		p.logger.Errorf("Failed creating req stream to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
		return
	}

	p.lock.Lock()
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
		sendTimeout:      timeout,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
	}
	connector.connectorCtx, connector.connectorCancelFunc = context.WithCancel(ctx)
	return connector
}
