/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/connectivity"

	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"

	"google.golang.org/grpc"
)

const (
	minRetryInterval = 50 * time.Millisecond
	maxRetryInterval = 10 * time.Second
	retryPolicy      = `{
	 "methodConfig": [
	   {
	     "name": [
	       {
	         "service": ""
	       }
	     ],
	     "retryPolicy": {
	       "maxAttempts": 7,
	       "initialBackoff": "0.1s",
	       "maxBackoff": "1s",
	       "backoffMultiplier": 2,
	       "retryableStatusCodes": [
	         "UNAVAILABLE"
	       ]
	     }
	   }
	 ]
	}`
)

type reconnectReq struct {
	connNumber   int
	streamInConn int
}

type ShardRouter struct {
	router2batcherConnPoolSize   int
	router2batcherStreamsPerConn int
	logger                       types.Logger
	batcherEndpoint              string
	batcherRootCAs               [][]byte
	once                         sync.Once
	lock                         sync.RWMutex
	connPool                     []*grpc.ClientConn
	streams                      [][]*stream
	tlsCert                      []byte
	tlsKey                       []byte
	clientConfig                 comm.ClientConfig
	reconnectOnce                sync.Once
	closeReconnectOnce           sync.Once
	reconnectRequests            chan reconnectReq
	closeReconnect               chan bool
	verifier                     *requestfilter.RulesVerifier
}

func NewShardRouter(l types.Logger,
	batcherEndpoint string,
	batcherRootCAs [][]byte,
	tlsCert []byte,
	tlsKey []byte,
	numOfConnectionsForBatcher int,
	numOfgRPCStreamsPerConnection int,
	verifier *requestfilter.RulesVerifier,
) *ShardRouter {
	cc := comm.ClientConfig{
		AsyncConnect: false,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: 30 * time.Second,
			ClientTimeout:  30 * time.Second,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     batcherRootCAs,
			Key:               tlsKey,
			Certificate:       tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: time.Second * 20,
	}

	sr := &ShardRouter{
		tlsCert:                      tlsCert,
		tlsKey:                       tlsKey,
		logger:                       l,
		batcherEndpoint:              batcherEndpoint,
		batcherRootCAs:               batcherRootCAs,
		router2batcherStreamsPerConn: numOfgRPCStreamsPerConnection,
		router2batcherConnPoolSize:   numOfConnectionsForBatcher,
		clientConfig:                 cc,
		reconnectRequests:            make(chan reconnectReq, 2*numOfgRPCStreamsPerConnection*numOfConnectionsForBatcher),
		closeReconnect:               make(chan bool),
		verifier:                     verifier,
	}

	return sr
}

func (sr *ShardRouter) Forward(trackedReq *TrackedRequest) {
	connIndex := int(binary.BigEndian.Uint16(trackedReq.reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(trackedReq.reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		// Notify reconnection goroutine
		sr.maybeNotifyReconnectRoutine(stream, connIndex, streamInConnIndex)

		// Send a response and return
		trackedReq.responses <- Response{
			err:   fmt.Errorf("server error: connection between router and batcher %s is broken, try again later", sr.batcherEndpoint),
			reqID: trackedReq.reqID,
		}
		return
	}
	if trackedReq.trace != nil {
		stream.registerReply(trackedReq.trace, trackedReq.responses)
		trackedReq.request.TraceId = trackedReq.trace
	}

	sr.logger.Debugf("enter request %x to the requests list", trackedReq.reqID)
	stream.requestsChannel <- trackedReq
}

func (sr *ShardRouter) maybeReconnectStream(connIndex int, streamInConnIndex int) error {
	var stream *stream
	var err error
	sr.lock.RLock()
	currentStream := sr.streams[connIndex][streamInConnIndex]
	conn := sr.connPool[connIndex]
	sr.lock.RUnlock()
	sr.logger.Debugf("Checking stream %d,%d that was reported", connIndex, streamInConnIndex)

	// check the connection, and if it's bad - try to reconnect until success
	if conn == nil || conn.GetState() == connectivity.Shutdown || conn.GetState() == connectivity.TransientFailure {
		if err = sr.reconnect(connIndex); err != nil {
			return err
		}
	}

	// try to initialize or renew the stream.
	// It could fail when the stream is reported faster than the connection's status becomes bad, or if the connection breaks again.
	if currentStream == nil {
		sr.lock.Lock()
		err = sr.initStream(connIndex, streamInConnIndex)
		sr.lock.Unlock()
		if err != nil {
			return err
		}
	} else if currentStream.faulty() {
		client := protos.NewRequestTransmitClient(sr.connPool[connIndex])
		ctx, cancel := context.WithCancel(context.Background())
		stream, err = currentStream.renewStream(client, sr.batcherEndpoint, sr.logger, ctx, cancel)
		if err != nil {
			sr.logger.Errorf("failed to renew stream in connection %d stream %d, err: %v ", connIndex, streamInConnIndex, err)
			return err
		}
		sr.logger.Debugf("stream number %d in connection %d was renewed", streamInConnIndex, connIndex)
		sr.lock.Lock()
		sr.streams[connIndex][streamInConnIndex] = stream
		sr.lock.Unlock()
	} else {
		sr.logger.Debugf("Stream %d,%d is OK but was reported", connIndex, streamInConnIndex)
	}

	return nil
}

func (sr *ShardRouter) reconnect(connIndex int) error {
	sr.logger.Debugf("Connection %d is broken, attempting to reconnect...", connIndex)

	interval := minRetryInterval
	numOfRetries := 1
	for {
		select {
		case <-sr.closeReconnect:
			return fmt.Errorf("reconnection aborted because the shard-router was stoped")
		case <-time.After(interval):
			sr.logger.Warnf("Retry attempt #%d", numOfRetries)
			numOfRetries++
			conn, err := sr.clientConfig.Dial(sr.batcherEndpoint)
			if err != nil {
				interval = min(interval*2, maxRetryInterval)
				sr.logger.Errorf("Reconnection failed: %v, trying again in: %s", err, interval)
				continue
			} else {
				sr.lock.Lock()
				sr.connPool[connIndex] = conn
				sr.lock.Unlock()
				sr.logger.Debugf("Reconnection succeeded for connection %d", connIndex)
				return nil
			}
		}
	}
}

func (sr *ShardRouter) MaybeInit() {
	sr.initConnPoolAndStreamsOnce()
	sr.maybeConnect()
	sr.startReconnectionRoutineOnce()
}

func (sr *ShardRouter) maybeConnect() {
	if !sr.replenishNeeded() {
		return
	}

	sr.replenishConnPool()
	sr.initStreams()
}

func (sr *ShardRouter) replenishNeeded() bool {
	sr.lock.RLock()
	defer sr.lock.RUnlock()

	if len(sr.connPool) == 0 {
		return true
	}

	for _, conn := range sr.connPool {
		if conn == nil {
			return true
		}
	}

	return false
}

func (sr *ShardRouter) replenishConnPool() error {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	for i, conn := range sr.connPool {
		if conn == nil || conn.GetState() == connectivity.Shutdown || conn.GetState() == connectivity.TransientFailure {
			sr.logger.Debugf("Establishing connection %d to %s", i, sr.batcherEndpoint)

			dialOpts, err := sr.clientConfig.DialOptions()
			if err != nil {
				sr.logger.Errorf("Failed to get DialOptions: %v", err)
				return fmt.Errorf("failed to get DialOptions: %v", err)
			}

			dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(retryPolicy))

			conn, err := grpc.DialContext(
				context.Background(),
				sr.batcherEndpoint,
				dialOpts...,
			)
			if err != nil {
				sr.logger.Errorf("Failed connecting to %s: %v", sr.batcherEndpoint, err)
				return fmt.Errorf("failed connecting to %s: %v", sr.batcherEndpoint, err)
			}

			if sr.connPool[i] != nil {
				sr.connPool[i].Close() // Close the old connection
			}
			sr.logger.Debugf("Connection %d to %s was replenished", i, sr.batcherEndpoint)
			sr.connPool[i] = conn
		}
	}
	return nil
}

func (sr *ShardRouter) initStreams() {
	sr.lock.Lock()
	for i := 0; i < sr.router2batcherConnPoolSize; i++ {
		for j := 0; j < sr.router2batcherStreamsPerConn; j++ {
			sr.initStream(i, j)
		}
	}
	sr.lock.Unlock()
}

func (sr *ShardRouter) initStream(i int, j int) error {
	if sr.connPool[i] == nil {
		return fmt.Errorf("could not init stream because connection %d is nil", i)
	}

	client := protos.NewRequestTransmitClient(sr.connPool[i])
	ctx, cancel := context.WithCancel(context.Background())
	newStream, err := client.SubmitStream(ctx)
	if err == nil {
		s := &stream{
			endpoint:                          sr.batcherEndpoint,
			logger:                            sr.logger,
			requestTraceIdToResponseChannel:   make(map[string]chan Response),
			requestsChannel:                   make(chan *TrackedRequest, 1000),
			doneChannel:                       make(chan bool, 1),
			requestTransmitSubmitStreamClient: newStream,
			cancelFunc:                        cancel,
			ctx:                               ctx,
			connNum:                           i,
			streamNum:                         j,
			srReconnectChan:                   sr.reconnectRequests,
			notifiedReconnect:                 false,
			verifier:                          sr.verifier,
		}
		go s.sendRequests()
		go s.readResponses()
		sr.streams[i][j] = s

	} else {
		sr.logger.Errorf("Failed establishing stream %d to %s: %v", i*sr.router2batcherStreamsPerConn+j, sr.batcherEndpoint, err)
		cancel()
		return err
	}

	return nil
}

func (sr *ShardRouter) initConnPoolAndStreamsOnce() {
	sr.once.Do(func() {
		sr.connPool = make([]*grpc.ClientConn, sr.router2batcherConnPoolSize)
		sr.streams = make([][]*stream, sr.router2batcherConnPoolSize)
		for i := 0; i < sr.router2batcherConnPoolSize; i++ {
			sr.streams[i] = make([]*stream, sr.router2batcherStreamsPerConn)
		}
	})
}

func (sr *ShardRouter) startReconnectionRoutineOnce() {
	sr.reconnectOnce.Do(func() {
		go sr.reconnectRoutine()
	})
}

func (sr *ShardRouter) maybeNotifyReconnectRoutine(stream *stream, connIndex int, streamInConnIndex int) {
	if stream == nil {
		sr.reconnectRequests <- reconnectReq{connIndex, streamInConnIndex}
	} else {
		stream.notifyReconnectRoutine()
	}
}

func (sr *ShardRouter) reconnectRoutine() {
	for {
		select {
		case <-sr.closeReconnect:
			sr.logger.Infof("Reconnection goroutine in shard-router is exiting")
			return
		case req := <-sr.reconnectRequests:
			sr.logger.Debugf("Reconnect routine recieved request to reconnect to conn %d stream %d...", req.connNumber, req.streamInConn)
			reconnected := false
			for !reconnected {
				select {
				case <-sr.closeReconnect:
					sr.logger.Infof("Reconnection goroutine in shard-router is exiting")
					return
				default:
					if err := sr.maybeReconnectStream(req.connNumber, req.streamInConn); err == nil {
						reconnected = true
					}
				}
			}
		}
	}
}

func (sr *ShardRouter) Stop() {
	// close the reconnection goroutine
	sr.closeReconnectOnce.Do(func() {
		close(sr.closeReconnect)
	})

	// close all connetions in connection pool
	sr.lock.RLock()
	for _, con := range sr.connPool {
		if con != nil {
			con.Close()
		}
	}
	sr.lock.RUnlock()
}

// IsAllStreamsOKinSR checks that all the streams in the shard-router are not faulty.
// Use for testing only.
func (sr *ShardRouter) IsAllStreamsOKinSR() bool {
	sr.lock.RLock()
	defer sr.lock.RUnlock()
	for connIdx := 0; connIdx < sr.router2batcherConnPoolSize; connIdx++ {
		for streamIdx := 0; streamIdx < sr.router2batcherStreamsPerConn; streamIdx++ {
			stream := sr.streams[connIdx][streamIdx]
			if stream == nil || stream.faulty() {
				return false
			}
		}
	}
	return true
}

// IsConnectionsToBatcherDown checks that all the streams are faulty (disconnected from batcher).
// Use for testing only.
func (sr *ShardRouter) IsConnectionsToBatcherDown() bool {
	sr.lock.RLock()
	defer sr.lock.RUnlock()
	for connIdx := 0; connIdx < sr.router2batcherConnPoolSize; connIdx++ {
		for streamIdx := 0; streamIdx < sr.router2batcherStreamsPerConn; streamIdx++ {
			stream := sr.streams[connIdx][streamIdx]
			if !(stream == nil || stream.faulty()) {
				return false
			}
		}
	}
	return true
}
