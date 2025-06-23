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

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"

	"google.golang.org/grpc"
)

const (
	minRetryInterval = 1 * time.Second
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
}

func NewShardRouter(l types.Logger,
	batcherEndpoint string,
	batcherRootCAs [][]byte,
	tlsCert []byte,
	tlsKey []byte,
	numOfConnectionsForBatcher int,
	numOfgRPCStreamsPerConnection int,
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
	}

	return sr
}

func (sr *ShardRouter) ForwardBestEffort(reqID, request []byte) error {
	connIndex := int(binary.BigEndian.Uint16(reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		stream = sr.maybeInitStream(connIndex, streamInConnIndex)
	}

	if stream == nil || stream.faulty() {
		return fmt.Errorf("could not establish stream to %s", sr.batcherEndpoint)
	}

	stream.requestsChannel <- &protos.Request{
		Payload: request,
	}
	return nil
}

func (sr *ShardRouter) Forward(reqID, request []byte, responses chan Response, trace []byte) {
	connIndex := int(binary.BigEndian.Uint16(reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		stream = sr.maybeInitStream(connIndex, streamInConnIndex)
	}

	if stream == nil || stream.faulty() {
		responses <- Response{
			err:   fmt.Errorf("could not establish stream to %s", sr.batcherEndpoint),
			reqID: reqID,
		}
		return
	}

	stream.registerReply(trace, responses)

	sr.logger.Debugf("enter request %x to the requests list", reqID)
	stream.requestsChannel <- &protos.Request{
		TraceId: trace,
		Payload: request,
	}
}

func (sr *ShardRouter) maybeInitStream(connIndex int, streamInConnIndex int) *stream {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	var stream *stream
	var err error

	currentStream := sr.streams[connIndex][streamInConnIndex]

	if currentStream == nil || currentStream.faulty() {
		// check the connection, and if it is faulty try to reconnect and renew the stream
		sr.logger.Infof("stream is faulty, checking connection")
		conn := sr.connPool[connIndex]
		if conn == nil || conn.GetState() == connectivity.Shutdown || conn.GetState() == connectivity.TransientFailure {
			sr.reconnect(connIndex)
			client := protos.NewRequestTransmitClient(sr.connPool[connIndex])
			ctx, cancel := context.WithCancel(context.Background())
			stream, err = currentStream.renewStream(client, sr.batcherEndpoint, sr.logger, ctx, cancel)
			if err != nil {
				sr.logger.Errorf("failed to renew stream, err: %v", err)
			}
			sr.streams[connIndex][streamInConnIndex] = stream
		} else {
			currentStream.close()
			sr.initStream(connIndex, streamInConnIndex)
			stream = sr.streams[connIndex][streamInConnIndex]
		}
	}

	return stream
}

func (sr *ShardRouter) reconnect(connIndex int) {
	sr.logger.Infof("Connection %d is broken, attempting to reconnect...", connIndex)

	interval := minRetryInterval
	numOfRetries := 1
	for {
		sr.logger.Infof("Retry attempt #%d", numOfRetries)
		numOfRetries++

		conn, err := sr.clientConfig.Dial(sr.batcherEndpoint)
		if err != nil {
			sr.logger.Errorf("Reconnection failed: %v, trying again in: %s", err, interval)
			time.Sleep(interval)
			interval = min(interval*2, maxRetryInterval)
			continue
		}
		sr.connPool[connIndex] = conn
		sr.logger.Infof("Reconnection succeeded")
		break
	}
}

func (sr *ShardRouter) MaybeInit() {
	sr.initConnPoolAndStreamsOnce()
	sr.maybeConnect()
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
	defer sr.lock.Unlock()

	for i := range sr.connPool {
		sr.initStreamsForConn(i)
	}
}

func (sr *ShardRouter) initStreamsForConn(i int) {
	for j := 0; j < len(sr.streams[i]); j++ {
		sr.initStream(i, j)
	}
}

func (sr *ShardRouter) initStream(i int, j int) {
	if sr.connPool[i] == nil {
		return
	}

	client := protos.NewRequestTransmitClient(sr.connPool[i])
	ctx, cancel := context.WithCancel(context.Background())
	newStream, err := client.SubmitStream(ctx)
	if err == nil {
		s := &stream{
			endpoint:                          sr.batcherEndpoint,
			logger:                            sr.logger,
			requestTraceIdToResponseChannel:   make(map[string]chan Response),
			requestsChannel:                   make(chan *protos.Request, 1000),
			doneChannel:                       make(chan bool, 1),
			requestTransmitSubmitStreamClient: newStream,
			cancelFunc:                        cancel,
			ctx:                               ctx,
		}
		go s.sendRequests()
		go s.readResponses()
		sr.streams[i][j] = s

	} else {
		sr.logger.Errorf("Failed establishing stream %d to %s: %v", i*sr.router2batcherStreamsPerConn+j, sr.batcherEndpoint, err)
		cancel()
	}
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
