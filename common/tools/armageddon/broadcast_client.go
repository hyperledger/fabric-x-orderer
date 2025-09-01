/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"google.golang.org/grpc"
)

type StreamInfo struct {
	conn                  *grpc.ClientConn
	stream                ab.AtomicBroadcast_BroadcastClient
	stopChan              chan struct{}
	isBroken              bool
	isAlreadyReconnecting bool
	maxRetryDelay         time.Duration
	endpoint              string
	lock                  sync.Mutex
	logger                *flogging.FabricLogger
}

func (streamInfo *StreamInfo) IsBroken() bool {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	return streamInfo.isBroken
}

func (streamInfo *StreamInfo) SetIsBroken(value bool) {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	streamInfo.isBroken = value
}

func (streamInfo *StreamInfo) CheckIfReconnectionIsNeeded() bool {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	if streamInfo.isAlreadyReconnecting {
		return false // reconnection is not needed again
	} else {
		streamInfo.isAlreadyReconnecting = true // mark that reconnection is needed
		return true
	}
}

func (streamInfo *StreamInfo) SetNewConnAndStream(newConnection *grpc.ClientConn, newStream ab.AtomicBroadcast_BroadcastClient) {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	// close old connection
	err := streamInfo.conn.Close()
	if err != nil {
		streamInfo.logger.Infof("set new connection and stream failed, err: %v", err)
	}
	// set new connection and stream
	streamInfo.logger.Infof("Set new connection and stream to router: %s", streamInfo.endpoint)
	streamInfo.stream = newStream
	streamInfo.conn = newConnection
	streamInfo.isBroken = false
	streamInfo.isAlreadyReconnecting = false
}

func (streamInfo *StreamInfo) TryReconnect(userConfig *UserConfig) {
	if !streamInfo.CheckIfReconnectionIsNeeded() {
		return
	}

	go func() {
		delay := 2 * time.Second
		for {
			ticker := time.NewTicker(delay)
			select {
			case <-streamInfo.stopChan:
				return
			case <-ticker.C:
				newConn, newStream, err := createConnAndStream(userConfig, streamInfo.endpoint)
				if err != nil {
					delay *= 2
					if delay > streamInfo.maxRetryDelay {
						delay = streamInfo.maxRetryDelay
					}
					streamInfo.logger.Infof("Reconnection to router: %s failed, going to try again in %v", streamInfo.endpoint, delay)
				} else {
					streamInfo.logger.Infof("Reconnection to router: %s succeeded", streamInfo.endpoint)
					streamInfo.SetNewConnAndStream(newConn, newStream)
					go ReceiveResponseFromRouter(userConfig, streamInfo)
					return
				}
			}
		}
	}()
}

type BroadcastTxClient struct {
	// userConfig is the client configuration that includes the TLS configuration and the endpoints of router and assembler nodes
	userConfig *UserConfig
	// streamsToRouters holds streams between client and routers.
	streamsToRouters []*StreamInfo
}

// NewBroadcastTxClient initializes a Broadcast TXs Client that sends transactions to the all routers.
// The client configuration comes from the user config.
// When a router becomes faulty, a reconnection process is running in the background, and txs are still sent to the available routers.
// When the faulty router recovers, the client continues to send to the router transactions.
func NewBroadcastTxClient(userConfigFile *UserConfig) *BroadcastTxClient {
	return &BroadcastTxClient{
		userConfig:       userConfigFile,
		streamsToRouters: make([]*StreamInfo, len(userConfigFile.RouterEndpoints)),
	}
}

func (c *BroadcastTxClient) InitStreams() error {
	for i, routerEndpoint := range c.userConfig.RouterEndpoints {
		conn, stream, err := createConnAndStream(c.userConfig, routerEndpoint)
		if err != nil {
			return err
		}

		c.streamsToRouters[i] = &StreamInfo{
			conn:          conn,
			stream:        stream,
			stopChan:      make(chan struct{}),
			maxRetryDelay: 8 * time.Second,
			endpoint:      routerEndpoint,
			lock:          sync.Mutex{},
			logger:        flogging.MustGetLogger(fmt.Sprintf("BroadcastClient%d", i)),
		}
	}
	return nil
}

func ReceiveResponseFromRouter(userConfig *UserConfig, streamInfo *StreamInfo) {
	for {
		select {
		case <-streamInfo.stopChan:
			return
		default:
			_, err := streamInfo.stream.Recv()
			if err != nil {
				// An error can occur if the server is faulty (Unavailable desc) or the connections is closed (Canceled desc)
				// If the connection is closed, then the reconnection go routine is opened, but then it exits with the stopChan.
				streamInfo.logger.Infof("Failed to receive response from router, close receive go routine, mark router %s as broken and start reconnection, err: %v", streamInfo.endpoint, err)
				streamInfo.SetIsBroken(true)
				streamInfo.TryReconnect(userConfig)
				return
			}
		}
	}
}

func (c *BroadcastTxClient) SendTxToAllRouters(envelope *common.Envelope) {
	for _, streamInfo := range c.streamsToRouters {
		if !streamInfo.IsBroken() {
			err := streamInfo.stream.Send(envelope)
			if err != nil {
				streamInfo.logger.Infof("Failed to send envelope to the router, mark router %s as broken and start reconnection", streamInfo.endpoint)
				streamInfo.SetIsBroken(true)
				streamInfo.TryReconnect(c.userConfig)
			}
		}
	}
}

func (c *BroadcastTxClient) Stop() error {
	// close the reconnection go routine
	// close all connections
	for _, streamInfo := range c.streamsToRouters {
		if err := streamInfo.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection to router %s", streamInfo.endpoint)
		}
		close(streamInfo.stopChan)
	}
	return nil
}

func createConnAndStream(userConfig *UserConfig, endpoint string) (*grpc.ClientConn, ab.AtomicBroadcast_BroadcastClient, error) {
	gRPCRouterClient := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               userConfig.TLSPrivateKey,
			Certificate:       userConfig.TLSCertificate,
			RequireClientCert: userConfig.UseTLSRouter == "mTLS",
			UseTLS:            userConfig.UseTLSRouter != "none",
			ServerRootCAs:     userConfig.TLSCACerts,
		},
		DialTimeout: time.Second * 5,
	}

	gRPCRouterClientConn, err := gRPCRouterClient.Dial(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to close gRPC connection to router %s, err: %v", endpoint, err)
	}

	stream, err := ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open a broadcast stream to router %s, err: %v", endpoint, err)
	}

	return gRPCRouterClientConn, stream, nil
}
