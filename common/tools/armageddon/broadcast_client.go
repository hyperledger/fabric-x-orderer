/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"google.golang.org/grpc"
)

type StreamInfo struct {
	conn                  *grpc.ClientConn
	stream                ab.AtomicBroadcast_BroadcastClient
	ctx                   context.Context
	cancel                context.CancelFunc
	isBroken              bool
	isAlreadyReconnecting bool
	endpoint              string
	lock                  sync.Mutex
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

func (streamInfo *StreamInfo) IsAlreadyReconnecting() bool {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	return streamInfo.isAlreadyReconnecting
}

func (streamInfo *StreamInfo) SetIsAlreadyReconnecting(value bool) {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	streamInfo.isAlreadyReconnecting = value
}

func (streamInfo *StreamInfo) SetNewConnAndStream(newConnection *grpc.ClientConn, newStream ab.AtomicBroadcast_BroadcastClient) {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	streamInfo.stream = newStream
	streamInfo.conn = newConnection
	streamInfo.ctx = ctx
	streamInfo.cancel()
	streamInfo.cancel = cancel
	streamInfo.isBroken = false
	streamInfo.isAlreadyReconnecting = false
}

func (streamInfo *StreamInfo) TryReconnect(userConfig *UserConfig) {
	if streamInfo.IsAlreadyReconnecting() {
		return
	}

	streamInfo.SetIsAlreadyReconnecting(true)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for {
			select {
			case <-streamInfo.ctx.Done():
				return
			case <-ticker.C:
				newConn, newStream, err := createConnAndStream(userConfig, streamInfo.endpoint)
				if err != nil {
					streamInfo.SetNewConnAndStream(newConn, newStream)
					go ReceiveResponseFromRouter(userConfig, streamInfo)
					return
				}
			}
		}
	}()
}

type BroadcastTxClient struct {
	// userConfig is the client configuration includes the TLS configuration and the endpoints of router and assembler nodes
	userConfig *UserConfig
	// streamRoutersMap maps from endpoint of node to the stream the client opened for their communication.
	streamsToRouters []*StreamInfo
	// logger
	logger *flogging.FabricLogger
}

// NewBroadcastTxClient initializes a Broadcast TXs Client that sends transactions to the all routers.
// The client configuration comes from the user config.
// When a router becomes faulty, a reconnection process is running in the background, and txs are still sent to the available routers.
// When the faulty router recovers, the client continues to send him transactions.
func NewBroadcastTxClient(userConfigFile *UserConfig, logger *flogging.FabricLogger) *BroadcastTxClient {
	return &BroadcastTxClient{
		userConfig:       userConfigFile,
		streamsToRouters: make([]*StreamInfo, len(userConfigFile.RouterEndpoints)),
		logger:           logger,
	}
}

func (c *BroadcastTxClient) InitStreams() error {
	for i, routerEndpoint := range c.userConfig.RouterEndpoints {
		ctx, cancel := context.WithCancel(context.Background())
		conn, stream, err := createConnAndStream(c.userConfig, routerEndpoint)
		if err != nil {
			cancel()
			return err
		}

		c.streamsToRouters[i] = &StreamInfo{
			conn:     conn,
			stream:   stream,
			ctx:      ctx,
			cancel:   cancel,
			endpoint: routerEndpoint,
			lock:     sync.Mutex{},
		}
	}
	return nil
}

func ReceiveResponseFromRouter(userConfig *UserConfig, streamInfo *StreamInfo) {
	for {
		_, err := streamInfo.stream.Recv()
		if err != nil {
			if err == io.EOF {
				return
			} else {
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
				streamInfo.SetIsBroken(true)
				streamInfo.TryReconnect(c.userConfig)
			}
		}
	}
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
		logger.Infof("failed to close gRPC connection to router %s, err: %v", endpoint, err)
		return nil, nil, fmt.Errorf("failed to close gRPC connection to router %s, err: %v", endpoint, err)
	}

	stream, err := ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
	if err != nil {
		logger.Infof("failed to open a broadcast stream to router %s, err: %v", endpoint, err)
		return nil, nil, fmt.Errorf("failed to open a broadcast stream to router %s, err: %v", endpoint, err)
	}

	return gRPCRouterClientConn, stream, nil
}

func LoadThatTolerateRoutersFaulty(userConfig *UserConfig, numOfTxs int, rate int, txSize int, txsMap *protectedMap) {
	broadcastClient := NewBroadcastTxClient(userConfig, logger)
	err := broadcastClient.InitStreams()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init streams between client and router %v", err)
		os.Exit(3)
	}

	// create a session number (16 bytes)
	sessionNumber := make([]byte, 16)
	_, err = rand.Read(sessionNumber)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a session number, %v", err)
		os.Exit(3)
	}

	// send txs to all routers, using the rate limiter bucket
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	capacity := rate / fillFrequency
	rl, err := NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	// open go routines for receive response from the router
	for _, streamInfo := range broadcastClient.streamsToRouters {
		go ReceiveResponseFromRouter(userConfig, streamInfo)
	}

	for i := 0; i < numOfTxs; i++ {
		data := PrepareTx(i, txSize, sessionNumber)
		env := tx.CreateStructuredEnvelope(data)

		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}

		broadcastClient.SendTxToAllRouters(env)
	}
	rl.Stop()

	// close the recv go routine
	// close all connections
	for i, streamInfo := range broadcastClient.streamsToRouters {
		if err := streamInfo.conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close gRPC connection to router %d: %v", i+1, err)
			os.Exit(3)
		}
		streamInfo.cancel()
	}
}
