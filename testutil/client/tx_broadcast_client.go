/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package client

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

type StreamInfo struct {
	stream      ab.AtomicBroadcast_BroadcastClient
	totalTxSent uint32
	endpoint    string
	streamLock  sync.Mutex
}

var logger = flogging.MustGetLogger("BroadcastClient")

type BroadcastTxClient struct {
	userConfig       *armageddon.UserConfig
	timeOut          time.Duration
	streamRoutersMap map[string]*StreamInfo
	streamsMapLock   sync.Mutex
}

func NewBroadcastTxClient(userConfigFile *armageddon.UserConfig, timeOut time.Duration) *BroadcastTxClient {
	return &BroadcastTxClient{
		userConfig:       userConfigFile,
		timeOut:          timeOut,
		streamRoutersMap: make(map[string]*StreamInfo, len(userConfigFile.RouterEndpoints)),
	}
}

func (c *BroadcastTxClient) SendTx(envelope *common.Envelope) error {
	c.streamsMapLock.Lock()
	defer c.streamsMapLock.Unlock()

	err := c.createSendStreams()
	if err != nil {
		return err
	}
	var errorsAccumulator strings.Builder
	failures := 0
	var waitForReceiveDone sync.WaitGroup

	// Iterate over all streams and send the transaction
	// Use a goroutine for each stream to send the transaction concurrently
	// We use a mutex to ensure that we do not have concurrent writes to the errorsAccumulator
	// and failures count.
	// Each goroutine will send the transaction and wait for a response.
	// If the response is an error, we update the state of the stream and increment the
	// failures count.
	// If the response is successful, we print a success message and increment the total transactions sent
	// for that stream.

	updateStateLock := sync.Mutex{}

	// send the transaction to all streams.
	for _, streamInfo := range c.streamRoutersMap {
		waitForReceiveDone.Add(1)
		go func() {
			defer waitForReceiveDone.Done()
			streamInfo.streamLock.Lock()
			defer streamInfo.streamLock.Unlock()

			err := streamInfo.stream.Send(envelope)
			if err != nil {
				updateStateLock.Lock()
				streamInfo.stream = nil
				errorsAccumulator.WriteString(fmt.Sprintf("failed to send tx to %s: %s", streamInfo.endpoint, err.Error()))
				failures++
				updateStateLock.Unlock()
				return
			}
			resp, err := streamInfo.stream.Recv()
			if err != nil {
				if err != io.EOF {
					// some other error occurred
					updateStateLock.Lock()
					streamInfo.stream = nil
					errorsAccumulator.WriteString(fmt.Sprintf("received error response from %s: %s", streamInfo.endpoint, err.Error()))
					failures++
					updateStateLock.Unlock()
				}
				return
			}
			if resp.Status != common.Status_SUCCESS {
				updateStateLock.Lock()
				errorsAccumulator.WriteString(fmt.Sprintf("received error response from %s: %s", streamInfo.endpoint, resp.Status.String()))
				failures++
				updateStateLock.Unlock()
				return
			}

			// increment the total transactions sent for this stream
			streamInfo.totalTxSent++
		}()
	}

	waitForReceiveDone.Wait()
	// check if we had any failures
	possibleNumOfFailures := len(c.userConfig.RouterEndpoints)
	if len(c.userConfig.RouterEndpoints) >= 3 {
		possibleNumOfFailures = len(c.userConfig.RouterEndpoints) / 3
	}
	if failures > possibleNumOfFailures {
		er := fmt.Sprintf("\nfailed to send tx to %d out of %d send streams", failures, len(c.streamRoutersMap))
		errorsAccumulator.WriteString(er)
	}
	if errorsAccumulator.Len() > 0 {
		return fmt.Errorf("%v", errorsAccumulator.String())
	}
	return nil
}

func createSendStream(userConfig *armageddon.UserConfig, serverRootCAs [][]byte, endpoint string) ab.AtomicBroadcast_BroadcastClient {
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
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}
	gRPCRouterClientConn, err := gRPCRouterClient.Dial(endpoint)
	if err == nil {
		stream, err := ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
		if err == nil {
			return stream
		} else {
			gRPCRouterClientConn.Close()
		}
	}
	return nil
}

func (c *BroadcastTxClient) createSendStreams() error {
	userConfig := c.userConfig
	serverRootCAs := append([][]byte{}, userConfig.TLSCACerts...)

	if c.streamRoutersMap == nil {
		return fmt.Errorf("broadcast client is stopped")
	}

	// create gRPC clients and streams to the routers
	for i := 0; i < len(userConfig.RouterEndpoints); i++ {
		routerEndpoint := userConfig.RouterEndpoints[i]
		streamInfo, ok := c.streamRoutersMap[routerEndpoint]
		if !ok {
			// create a gRPC connection to the router
			stream := createSendStream(userConfig, serverRootCAs, routerEndpoint)
			// if stream is created successfully, add it to the map
			if stream != nil {
				c.streamRoutersMap[routerEndpoint] = &StreamInfo{stream: stream, totalTxSent: 0, endpoint: routerEndpoint}
			}
		} else {
			if streamInfo.stream == nil {
				stream := createSendStream(userConfig, serverRootCAs, routerEndpoint)
				if stream != nil {
					streamInfo.stream = stream
				} else {
					// if stream is nil, it means we failed to create a gRPC connection to the router
					// so we remove it from the map
					delete(c.streamRoutersMap, routerEndpoint)
					logger.Infof("failed to create a gRPC client connection to router: %s", routerEndpoint)
				}
			}
		}
	}
	return nil
}

func (c *BroadcastTxClient) Stop() {
	c.streamsMapLock.Lock()
	defer c.streamsMapLock.Unlock()

	if c.streamRoutersMap == nil {
		return
	}

	totalTxSent := 0
	for key := range c.streamRoutersMap {
		if sInfo, ok := c.streamRoutersMap[key]; ok {
			sInfo.streamLock.Lock()
			logger.Infof("Sent to router %s: txs %d\n", sInfo.endpoint, sInfo.totalTxSent)
			totalTxSent += int(sInfo.totalTxSent)
			if sInfo.stream != nil {
				sInfo.stream.CloseSend()
			}
			sInfo.streamLock.Unlock()
		}
	}

	c.streamRoutersMap = nil

	logger.Infof("Total sent by all routers: txs %d\n", totalTxSent)
}
