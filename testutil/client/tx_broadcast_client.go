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

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
)

type StreamInfo struct {
	stream      ab.AtomicBroadcast_BroadcastClient
	totalTxSent uint32
	endPoint    string
	lock        sync.Mutex
	canClose    bool // indicates if the stream should be closed and recreated
	idChannel   chan uint32
}

func processResponses(s *StreamInfo, rchannnel chan<- BroadcastClientResponse) {
	// continuously receive responses from the stream
	// and send them to the response channel
	// until an error occurs or the stream is closed

	for id := range s.idChannel {
		// wait for a response
		resp, err := s.stream.Recv()
		// if the response channel is nil, just continue
		// this means the caller is not interested in responses
		if rchannnel == nil {
			continue
		}
		if err != nil {
			// some error occurred
			rchannnel <- BroadcastClientResponse{Status: common.Status_UNKNOWN, Error: err, Id: id, Endpoint: s.endPoint}
			return
		}
		if resp.Status != common.Status_SUCCESS {
			rchannnel <- BroadcastClientResponse{
				Status: common.Status_UNKNOWN,
				Error:  fmt.Errorf("received error response from %s: %s", s.endPoint, resp.Status.String()), Id: id, Endpoint: s.endPoint,
			}
			continue
		}
		rchannnel <- BroadcastClientResponse{Status: common.Status_SUCCESS, Error: nil, Id: id, Endpoint: s.endPoint}
	}
}

type BroadcastTxClient struct {
	userConfig       *armageddon.UserConfig
	timeOut          time.Duration
	streamRoutersMap map[string]*StreamInfo
	lock             sync.Mutex
}

func NewBroadcastTxClient(userConfigFile *armageddon.UserConfig, timeOut time.Duration) *BroadcastTxClient {
	return &BroadcastTxClient{
		userConfig:       userConfigFile,
		timeOut:          timeOut,
		streamRoutersMap: make(map[string]*StreamInfo, len(userConfigFile.RouterEndpoints)),
		lock:             sync.Mutex{},
	}
}

type BroadcastClientResponse struct {
	Status   common.Status
	Error    error
	Id       uint32
	Endpoint string
}

func (c *BroadcastTxClient) SendTx(txContent []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.createSendStreams(nil)
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
			streamInfo.lock.Lock()
			defer streamInfo.lock.Unlock()

			env := tx.CreateStructuredEnvelope(txContent)
			err := streamInfo.stream.Send(env)
			if err != nil {
				updateStateLock.Lock()
				streamInfo.canClose = true
				errorsAccumulator.WriteString(err.Error())
				failures++
				return
			}
			resp, err := streamInfo.stream.Recv()
			if err != nil {
				if err != io.EOF {
					// some other error occurred
					updateStateLock.Lock()
					errorsAccumulator.WriteString(err.Error())
					failures++
				}
				return
			}
			if resp.Status != common.Status_SUCCESS {
				updateStateLock.Lock()
				errorsAccumulator.WriteString(fmt.Sprintf("received error response from %s: %s", streamInfo.endPoint, resp.Status.String()))
				failures++
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

func (c *BroadcastTxClient) FastSendTx(txId uint32, txContent []byte, rchannel chan<- BroadcastClientResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.createSendStreams(rchannel)
	if err != nil {
		return err
	}
	var errorsAccumulator strings.Builder
	failures := 0

	// Iterate over all streams and send the transaction
	// Use a mutex to ensure that we do not have concurrent writes to the errorsAccumulator
	// and failures count.

	// send the transaction to all streams.
	for _, streamInfo := range c.streamRoutersMap {
		streamInfo.lock.Lock()
		if streamInfo.canClose {
			streamInfo.lock.Unlock()
			continue
		}

		env := tx.CreateStructuredEnvelope(txContent)
		err := streamInfo.stream.Send(env)
		if err != nil {
			streamInfo.canClose = true
			close(streamInfo.idChannel)
			streamInfo.lock.Unlock()
			errorsAccumulator.WriteString(err.Error())
			failures++
			continue
		}

		// increment the total transactions sent for this stream
		streamInfo.totalTxSent++
		streamInfo.lock.Unlock()
		streamInfo.idChannel <- txId
	}

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

func (c *BroadcastTxClient) createSendStreams(rchannel chan<- BroadcastClientResponse) error {
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
				streamInfo = &StreamInfo{stream: stream, totalTxSent: 0, endPoint: routerEndpoint, lock: sync.Mutex{}, idChannel: make(chan uint32, 1000)}
				go processResponses(streamInfo, rchannel) // start processing responses
				c.streamRoutersMap[routerEndpoint] = streamInfo
			}
		} else {
			if streamInfo.canClose {
				streamInfo.stream.CloseSend()
				stream := createSendStream(userConfig, serverRootCAs, routerEndpoint)
				if stream != nil {
					streamInfo = &StreamInfo{stream: stream, totalTxSent: streamInfo.totalTxSent, endPoint: routerEndpoint, lock: sync.Mutex{}, idChannel: make(chan uint32, 1000)}
					go processResponses(streamInfo, rchannel) // start processing responses
					c.streamRoutersMap[routerEndpoint] = streamInfo
				} else {
					// if stream is nil, it means we failed to create a gRPC connection to the router
					fmt.Printf("failed to create a gRPC client connection to router: %s\n", routerEndpoint)
				}
			}
		}
	}
	return nil
}

func (c *BroadcastTxClient) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.streamRoutersMap == nil {
		return
	}

	totalTxSent := 0
	// close all streams
	// and print the total transactions sent by each stream
	// and the total transactions sent by all streams
	for key := range c.streamRoutersMap {
		if sInfo, ok := c.streamRoutersMap[key]; ok {
			sInfo.lock.Lock()
			fmt.Printf("Sent to router %s: txs %d\n", sInfo.endPoint, sInfo.totalTxSent)
			totalTxSent += int(sInfo.totalTxSent)
			sInfo.lock.Unlock()
			sInfo.stream.CloseSend()
		}
	}

	c.streamRoutersMap = nil

	fmt.Printf("Total sent by all routers: txs %d\n", totalTxSent)
}
