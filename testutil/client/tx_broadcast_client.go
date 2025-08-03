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
)

type StreamInfo struct {
	stream      ab.AtomicBroadcast_BroadcastClient
	totalTxSent uint32
	endpoint    string
}
type BroadCastTxClient struct {
	userConfig       *armageddon.UserConfig
	timeOut          time.Duration
	streamRoutersMap map[string]*StreamInfo
}

func NewBroadCastTxClient(userConfigFile *armageddon.UserConfig, timeOut time.Duration) *BroadCastTxClient {
	return &BroadCastTxClient{
		userConfig:       userConfigFile,
		timeOut:          timeOut,
		streamRoutersMap: make(map[string]*StreamInfo),
	}
}

func (c *BroadCastTxClient) SendTx(txContent []byte) error {
	c.createSendStreams()
	var errorsAccumulator strings.Builder
	failures := 0
	var waitForReceiveDone sync.WaitGroup

	lock := &sync.Mutex{}

	updateState := func(streamInfo *StreamInfo, errMsg string) {
		lock.Lock()
		defer lock.Unlock()
		streamInfo.stream = nil
		errorsAccumulator.WriteString(errMsg)
		failures++
	}

	waitForReceiveDone.Add(len(c.streamRoutersMap))
	// Iterate over all streams and send the transaction
	// Use a goroutine for each stream to send the transaction concurrently
	// We use a mutex to ensure that we do not have concurrent writes to the errorsAccumulator
	// and failures count.
	// Each goroutine will send the transaction and wait for a response.
	// If the response is an error, we update the state of the stream and increment the
	// failures count.
	// If the response is successful, we print a success message and increment the total transactions sent
	// for that stream.

	// send the transaction to all streams.
	for _, streamInfo := range c.streamRoutersMap {
		go func() {
			defer waitForReceiveDone.Done()

			err := streamInfo.stream.Send(&common.Envelope{Payload: txContent})
			if err != nil {
				updateState(streamInfo, err.Error())
				return
			}
			resp, err := streamInfo.stream.Recv()
			if err != nil {
				if err != io.EOF {
					// some other error occurred
					updateState(streamInfo, err.Error())
				}
				return
			}
			if resp.Status != common.Status_SUCCESS {
				updateState(streamInfo, fmt.Sprintf("received error response from %s: %s", streamInfo.endpoint, resp.Status.String()))
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
			UseTLS:            false,
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

func (c *BroadCastTxClient) createSendStreams() error {
	userConfig := c.userConfig
	serverRootCAs := append([][]byte{}, userConfig.TLSCACerts...)

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
				}
			}
		}
	}
	return nil
}

func (c *BroadCastTxClient) Stop() {
	totalTxSent := 0
	for key := range c.streamRoutersMap {
		if sInfo, ok := c.streamRoutersMap[key]; ok {
			fmt.Printf("Sent to router %s: txs %d\n", sInfo.endpoint, sInfo.totalTxSent)
			totalTxSent += int(sInfo.totalTxSent)
			if sInfo.stream != nil {
				sInfo.stream.CloseSend()
			}
		}
	}

	fmt.Printf("Total sent by all routers: txs %d\n", totalTxSent)
}
