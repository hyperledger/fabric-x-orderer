/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

type BroadCastTxClient struct {
	userConfig       *armageddon.UserConfig
	timeOut          time.Duration
	streamRoutersMap map[string]ab.AtomicBroadcast_BroadcastClient
}

func NewBroadCastTxClient(userConfigFile *armageddon.UserConfig, timeOut time.Duration) *BroadCastTxClient {
	return &BroadCastTxClient{
		userConfig:       userConfigFile,
		timeOut:          timeOut,
		streamRoutersMap: make(map[string]ab.AtomicBroadcast_BroadcastClient),
	}
}

func (c *BroadCastTxClient) SendTx(txContent []byte) error {
	c.createSendStreams()
	failures := 0
	var errorsAccumulator strings.Builder
	for key, stream := range c.streamRoutersMap {
		err := stream.Send(&common.Envelope{Payload: txContent})
		if err != nil {
			delete(c.streamRoutersMap, key)
			errorsAccumulator.WriteString(err.Error())
			failures++
		}
	}

	possibleNumOfFailures := len(c.userConfig.RouterEndpoints)
	if len(c.userConfig.RouterEndpoints) >= 3 {
		possibleNumOfFailures = len(c.userConfig.RouterEndpoints) / 3
	}
	if failures >= possibleNumOfFailures {
		er := fmt.Sprintf("\nfailed to send tx to %d out of %d send streams", failures, len(c.streamRoutersMap))
		errorsAccumulator.WriteString(er)
	}
	if errorsAccumulator.Len() > 0 {
		return fmt.Errorf("%v", errorsAccumulator.String())
	}
	return nil
}

func (c *BroadCastTxClient) createSendStreams() error {
	userConfig := c.userConfig
	serverRootCAs := append([][]byte{}, userConfig.TLSCACerts...)

	// create gRPC clients and streams to the routers
	for i := 0; i < len(userConfig.RouterEndpoints); i++ {
		_, ok := c.streamRoutersMap[userConfig.RouterEndpoints[i]]
		if !ok {
			// create a gRPC connection to the router
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
			gRPCRouterClientConn, err := gRPCRouterClient.Dial(userConfig.RouterEndpoints[i])
			if err == nil {
				stream, err := ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
				if err == nil {
					c.streamRoutersMap[userConfig.RouterEndpoints[i]] = stream
				} else {
					gRPCRouterClientConn.Close()
				}
			}
		}
	}
	return nil
}

func (c *BroadCastTxClient) Stop() {
	for key := range c.streamRoutersMap {
		if stream, ok := c.streamRoutersMap[key]; ok {
			stream.CloseSend()
		}
	}
}
