/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package client

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type DeliverClient struct {
	userConfig *UserConfig
}

type BlockHandler func(block *common.Block) error

func NewDeliverClient(userConfig *UserConfig) *DeliverClient {
	return &DeliverClient{
		userConfig: userConfig,
	}
}

// PullBlocks is blocking untill all blocks are delivered: startBlock to endBlock, inclusive; or an error is returned.
func (c *DeliverClient) PullBlocks(ctx context.Context, assemblerId types.PartyID, startBlock uint64, endBlock uint64, handler BlockHandler) error {
	client, gRPCAssemblerClientConn, err := c.createClientAndSendRequest(startBlock, assemblerId)
	if err != nil {
		return errors.Wrapf(err, "failed to create client to assembler: %d", assemblerId)
	}

	cleanUp := func() {
		_ = client.CloseSend()
		_ = gRPCAssemblerClientConn.Close()
	}

	stopChan := make(chan error)

	var waitToFinish sync.WaitGroup
	waitToFinish.Add(1)

	// pull blocks from the assembler, this goroutine only receives from the stream
	go func() {
		for {
			block, err := pullBlock(client)
			if err != nil {
				fmt.Printf("Failed to pull block: %s \n", err.Error())
				stopChan <- err
				close(stopChan)
				return
			}

			err = handler(block)
			if err != nil {
				fmt.Printf("Failed to handle block: %s \n", err.Error())
				stopChan <- err
				close(stopChan)
				return
			}

			if block.GetHeader().GetNumber() == endBlock {
				close(stopChan)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		cleanUp() // we call cleanUp here so that the goroutine would exit.
		errCanceled := <-stopChan
		return errors.Errorf("cancelled pull from assembler: %d; pull ended: %s", assemblerId, errCanceled)
	case err := <-stopChan:
		defer cleanUp()
		if err != nil {
			return errors.Wrapf(err, "failed to pull from assembler: %d", assemblerId)
		}
		return nil
	}
}

func (c *DeliverClient) createClientAndSendRequest(startBlock uint64, assemblerId types.PartyID) (ab.AtomicBroadcast_DeliverClient, *grpc.ClientConn, error) {
	serverRootCAs := append([][]byte{}, c.userConfig.TLSCACerts...)

	// create a gRPC connection to the assembler
	gRPCAssemblerClient := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               c.userConfig.TLSPrivateKey,
			Certificate:       c.userConfig.TLSCertificate,
			RequireClientCert: c.userConfig.UseTLSAssembler == "mTLS",
			UseTLS:            c.userConfig.UseTLSAssembler != "none",
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		nil,
		nextSeekInfo(startBlock),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed create a request envelope")
	}

	var client ab.AtomicBroadcast_DeliverClient
	var gRPCAssemblerClientConn *grpc.ClientConn
	endpointToPullFrom := c.userConfig.AssemblerEndpoints[int(assemblerId-1)]

	gRPCAssemblerClientConn, err = gRPCAssemblerClient.Dial(endpointToPullFrom)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create a gRPC client connection to assembler: %d", assemblerId)
	}

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver client
	client, err = abc.Deliver(context.TODO())
	if err != nil {
		_ = gRPCAssemblerClientConn.Close()
		return nil, nil, errors.Wrapf(err, "failed to create a deliver client to assembler: %d", assemblerId)
	}

	// send request envelope
	err = client.Send(requestEnvelope)
	if err != nil {
		_ = client.CloseSend()
		_ = gRPCAssemblerClientConn.Close()
		return nil, nil, errors.Wrapf(err, "failed to send a request envelope to assembler: %d", assemblerId)
	}

	return client, gRPCAssemblerClientConn, nil
}

func pullBlock(client ab.AtomicBroadcast_DeliverClient) (*common.Block, error) {
	resp, err := client.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "failed to receive a deliver response")
	}

	block := resp.GetBlock()

	if block == nil {
		return nil, fmt.Errorf("received a non block message: %v", resp)
	}

	return block, nil
}

func nextSeekInfo(startSeq uint64) *ab.SeekInfo {
	return &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: startSeq}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT,
	}
}
