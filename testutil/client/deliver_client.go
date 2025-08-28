/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type DeliverClient struct {
	userConfig *armageddon.UserConfig
}

type BlockHandler func(block *common.Block) error

func NewDeliverClient(userConfig *armageddon.UserConfig) *DeliverClient {
	return &DeliverClient{
		userConfig: userConfig,
	}
}

type response struct {
	status common.Status
	err    error
}

// PullBlocks is blocking untill all blocks are delivered: startBlock to endBlock, inclusive; or an error is returned.
func (c *DeliverClient) PullBlocks(ctx context.Context, assemblerId types.PartyID, startBlock uint64, endBlock uint64, handler BlockHandler) (common.Status, error) {
	client, gRPCAssemblerClientConn, err := c.createClientAndSendRequest(startBlock, endBlock, assemblerId)
	if err != nil {
		return common.Status(0), errors.Wrapf(err, "failed to create client to assembler: %d", assemblerId)
	}

	stopChan := make(chan response)

	cleanUp := func() {
		_ = client.CloseSend()
		_ = gRPCAssemblerClientConn.Close()
	}

	// pull blocks from the assembler, this goroutine only receives from the stream
	go func() {
		for {
			block, status, err := pullBlock(client)
			if err != nil || status != common.Status(0) {
				fmt.Printf("Pulling blocks from assembler: %d ended with status: %v, err: %v \n", assemblerId, status, err)
				stopChan <- response{err: err, status: status}
				close(stopChan)
				return
			}

			err = handler(block)
			if err != nil {
				fmt.Printf("Failed to handle block: %s \n", err.Error())
				stopChan <- response{err: err, status: common.Status(0)}
				close(stopChan)
				return
			}

			if block.GetHeader().GetNumber() == endBlock {
				block, status, err = pullBlock(client)
				if status == common.Status_SUCCESS {
					fmt.Printf("Pulling blocks from assembler: %d completed successfully \n", assemblerId)
				} else if block != nil {
					err = errors.Errorf("expected Status_SUCCESS when reaching endBlock: %d, but got block", endBlock)
				} else {
					fmt.Printf("Failed to pull status: %v error: %v \n", status, err)
				}

				stopChan <- response{err: err, status: status}
				close(stopChan)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		cleanUp() // we call cleanUp here so that the goroutine would exit.
		errCanceled := <-stopChan
		return errCanceled.status, errors.Errorf("cancelled pull from assembler: %d; pull ended: %s", assemblerId, errCanceled.err)
	case resp := <-stopChan:
		defer cleanUp()
		if resp.err != nil {
			resp.err = errors.Wrapf(resp.err, "pull from assembler: %d ended", assemblerId)
		}
		return resp.status, resp.err
	}
}

func (c *DeliverClient) createClientAndSendRequest(startBlock uint64, endBlock uint64, assemblerId types.PartyID) (ab.AtomicBroadcast_DeliverClient, *grpc.ClientConn, error) {
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
		nextSeekInfo(startBlock, endBlock),
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

func pullBlock(client ab.AtomicBroadcast_DeliverClient) (*common.Block, common.Status, error) {
	resp, err := client.Recv()
	if err == io.EOF {
		return nil, common.Status(0), err
	}
	if err != nil {
		return nil, common.Status(0), errors.Wrap(err, "failed to receive a deliver response")
	}

	block := resp.GetBlock()

	if block == nil {
		status := resp.GetStatus()
		if status != common.Status_SUCCESS {
			err = fmt.Errorf("received a non block message: %v", resp)
		}
		return nil, status, err
	}

	return block, common.Status(0), nil
}

func nextSeekInfo(start uint64, stop uint64) *ab.SeekInfo {
	return &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: start}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: stop}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT,
	}
}
