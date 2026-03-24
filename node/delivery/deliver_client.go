/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/node/comm"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"google.golang.org/grpc"
)

const (
	minRetryInterval = 100 * time.Millisecond
	maxRetryInterval = 10 * time.Second
)

func NewestSeekInfo() *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Newest{Newest: &orderer.SeekNewest{}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func NextSeekInfo(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func SingleSpecifiedSeekInfo(seq uint64) *orderer.SeekInfo {
	seekPosition := &orderer.SeekPosition{
		Type: &orderer.SeekPosition_Specified{
			Specified: &orderer.SeekSpecified{
				Number: seq,
			},
		},
	}
	seekInfo := &orderer.SeekInfo{
		Start:         seekPosition,
		Stop:          seekPosition,
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}

	return seekInfo
}

func Pull(context context.Context, channel string, logger *flogging.FabricLogger, endpoint func() string, requestEnvelopeFactory func() *common.Envelope, cc comm.ClientConfig, handleBlock func(block *common.Block), onClose func()) {
	logger.Infof("Started pulling from: %s", channel)

	select {
	case <-context.Done():
		logger.Infof("Returning since context is done")
		if onClose != nil {
			onClose()
		}
		return
	default:
	}

	count := 0
	retryInterval := minRetryInterval

	for {
		endpointToPullFrom := endpoint()
		logger.Infof("Endpoint to pull from is %s", endpointToPullFrom)
		if endpointToPullFrom == "" {
			logger.Errorf("No one to pull from (empty endpoint), returning")
			if onClose != nil {
				onClose()
			}
			return
		}

		if count > 0 {
			if count > 1 {
				retryInterval = 2 * retryInterval
			}
			if retryInterval > maxRetryInterval {
				retryInterval = maxRetryInterval
			}

			logger.Infof("Going to try pulling again in %s, channel: %s, endpoint: %s", retryInterval, channel, endpointToPullFrom)

			select {
			case <-context.Done():
				logger.Infof("Returning since context is done")
				if onClose != nil {
					onClose()
				}
				return
			case <-time.After(retryInterval):
				logger.Debugf("Attempt %d to connect to %s", count, endpointToPullFrom)
			}
		}
		count++

		conn, err := cc.Dial(endpointToPullFrom)
		if err != nil {
			logger.Errorf("Failed connecting to %s: %v", endpointToPullFrom, err)
			continue
		}

		abc := orderer.NewAtomicBroadcastClient(conn)

		stream, err := abc.Deliver(context)
		if err != nil {
			logger.Errorf("Failed creating Deliver stream to %s: %v", endpointToPullFrom, err)
			conn.Close()
			continue
		}

		err = stream.Send(requestEnvelopeFactory())
		if err != nil {
			logger.Errorf("Failed sending request envelope to %s: %v", endpointToPullFrom, err)
			stream.CloseSend()
			conn.Close()
			continue
		}

		resetRetryOnSuccess := func() {
			count = 0
			retryInterval = minRetryInterval
		}

		pullBlocks(
			channel,
			logger,
			stream,
			endpointToPullFrom,
			conn,
			handleBlock,
			resetRetryOnSuccess,
		)
	}
}

func pullBlocks(
	channel string,
	logger *flogging.FabricLogger,
	stream orderer.AtomicBroadcast_DeliverClient,
	endpoint string,
	conn *grpc.ClientConn,
	handleBlock func(block *common.Block),
	onSuccess func(),
) {
	logger.Infof("Started pulling blocks from: %s", channel)
	for {
		resp, err := stream.Recv()
		if err != nil {
			logger.Errorf("Failed receiving block for %s from %s: %v", channel, endpoint, err)
			stream.CloseSend()
			conn.Close()
			return
		}

		if resp.GetBlock() == nil {
			logger.Errorf("Received a non block message from %s: %v", endpoint, resp)
			stream.CloseSend()
			conn.Close()
			return
		}

		block := resp.GetBlock()
		if block.Data == nil || len(block.Data.Data) == 0 {
			logger.Errorf("Received empty block from %s", endpoint)
			stream.CloseSend()
			conn.Close()
			return
		}

		handleBlock(block)
		onSuccess()
	}
}

// PullOne will pull a single block, as specified in the request.
func PullOne(ctx context.Context, channel string, logger *flogging.FabricLogger, endpointToPullFrom string, requestEnvelopeFactory func() *common.Envelope, cc comm.ClientConfig, successErr error) (*common.Block, error) {
	logger.Infof("Started pulling one batch, channel: %s, endpoint: %s", channel, endpointToPullFrom)

	count := 0
	retryInterval := minRetryInterval

	for {
		if count > 0 {
			if count > 1 {
				retryInterval = 2 * retryInterval
			}
			if retryInterval > maxRetryInterval {
				retryInterval = maxRetryInterval
			}

			logger.Infof("Going to try pulling one block again in %s, channel: %s, endpoint: %s", retryInterval, channel, endpointToPullFrom)

			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context is done, pulling one batch from: channel: %s, endpoint: %s", channel, endpointToPullFrom)
			case <-time.After(retryInterval):
				logger.Debugf("Attempt %d to connect to %s", count, endpointToPullFrom)
			}
		}
		count++

		conn, err := cc.Dial(endpointToPullFrom)
		if err != nil {
			logger.Errorf("Failed connecting to %s: %v", endpointToPullFrom, err)
			continue
		}

		atomicBroadcastClient := orderer.NewAtomicBroadcastClient(conn)

		stream, err := atomicBroadcastClient.Deliver(ctx)
		if err != nil {
			logger.Errorf("Failed creating Deliver stream to %s: %v", endpointToPullFrom, err)
			conn.Close()
			continue
		}

		requestEnvelope := requestEnvelopeFactory()
		err = stream.Send(requestEnvelope)
		if err != nil {
			logger.Errorf("Failed sending request envelope to %s: %v", endpointToPullFrom, err)
			stream.CloseSend()
			conn.Close()
			continue
		}

		block, err := readBlock(ctx, successErr, stream, conn, endpointToPullFrom, channel, logger)
		if err != nil {
			if cause := context.Cause(ctx); cause != nil { // context was canceled while trying to read block
				if cause == successErr {
					logger.Infof("Block was not read because context was canceled on success, with cause: %v, endpoint: %s, channel: %s, err: %v", cause, endpointToPullFrom, channel, err)
				} else {
					logger.Errorf("Failed to read block from %s: %v. context was canceled with cause: %v", endpointToPullFrom, err, cause)
				}
				return nil, fmt.Errorf("context is done, pulling one batch from: channel: %s, endpoint: %s", channel, endpointToPullFrom)
			} else {
				logger.Errorf("Failed to read block from %s: %v", endpointToPullFrom, err)
			}
			continue
		}
		return block, nil
	}
}

// readBlock reads a single block and closes the stream and the connection.
func readBlock(ctx context.Context, successErr error, stream orderer.AtomicBroadcast_DeliverClient, conn *grpc.ClientConn, endpoint string, channel string, logger *flogging.FabricLogger) (*common.Block, error) {
	defer func() {
		stream.CloseSend()
		conn.Close()
	}()

	logger.Infof("Assembler reading block from endpoint: %s, channel: %s", endpoint, channel)

	resp, err := stream.Recv()
	if err != nil {
		if cause := context.Cause(ctx); cause != nil { // context was canceled during recv.
			if cause == successErr {
				logger.Infof("Block was not received because context was canceled on success, with cause: %v, endpoint: %s, channel: %s, err: %v", cause, endpoint, channel, err)
			} else {
				logger.Errorf("Failed to receive block from endpoint: %s, channel: %s, err: %v. cause: %v", endpoint, channel, err, cause)
			}
		}
		return nil, err
	}

	if resp == nil {
		logger.Errorf("received nil response from endpoint: %s, channel: %s", endpoint, channel)
		return nil, fmt.Errorf("received nil response from endpoint: %s, channel: %s", endpoint, channel)
	}

	block := resp.GetBlock()
	if block == nil {
		logger.Errorf("received a non block message from endpoint: %s, channel: %s, resp: %+v", endpoint, channel, resp)
		return nil, fmt.Errorf("received a non block message from endpoint: %s, channel: %s, resp: %+v", endpoint, channel, resp)
	}

	return block, nil
}
