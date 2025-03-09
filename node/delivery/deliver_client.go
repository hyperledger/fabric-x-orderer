package delivery

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"google.golang.org/grpc"
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

func Pull(context context.Context, channel string, logger types.Logger, endpoint func() string, requestEnvelopeFactory func() *common.Envelope, cc comm.ClientConfig, handleBlock func(block *common.Block), onClose func()) {
	logger.Infof("Started pulling from: %s", channel)
	for {
		time.Sleep(time.Second)

		select {
		case <-context.Done():
			logger.Infof("Returning since context is done")
			if onClose != nil {
				onClose()
			}
			return
		default:
		}

		endpointToPullFrom := endpoint()

		if endpointToPullFrom == "" {
			logger.Errorf("No one to pull from, waiting...")
			continue
		}

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

		pullBlocks(channel, logger, stream, endpointToPullFrom, conn, handleBlock)
	}
}

func pullBlocks(channel string, logger types.Logger, stream orderer.AtomicBroadcast_DeliverClient, endpoint string, conn *grpc.ClientConn, handleBlock func(block *common.Block)) {
	logger.Infof("Assembler pulling blocks from: %s", channel)
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
	}
}

// PullOne will pull a single block, as specified in the request.
func PullOne(context context.Context, channel string, logger types.Logger, endpointToPullFrom string, requestEnvelope *common.Envelope, cc comm.ClientConfig) (*common.Block, error) {
	logger.Infof("Assembler pulling one from: %s", channel)
	for {
		select {
		case <-context.Done():
			return nil, fmt.Errorf("context is done")
		default:
		}

		conn, err := cc.Dial(endpointToPullFrom)
		if err != nil {
			logger.Errorf("Failed connecting to %s: %v", endpointToPullFrom, err)
			continue
		}

		atomicBroadcastClient := orderer.NewAtomicBroadcastClient(conn)

		stream, err := atomicBroadcastClient.Deliver(context)
		if err != nil {
			logger.Errorf("Failed creating Deliver stream to %s: %v", endpointToPullFrom, err)
			conn.Close()
			continue
		}

		err = stream.Send(requestEnvelope)
		if err != nil {
			logger.Errorf("Failed sending request envelope to %s: %v", endpointToPullFrom, err)
			stream.CloseSend()
			conn.Close()
			continue
		}

		block, err := readBlock(stream, conn, endpointToPullFrom, channel, logger)
		if err != nil {
			logger.Errorf("Failed to read block from %s: %v", endpointToPullFrom, err)
			continue
		}

		return block, nil
	}
}

// readBlock reads a single block and closes the stream and the connection.
func readBlock(stream orderer.AtomicBroadcast_DeliverClient, conn *grpc.ClientConn, endpoint string, channel string, logger types.Logger) (*common.Block, error) {
	defer func() {
		stream.CloseSend()
		conn.Close()
	}()

	logger.Infof("Assembler reading block from endpoint: %s, channel: %s", endpoint, channel)

	resp, err := stream.Recv()
	if err != nil {
		logger.Errorf("Failed receiving block from endpoint: %s, channel: %s, err: %s", endpoint, channel, err.Error())
		return nil, err
	}

	if resp.GetBlock() == nil {
		logger.Errorf("Received a non block message from endpoint: %s, channel: %s,resp: %+v", endpoint, channel, resp)
		return nil, fmt.Errorf("non block message from endpoint: %s, channel: %s,resp: %+v", endpoint, channel, resp)
	}

	return resp.GetBlock(), nil
}
