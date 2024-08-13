package delivery

import (
	"context"
	"encoding/binary"
	"math"
	"time"

	"arma/core"
	"arma/node/comm"
	"arma/node/config"
	cstate "arma/node/consensus/state"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
)

func NextSeekInfo(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func ClientConfig(TLSCACerts []config.RawBytes, tlsKey, tlsCert []byte) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range TLSCACerts {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               tlsKey,
			Certificate:       tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func Pull(context context.Context, channel string, logger core.Logger, endpoint func() string, requestEnvelope *common.Envelope, cc comm.ClientConfig, parseBlock func(block *common.Block)) {
	logger.Infof("Assembler pulling from: %s", channel)
	for {
		time.Sleep(time.Second)

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

		err = stream.Send(requestEnvelope)
		if err != nil {
			logger.Errorf("Failed sending request envelope to %s: %v", endpointToPullFrom, err)
			stream.CloseSend()
			conn.Close()
			continue
		}

		pullBlocks(channel, logger, stream, endpointToPullFrom, conn, parseBlock)
	}
}

func pullBlocks(channel string, logger core.Logger, stream orderer.AtomicBroadcast_DeliverClient, endpoint string, conn *grpc.ClientConn, parseBlock func(block *common.Block)) {
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

		parseBlock(block)
	}
}

type BAReplicator struct {
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          core.Logger
}

func NewBAReplicator(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, logger core.Logger) *BAReplicator {
	baReplicator := &BAReplicator{
		cc:       ClientConfig(tlsCACerts, tlsKey, tlsCert),
		endpoint: endpoint,
		logger:   logger,
		tlsKey:   tlsKey,
		tlsCert:  tlsCert,
	}
	return baReplicator
}

func (bar *BAReplicator) Replicate(seq uint64) <-chan core.BatchAttestation {
	endpoint := func() string {
		return bar.endpoint
	}

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"consensus",
		nil,
		NextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		bar.logger.Panicf("Failed creating signed envelope: %v", err)
	}

	res := make(chan core.BatchAttestation, 100)

	go Pull(context.Background(), "consensus", bar.logger, endpoint, requestEnvelope, bar.cc, func(block *common.Block) {
		header := extractHeaderFromBlock(block, bar.logger)

		for _, ab := range header.AvailableBatches {
			bar.logger.Infof("Replicated batch attestation with seq %d and shard %d", ab.Seq(), ab.Shard())
			ab2 := ab
			res <- &ab2
		}
	})

	return res
}

func (bar *BAReplicator) ReplicateState(seq uint64) <-chan *core.State {
	endpoint := func() string {
		return bar.endpoint
	}

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"consensus",
		nil,
		NextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		bar.logger.Panicf("Failed creating signed envelope: %v", err)
	}

	res := make(chan *core.State, 100)

	go Pull(context.Background(), "consensus", bar.logger, endpoint, requestEnvelope, bar.cc, func(block *common.Block) {
		header := extractHeaderFromBlock(block, bar.logger)
		res <- header.State
	})

	return res
}

func extractHeaderFromBlock(block *common.Block, logger core.Logger) *cstate.Header {
	decisionAsBytes := block.Data.Data[0]

	headerSize := decisionAsBytes[:4]

	rawHeader := decisionAsBytes[12 : 12+binary.BigEndian.Uint32(headerSize)]

	header := &cstate.Header{}
	if err := header.FromBytes(rawHeader); err != nil {
		logger.Panicf("Failed parsing rawHeader")
	}
	return header
}
