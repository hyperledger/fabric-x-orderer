package batcher

import (
	"context"
	"math"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/comm"
	"arma/node/config"
	node_ledger "arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
)

// TODO The deliver service and client (puller) were copied almost as is from Fabric.
// Both the server and side and client side will need to go a revision.

type BatchPuller struct {
	ledger  core.BatchLedger
	logger  types.Logger
	config  config.BatcherNodeConfig
	tlsKey  []byte
	tlsCert []byte
}

func NewBatchPuller(config config.BatcherNodeConfig, ledger core.BatchLedger, logger types.Logger) *BatchPuller {
	puller := &BatchPuller{
		ledger:  ledger,
		logger:  logger,
		config:  config,
		tlsKey:  config.TLSPrivateKeyFile,
		tlsCert: config.TLSCertificateFile,
	}
	return puller
}

func (bp *BatchPuller) Stop() {
	// TODO cause the goroutine to exit
}

func (bp *BatchPuller) PullBatches(from types.PartyID) <-chan core.Batch {
	res := make(chan core.Batch, 100)

	seq := bp.ledger.Height(from)

	primary := bp.findPrimary(types.ShardID(bp.config.ShardId), from)

	endpoint := func() string {
		return primary.Endpoint
	}

	channelName := node_ledger.ShardPartyToChannelName(types.ShardID(bp.config.ShardId), types.PartyID(primary.PartyID))
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		channelName,
		nil,
		nextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		bp.logger.Panicf("Failed creating seek envelope: %v", err)
	}

	go pull(
		context.Background(),
		channelName,
		bp.logger,
		endpoint,
		requestEnvelope,
		bp.clientConfig(from),
		func(block *common.Block) {
			bp.logger.Infof("[%s] Fetched block %d with %d transactions", channelName, block.Header.Number, len(block.Data.Data))
			fb := (*node_ledger.FabricBatch)(block)
			res <- fb
		},
	)

	return res
}

func (bp *BatchPuller) clientConfig(primary types.PartyID) comm.ClientConfig {
	shardInfo := bp.findPrimary(types.ShardID(bp.config.ShardId), primary)

	var tlsCAs [][]byte
	for _, cert := range shardInfo.TLSCACerts {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               bp.tlsKey,
			Certificate:       bp.tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func (bp *BatchPuller) findPrimary(shardID types.ShardID, primary types.PartyID) config.BatcherInfo {
	for _, shard := range bp.config.Shards {
		if shard.ShardId == shardID {
			for _, b := range shard.Batchers {
				bp.logger.Infof("Primary: %d, primaryID: %d, b.PartyID: %d", primary, primary, b.PartyID)
				if types.PartyID(b.PartyID) == primary {
					return b
				}
				bp.logger.Infof("primary: %d, shardID: %d, current partyID: %d, currentShard: %d", primary, shardID, b.PartyID, shard.ShardId)
			}

			bp.logger.Panicf("Failed finding primary for shard %d %d within %v", shardID, bp.config.PartyId, shard.Batchers)
		}
	}

	bp.logger.Panicf("Failed finding shard ID %d within %v", shardID, bp.config.Shards)
	return config.BatcherInfo{}
}

func nextSeekInfo(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func pull(context context.Context, channel string, logger types.Logger, endpoint func() string, requestEnvelope *common.Envelope, cc comm.ClientConfig, parseBlock func(block *common.Block)) {
	for {
		time.Sleep(time.Second)

		endpointToPullFrom := endpoint()

		if endpointToPullFrom == "" {
			logger.Infof("No one to pull from, waiting...")
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

func pullBlocks(channel string, logger types.Logger, stream orderer.AtomicBroadcast_DeliverClient, endpoint string, conn *grpc.ClientConn, parseBlock func(block *common.Block)) {
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
