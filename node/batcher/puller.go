/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"math"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/protoutil"
)

// TODO The deliver service and client (puller) were copied almost as is from Fabric.
// Both the server and side and client side will need to go a revision.

//go:generate counterfeiter -o mocks/batches_puller.go . BatchesPuller
type BatchesPuller interface {
	PullBatches(from types.PartyID) <-chan types.Batch
	Stop()
}

type BatchPuller struct {
	ledger     BatchLedger
	logger     *flogging.FabricLogger
	config     *config.BatcherNodeConfig
	tlsKey     []byte
	tlsCert    []byte
	stopPuller context.CancelFunc
}

func NewBatchPuller(config *config.BatcherNodeConfig, ledger BatchLedger, logger *flogging.FabricLogger) *BatchPuller {
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
	bp.stopPuller()
}

func (bp *BatchPuller) PullBatches(from types.PartyID) <-chan types.Batch {
	var stopCtx context.Context
	stopCtx, bp.stopPuller = context.WithCancel(context.Background())

	primary := bp.findPrimary(bp.config.ShardId, from)

	channelName := node_ledger.ShardPartyToChannelName(bp.config.ShardId, primary.PartyID)
	requestEnvelopeFactoryFunc := func() *common.Envelope {
		seq := bp.ledger.Height(from)
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
		return requestEnvelope
	}

	res := make(chan types.Batch, 100)

	endpoint := func() string {
		return primary.Endpoint
	}

	blockHandlerFunc := func(block *common.Block) {
		bp.logger.Infof("[%s] Fetched batch %d with %d transactions", channelName, block.Header.Number, len(block.Data.Data))
		fb := (*node_ledger.FabricBatch)(block)
		res <- fb
	}

	go delivery.Pull(
		stopCtx,
		channelName,
		bp.logger,
		endpoint,
		requestEnvelopeFactoryFunc,
		bp.clientConfig(from),
		blockHandlerFunc,
		func() {
			bp.logger.Infof("Closed PullBatches from %s, channel %s", endpoint(), channelName)
		},
	)

	return res
}

func (bp *BatchPuller) clientConfig(primary types.PartyID) comm.ClientConfig {
	shardInfo := bp.findPrimary(bp.config.ShardId, primary)

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
				bp.logger.Debugf("Primary: %d, primaryID: %d, b.PartyID: %d", primary, primary, b.PartyID)
				if b.PartyID == primary {
					return b
				}
				bp.logger.Debugf("primary: %d, shardID: %d, current partyID: %d, currentShard: %d", primary, shardID, b.PartyID, shard.ShardId)
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
