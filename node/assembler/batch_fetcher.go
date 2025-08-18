/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/protoutil"
)

//go:generate counterfeiter -o ./mocks/batch_fetcher.go . BatchBringer
type BatchBringer interface {
	Replicate(shardID types.ShardID) <-chan types.Batch
	GetBatch(batchID types.BatchID) (types.Batch, error)
	Stop()
}

//go:generate counterfeiter -o ./mocks/batch_fetcher_factory.go . BatchBringerFactory
type BatchBringerFactory interface {
	Create(initialBatchFrontier map[types.ShardID]map[types.PartyID]types.BatchSequence, config *config.AssemblerNodeConfig, logger types.Logger) BatchBringer
}

type DefaultBatchBringerFactory struct{}

func (f *DefaultBatchBringerFactory) Create(initialBatchFrontier map[types.ShardID]map[types.PartyID]types.BatchSequence, config *config.AssemblerNodeConfig, logger types.Logger) BatchBringer {
	return NewBatchFetcher(initialBatchFrontier, config, logger)
}

type BatchFetcher struct {
	initialBatchFrontier map[types.ShardID]map[types.PartyID]types.BatchSequence
	config               *config.AssemblerNodeConfig
	clientConfig         comm.ClientConfig
	logger               types.Logger
	cancelCtx            context.Context
	cancelCtxFunc        context.CancelFunc
}

func fetcherClientConfig(config *config.AssemblerNodeConfig) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, shard := range config.Shards {
		for _, batcher := range shard.Batchers {
			for _, tlsCA := range batcher.TLSCACerts {
				tlsCAs = append(tlsCAs, tlsCA)
			}
		}
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               config.TLSPrivateKeyFile,
			Certificate:       config.TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func NewBatchFetcher(initialBatchFrontier map[types.ShardID]map[types.PartyID]types.BatchSequence, config *config.AssemblerNodeConfig, logger types.Logger) *BatchFetcher {
	logger.Infof("Creating new Batch Fetcher using batch frontier with assembler: endpoint %s partyID %d ", config.ListenAddress, config.PartyId)
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &BatchFetcher{
		initialBatchFrontier: initialBatchFrontier,
		config:               config,
		clientConfig:         fetcherClientConfig(config),
		logger:               logger,
		cancelCtx:            ctx,
		cancelCtxFunc:        cancelFunc,
	}
}

func (br *BatchFetcher) Replicate(shardID types.ShardID) <-chan types.Batch {
	br.logger.Infof("Assembler %d Replicate from shard %d", br.config.PartyId, shardID)

	// Find the batcher from my party in this shard.
	// TODO we need retry mechanisms with timeouts and be able to connect to another party on that shard.
	batcherToPullFrom := br.findShardID(shardID)

	br.logger.Infof("Assembler %d Replicate from shard %d, batcher endpoint: %s, batcher party: %d", br.config.PartyId, shardID, batcherToPullFrom.Endpoint, batcherToPullFrom.PartyID)

	res := make(chan types.Batch, br.config.ReplicationChannelSize)

	var partyPullerWg sync.WaitGroup

	for _, p := range partiesFromAssemblerConfig(br.config) {
		partyPullerWg.Add(1)
		br.pullFromParty(shardID, batcherToPullFrom, p, res, &partyPullerWg)
	}

	go func() {
		partyPullerWg.Wait()
		close(res)
	}()

	return res
}

func (br *BatchFetcher) pullFromParty(shardID types.ShardID, batcherToPullFrom config.BatcherInfo, primaryID types.PartyID, resultChan chan types.Batch, wg *sync.WaitGroup) {
	seq := br.initialBatchFrontier[shardID][primaryID]

	endpoint := func() string {
		return batcherToPullFrom.Endpoint
	}

	channelName := ledger.ShardPartyToChannelName(shardID, primaryID)
	br.logger.Infof("Assembler replicating from channel %s ", channelName)

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			channelName,
			nil,
			delivery.NextSeekInfo(uint64(seq)),
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			br.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	blockHandlerFunc := func(block *common.Block) {
		fb, errFB := ledger.NewFabricBatchFromBlock(block)
		if errFB != nil {
			br.logger.Errorf("Assembler pulled from %s a block that cannot be converted to a FabricBatch: %s", batcherToPullFrom.Endpoint, errFB)
			return
		}
		br.logger.Infof("Assembler pulled from %s batch %s", batcherToPullFrom.Endpoint, types.BatchIDToString(fb))
		resultChan <- fb
	}

	onClose := func() {
		wg.Done()
	}

	go delivery.Pull(
		br.cancelCtx,
		channelName,
		br.logger,
		endpoint,
		requestEnvelopeFactoryFunc,
		br.clientConfig,
		blockHandlerFunc,
		onClose,
	)
	br.logger.Infof("Started pulling from: %s, sqn=%d", channelName, seq)
}

func (br *BatchFetcher) findShardID(shardID types.ShardID) config.BatcherInfo {
	for _, shard := range br.config.Shards {
		if shard.ShardId == shardID {
			for _, b := range shard.Batchers {
				if b.PartyID == br.config.PartyId {
					return b
				}
			}

			br.logger.Panicf("Failed finding our party %d within %v", br.config.PartyId, shard.Batchers)
		}
	}

	br.logger.Panicf("Failed finding shard ID %d within %v", shardID, br.config.Shards)
	return config.BatcherInfo{}
}

// GetBatch polls every batcher in the shard for a batch that has a specific batchID.
// The ShardID is taken from the batchID.
// It polls all the batchers in parallel but cancels the requests as soon as the first match is found.
// The Arma protocol ensures that if the batchID is from consensus, at least one batcher in the shard has it.
func (br *BatchFetcher) GetBatch(batchID types.BatchID) (types.Batch, error) {
	var shardInfo config.ShardInfo
	var found bool
	for _, shard := range br.config.Shards {
		if shard.ShardId == batchID.Shard() {
			shardInfo = shard
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("failed finding shard %d within config: %v", batchID.Shard(), br.config.Shards)
	}

	// canceling ctx will not cancel br.cancelCtx,
	// canceling br.cancelCtx will cancel ctx.
	ctx, cancelFunc := context.WithCancel(br.cancelCtx)
	res := make(chan types.Batch, len(shardInfo.Batchers))

	for _, fromBatcher := range shardInfo.Batchers {
		go func(from config.BatcherInfo) {
			br.pullSingleBatch(ctx, from, batchID, res)
		}(fromBatcher)
	}

	count := 0

	for {
		// TODO use select with case for shutdown, will be implemented in next PR
		select {
		case <-ctx.Done():
			cancelFunc()
			br.logger.Errorf("operation canceled")
			return nil, context.Canceled
		case fb := <-res:
			count++
			if types.BatchIDEqual(fb, batchID) {
				br.logger.Infof("Found batch %s", types.BatchIDToString(fb))
				cancelFunc()
				return fb, nil
			} else if count == len(shardInfo.Batchers) {
				br.logger.Errorf("We got responses from all %d batchers in shard %d, but none match the desired BatchID: %s", count, shardInfo.ShardId, types.BatchIDToString(batchID))

				cancelFunc()
				return nil, fmt.Errorf("failed finding batchID %s within shard: %d", types.BatchIDToString(batchID), shardInfo.ShardId)
			}
		}
	}
}

func (br *BatchFetcher) pullSingleBatch(ctx context.Context, batcherToPullFrom config.BatcherInfo, batchID types.BatchID, resultChan chan types.Batch) {
	br.logger.Infof("Assembler trying to pull a single batch from %s, batch-ID: %s", batcherToPullFrom.Endpoint, types.BatchIDToString(batchID))

	channelName := ledger.ShardPartyToChannelName(batchID.Shard(), batchID.Primary())
	br.logger.Infof("Assembler replicating from channel %s ", channelName)

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			channelName,
			nil,
			delivery.SingleSpecifiedSeekInfo(uint64(batchID.Seq())),
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			br.logger.Panicf("Failed creating signed envelope: %s", err)
		}

		return requestEnvelope
	}

	block, err := delivery.PullOne(
		ctx,
		channelName,
		br.logger,
		batcherToPullFrom.Endpoint,
		requestEnvelopeFactoryFunc,
		br.clientConfig,
	)
	if err != nil {
		br.logger.Errorf("Assembler failed to pull batch %s from batcher: shard: %d, party: %d, endpoint: %s", types.BatchIDToString(batchID), batchID.Shard(), batcherToPullFrom.PartyID, batcherToPullFrom.Endpoint)
		resultChan <- nil
	}

	fb, err := ledger.NewFabricBatchFromBlock(block)
	if err != nil {
		br.logger.Errorf("Assembler pulled from %s a block that cannot be converted to a FabricBatch: %s", batcherToPullFrom.Endpoint, err)
		return
	}
	br.logger.Infof("Assembler pulled from %s batch: %s", batcherToPullFrom.Endpoint, types.BatchIDToString(fb))
	resultChan <- fb
}

func (br *BatchFetcher) Stop() {
	br.cancelCtxFunc()
}
