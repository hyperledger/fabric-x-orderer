package assembler

import (
	"context"
	"encoding/hex"
	"time"

	"arma/core"
	"arma/node/comm"
	"arma/node/config"
	"arma/node/delivery"
	"arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
)

type AssemblerLedgerHeightReader interface {
	Height(shardID core.ShardID, partyID core.PartyID) uint64
}

type BatchFetcher struct {
	ledgerHeightReader AssemblerLedgerHeightReader
	tlsKey, tlsCert    []byte
	config             config.AssemblerNodeConfig
	logger             core.Logger
}

func (br *BatchFetcher) clientConfig() comm.ClientConfig {
	var tlsCAs [][]byte
	for _, shard := range br.config.Shards {
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
			Key:               br.tlsKey,
			Certificate:       br.tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func (br *BatchFetcher) Replicate(shardID core.ShardID) <-chan core.Batch {
	br.logger.Infof("Assembler %d Replicate from shard %d", br.config.PartyId, shardID)

	// Find the batcher from my party in this shard.
	// TODO we need retry mechanisms with timeouts and be able to connect to another party on that shard.
	batcherToPullFrom := br.findShardID(shardID)

	br.logger.Infof("Assembler %d Replicate from shard %d batcher info %+v", br.config.PartyId, shardID, batcherToPullFrom)

	res := make(chan core.Batch, 100)

	for _, p := range partiesFromAssemblerConfig(br.config) {
		br.pullFromParty(shardID, batcherToPullFrom, p, res)
	}

	return res
}

func (br *BatchFetcher) pullFromParty(shardID core.ShardID, batcherToPullFrom config.BatcherInfo, partyID core.PartyID, resultChan chan core.Batch) {
	seq := br.ledgerHeightReader.Height(shardID, partyID)

	endpoint := func() string {
		return batcherToPullFrom.Endpoint
	}

	channelName := ledger.ShardPartyToChannelName(shardID, partyID)
	br.logger.Infof("Assembler replicating from channel %s ", channelName)

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		channelName,
		nil,
		delivery.NextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		br.logger.Panicf("Failed creating signed envelope: %v", err)
	}

	go delivery.Pull(
		context.Background(),
		channelName,
		br.logger, endpoint,
		requestEnvelope,
		br.clientConfig(),
		func(block *common.Block) {
			fb := ledger.FabricBatch(*block)
			br.logger.Infof("Assembler Pulled <%d,%d,%d> with digest %s", shardID, fb.Party(), fb.Seq(), hex.EncodeToString(fb.Digest()[:8]))
			resultChan <- &fb
		},
	)
	br.logger.Infof("Started pulling from: %s, sqn=%d", channelName, seq)
}

func (br *BatchFetcher) findShardID(shardID core.ShardID) config.BatcherInfo {
	for _, shard := range br.config.Shards {
		if shard.ShardId == uint16(shardID) {
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
