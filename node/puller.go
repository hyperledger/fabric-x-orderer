package node

import (
	arma "arma/pkg"
	"fmt"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"time"
)

type BatchPuller struct {
	getHeight func() uint64
	ledger    arma.BatchLedger
	logger    arma.Logger
	config    BatcherNodeConfig
	tlsKey    []byte
	tlsCert   []byte
}

func (bp *BatchPuller) PullBatches(from arma.PartyID) <-chan arma.Batch {
	res := make(chan arma.Batch)

	seq := bp.getHeight()

	primary := bp.findPrimary(arma.ShardID(bp.config.ShardId), from)

	endpoint := primary.Endpoint

	shardName := fmt.Sprintf("shard%d", bp.config.ShardId)
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		shardName,
		nil,
		nextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		bp.logger.Panicf("Failed creating seek envelope: %v", err)
	}

	go pull(shardName, bp.logger, endpoint, requestEnvelope, bp.clientConfig(from), func(block *common.Block) {
		bp.logger.Infof("Fetched block %d with %d transactions", block.Header.Number, len(block.Data.Data))
		fb := fabricBatch(*block)
		res <- &fb
	})

	return res
}

func (bp *BatchPuller) clientConfig(primary arma.PartyID) comm.ClientConfig {
	shardInfo := bp.findPrimary(arma.ShardID(bp.config.ShardId), primary)

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

func (bp *BatchPuller) findPrimary(shardID arma.ShardID, primary arma.PartyID) BatcherInfo {
	for _, shard := range bp.config.Shards {
		if shard.ShardId == uint16(shardID) {
			for _, b := range shard.Batchers {
				if b.PartyID == uint16(primary) {
					return b
				}
			}

			bp.logger.Panicf("Failed finding primary for shard %d %d within %v", shardID, bp.config.PartyId, shard.Batchers)
		}
	}

	bp.logger.Panicf("Failed finding shard ID %d within %v", shardID, bp.config.Shards)
	return BatcherInfo{}
}
