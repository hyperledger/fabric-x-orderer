package delivery

import (
	"context"
	"encoding/binary"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/comm"
	"arma/node/config"
	cstate "arma/node/consensus/state"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
)

// ConsensusReplicator replicates decisions from consensus and allows the consumption of `core.state` or `core.BatchAttestation` objects.
type ConsensusReplicator struct {
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          types.Logger
}

func NewConsensusReplicator(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, logger types.Logger) *ConsensusReplicator {
	baReplicator := &ConsensusReplicator{
		cc:       clientConfig(tlsCACerts, tlsKey, tlsCert),
		endpoint: endpoint,
		logger:   logger,
		tlsKey:   tlsKey,
		tlsCert:  tlsCert,
	}
	return baReplicator
}

func (bar *ConsensusReplicator) ReplicateState(seq uint64) <-chan *core.State {
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

func (bar *ConsensusReplicator) Replicate(seq uint64) <-chan core.BatchAttestation {
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

func extractHeaderFromBlock(block *common.Block, logger types.Logger) *cstate.Header {
	decisionAsBytes := block.Data.Data[0]

	headerSize := decisionAsBytes[:4]

	rawHeader := decisionAsBytes[12 : 12+binary.BigEndian.Uint32(headerSize)]

	header := &cstate.Header{}
	if err := header.FromBytes(rawHeader); err != nil {
		logger.Panicf("Failed parsing rawHeader")
	}
	return header
}

func clientConfig(TLSCACerts []config.RawBytes, tlsKey, tlsCert []byte) comm.ClientConfig {
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
