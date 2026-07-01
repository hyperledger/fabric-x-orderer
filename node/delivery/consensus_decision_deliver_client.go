/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
)

const (
	replicateDecisionChanSize = 100
)

// ConsensusDecisionReplicator replicates decisions from consensus and allows the consumption of those decisions.
type ConsensusDecisionReplicator struct {
	channelID       string
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          *flogging.FabricLogger
	cancelCtx       context.Context
	ctxCancelFunc   context.CancelFunc
	seekInfo        *orderer.SeekInfo
}

func NewConsensusDecisionReplicator(channelID string, tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, logger *flogging.FabricLogger, seekInfo *orderer.SeekInfo) *ConsensusDecisionReplicator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	decisionReplicator := &ConsensusDecisionReplicator{
		channelID:     channelID,
		tlsKey:        tlsKey,
		tlsCert:       tlsCert,
		endpoint:      endpoint,
		cc:            clientConfig(tlsCACerts, tlsKey, tlsCert),
		logger:        logger,
		cancelCtx:     ctx,
		ctxCancelFunc: cancelFunc,
		seekInfo:      seekInfo,
	}
	return decisionReplicator
}

func (cr *ConsensusDecisionReplicator) ReplicateDecision() <-chan *state.Header {
	endpoint := func() string {
		return cr.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			DecisionChannelName(cr.channelID),
			nil,
			cr.seekInfo,
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			cr.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	res := make(chan *state.Header, replicateDecisionChanSize)

	blockHandlerFunc := func(block *common.Block) {
		header := extractHeaderFromBlock(block, cr.logger)
		res <- header
	}

	onClose := func() {
		close(res)
	}

	go Pull(cr.cancelCtx, "consensus-decision-replicate", cr.logger, endpoint, requestEnvelopeFactoryFunc, cr.cc, blockHandlerFunc, onClose)

	return res
}

func (cr *ConsensusDecisionReplicator) Stop() {
	cr.ctxCancelFunc()
}
