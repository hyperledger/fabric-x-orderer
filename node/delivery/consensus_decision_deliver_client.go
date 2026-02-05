/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	replicateDecisionChanSize = 100
)

// ConsensusDecisionReplicator replicates decisions from consensus and allows the consumption of `core.state`objects.
type ConsensusDecisionReplicator struct {
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          types.Logger
	cancelCtx       context.Context
	ctxCancelFunc   context.CancelFunc
	seekInfo        *orderer.SeekInfo
}

func NewConsensusDecisionReplicator(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, logger types.Logger, seekInfo *orderer.SeekInfo) *ConsensusDecisionReplicator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	baReplicator := &ConsensusDecisionReplicator{
		cc:            clientConfig(tlsCACerts, tlsKey, tlsCert),
		endpoint:      endpoint,
		logger:        logger,
		tlsKey:        tlsKey,
		tlsCert:       tlsCert,
		cancelCtx:     ctx,
		ctxCancelFunc: cancelFunc,
		seekInfo:      seekInfo,
	}
	return baReplicator
}

// TODO refactor ReplicateState to ReplicateDecision.
func (cr *ConsensusDecisionReplicator) ReplicateState() <-chan *state.Header {
	endpoint := func() string {
		return cr.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			"consensus",
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
