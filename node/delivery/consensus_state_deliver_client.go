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
	"github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	replicateStateChanSize = 100
)

// ConsensusStateReplicator replicates decisions from consensus and allows the consumption of `core.state`objects.
type ConsensusStateReplicator struct {
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          types.Logger
	cancelCtx       context.Context
	ctxCancelFunc   context.CancelFunc
}

func NewConsensusStateReplicator(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) *ConsensusStateReplicator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	baReplicator := &ConsensusStateReplicator{
		cc:            clientConfig(tlsCACerts, tlsKey, tlsCert),
		endpoint:      endpoint,
		logger:        logger,
		tlsKey:        tlsKey,
		tlsCert:       tlsCert,
		cancelCtx:     ctx,
		ctxCancelFunc: cancelFunc,
	}
	return baReplicator
}

func (cr *ConsensusStateReplicator) ReplicateState() <-chan *state.Header {
	endpoint := func() string {
		return cr.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			"consensus",
			nil,
			NewestSeekInfo(),
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			cr.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	res := make(chan *state.Header, replicateStateChanSize)

	blockHandlerFunc := func(block *common.Block) {
		header := extractHeaderFromBlock(block, cr.logger)
		res <- header
	}

	onClose := func() {
		close(res)
	}

	go Pull(cr.cancelCtx, "consensus-state-replicate", cr.logger, endpoint, requestEnvelopeFactoryFunc, cr.cc, blockHandlerFunc, onClose)

	return res
}

func (cr *ConsensusStateReplicator) Stop() {
	cr.ctxCancelFunc()
}
