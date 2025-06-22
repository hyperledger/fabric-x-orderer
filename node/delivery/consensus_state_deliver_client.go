/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/ledger"

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

func (cr *ConsensusStateReplicator) ReplicateState() <-chan *core.State {
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

	res := make(chan *core.State, replicateStateChanSize)

	blockHandlerFunc := func(block *common.Block) {
		header := extractHeaderFromBlock(block, cr.logger)
		res <- header.State
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
