/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	configBlocksChanSize = 100
)

// ConsensusConfigPuller pulls decisions from consensus and consumes config blocks.
type ConsensusConfigPuller struct {
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          types.Logger
	seekInfo        *orderer.SeekInfo
	cancelCtx       context.Context
	ctxCancelFunc   context.CancelFunc
}

func NewConsensusConfigPuller(config *node_config.RouterNodeConfig, logger types.Logger, seekInfo *orderer.SeekInfo) *ConsensusConfigPuller {
	ctx, cancelFunc := context.WithCancel(context.Background())
	configPuller := &ConsensusConfigPuller{
		cc:            clientConfig(config.Consenter.TLSCACerts, config.TLSPrivateKeyFile, config.TLSCertificateFile),
		endpoint:      config.Consenter.Endpoint,
		logger:        logger,
		seekInfo:      seekInfo,
		tlsKey:        config.TLSPrivateKeyFile,
		tlsCert:       config.TLSCertificateFile,
		cancelCtx:     ctx,
		ctxCancelFunc: cancelFunc,
	}
	return configPuller
}

// PullConfigBlocks starts pulling decisions from consensus. each time a decision is received, we check if it contains
// a config block. Then, we check if last block in the decision is a config block, and if so we send it on the res channel.
// The returned channel is closed when Stop() is called.
func (ccp *ConsensusConfigPuller) PullConfigBlocks() <-chan *common.Block {
	endpoint := func() string {
		return ccp.endpoint
	}

	// TODO - use a signer
	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			"consensus",
			nil,
			ccp.seekInfo,
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			ccp.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	res := make(chan *common.Block, configBlocksChanSize)

	blockHandlerFunc := func(block *common.Block) {
		// check if the decision contains a config block. then extract the config block and send it on the res channel
		header, _, err := extractHeaderAndSigsFromBlock(block)
		if err != nil {
			ccp.logger.Errorf("Failed extracting header from decision: %s", err)
		} else if header.Num == header.DecisionNumOfLastConfigBlock {
			lastBlock := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
			if protoutil.IsConfigBlock(lastBlock) {
				ccp.logger.Infof("Pulled config block number %d from consensus", lastBlock.Header.Number)
				// send the config block on the res channel
				res <- lastBlock
			} else {
				ccp.logger.Errorf("Expected config block but got non-config block number %d", lastBlock.Header.Number)
			}
		}
	}

	onClose := func() {
		close(res)
	}

	go Pull(ccp.cancelCtx, "consensus-configBlock-pull", ccp.logger, endpoint, requestEnvelopeFactoryFunc, ccp.cc, blockHandlerFunc, onClose)

	return res
}

// Stop stops pulling config blocks and closes the config blocks channel.
func (ccp *ConsensusConfigPuller) Stop() {
	ccp.ctxCancelFunc()
}

func (ccp *ConsensusConfigPuller) Update() {
	// TODO - implement thread-safe update method.
}
