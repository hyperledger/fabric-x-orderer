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
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	"github.ibm.com/decentralized-trust-research/arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	replicateBAChanSize = 100
)

//go:generate counterfeiter -o ./mocks/consensus_bringer.go . ConsensusBringer
type ConsensusBringer interface {
	Replicate() <-chan core.OrderedBatchAttestation
	Stop()
}

//go:generate counterfeiter -o ./mocks/consensus_bringer_factory.go . ConsensusBringerFactory
type ConsensusBringerFactory interface {
	Create(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) ConsensusBringer
}

type DefaultConsensusBringerFactory struct{}

func (f *DefaultConsensusBringerFactory) Create(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) ConsensusBringer {
	return NewConsensusBAReplicator(tlsCACerts, tlsKey, tlsCert, endpoint, assemblerLedger, logger)
}

// ConsensusBAReplicator replicates decisions from consensus and allows the consumption of `core.BatchAttestation` objects.
type ConsensusBAReplicator struct {
	assemblerLedger ledger.AssemblerLedgerReaderWriter // TODO instead of using AssemblerLedgerReaderWriter define a more general interface to read the last block
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          types.Logger
	cancelCtx       context.Context
	ctxCancelFunc   context.CancelFunc
}

func NewConsensusBAReplicator(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) *ConsensusBAReplicator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	baReplicator := &ConsensusBAReplicator{
		assemblerLedger: assemblerLedger,
		cc:              clientConfig(tlsCACerts, tlsKey, tlsCert),
		endpoint:        endpoint,
		logger:          logger,
		tlsKey:          tlsKey,
		tlsCert:         tlsCert,
		cancelCtx:       ctx,
		ctxCancelFunc:   cancelFunc,
	}
	return baReplicator
}

func (cr *ConsensusBAReplicator) Replicate() <-chan core.OrderedBatchAttestation {
	endpoint := func() string {
		return cr.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		lastOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
		if err != nil {
			cr.logger.Panicf("Failed fetching last ordering info: %v", err)
		}
		cr.logger.Infof("Last OrderingInfo: %s", lastOrderingInfo.String())
		position := createAssemblerConsensusPosition(lastOrderingInfo)
		cr.logger.Infof("Last AssemblerConsensusPosition: %+v", position)

		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			"consensus",
			nil,
			NextSeekInfo(uint64(position.DecisionNum)),
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			cr.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	res := make(chan core.OrderedBatchAttestation, replicateBAChanSize)

	initOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
	if err != nil {
		cr.logger.Panicf("Failed fetching last ordering info: %v", err)
	}
	cr.logger.Infof("Initial OrderingInfo: %s", initOrderingInfo.String())

	initPosition := createAssemblerConsensusPosition(initOrderingInfo)
	cr.logger.Infof("Initial AssemblerConsensusPosition: %+v", initPosition)

	blockHandlerFunc := func(block *common.Block) {
		header, sigs, err2 := extractHeaderAndSigsFromBlock(block)
		if err2 != nil {
			cr.logger.Panicf("Failed extracting ordered batch attestation from decision: %s", err2)
		}

		cr.logger.Infof("Decision %d, with %d AvailableBlocks", block.GetHeader().GetNumber(), len(header.AvailableBlocks))
		for index, ab := range header.AvailableBlocks {
			cr.logger.Infof("BA index %d BatchID: %s", index, types.BatchIDToString(ab.Batch))
			cr.logger.Infof("BA block header: %s", ab.Header.String())
			cr.logger.Infof("BA block signers: %+v", signersFromSigs(sigs[index]))

			abo := &state.AvailableBatchOrdered{
				AvailableBatch: ab.Batch,
				OrderingInformation: &state.OrderingInformation{
					BlockHeader: ab.Header,
					Signatures:  sigs[index],
					DecisionNum: header.Num,
					BatchIndex:  index,
					BatchCount:  len(header.AvailableBlocks),
				},
			}
			cr.logger.Debugf("AvailableBatchOrdered: %+v", abo)
			// During recovery, this condition addresses scenarios where a partially committed decision exists in the ledger.
			// For instance, if a decision comprising three batches was interrupted after committing two, only the outstanding third batch should be reprocessed.
			// This prevents redundant batch processing and potential errors upon resumption.
			if abo.OrderingInformation.DecisionNum == initPosition.DecisionNum && abo.OrderingInformation.BatchIndex < initPosition.BatchIndex {
				cr.logger.Infof("AvailableBatchOrdered skipped, BatchID: %s, OrderingInfo: %s; but initial AssemblerConsensusPosition: %+v",
					types.BatchIDToString(abo.AvailableBatch), abo.OrderingInformation.String(), initPosition)
				return
			}

			res <- abo
		}
	}

	onClose := func() {
		close(res)
	}

	go Pull(cr.cancelCtx, "consensus-ba-replicate", cr.logger, endpoint, requestEnvelopeFactoryFunc, cr.cc, blockHandlerFunc, onClose)

	cr.logger.Infof("Starting to replicate from consenter")

	return res
}

func (cr *ConsensusBAReplicator) Stop() {
	cr.ctxCancelFunc()
}
