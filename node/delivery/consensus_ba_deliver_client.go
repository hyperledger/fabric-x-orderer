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
	replicateBAChanSize = 100
)

//go:generate counterfeiter -o ./mocks/consensus_bringer.go . ConsensusBringer
type ConsensusBringer interface {
	Replicate() <-chan *state.AvailableBatchOrdered
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

func (cr *ConsensusBAReplicator) Replicate() <-chan *state.AvailableBatchOrdered {
	endpoint := func() string {
		return cr.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		lastOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
		if err != nil {
			cr.logger.Panicf("Failed fetching last ordering info: %v", err)
		}
		position := createAssemblerConsensusPosition(lastOrderingInfo)
		cr.logger.Infof("Last OrderingInfo: %s; Last AssemblerConsensusPosition: %+v", lastOrderingInfo.String(), position)

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

	incomingBAsChan := make(chan *state.AvailableBatchOrdered, replicateBAChanSize)

	initOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
	if err != nil {
		cr.logger.Panicf("Failed fetching last ordering info: %v", err)
	}

	initPosition := createAssemblerConsensusPosition(initOrderingInfo)
	cr.logger.Infof("Initial OrderingInfo: %s; Initial AssemblerConsensusPosition: %+v", initOrderingInfo.String(), initPosition)

	blockHandlerFunc := func(block *common.Block) {
		header, sigs, err2 := extractHeaderAndSigsFromBlock(block)
		if err2 != nil {
			cr.logger.Panicf("Failed extracting ordered batch attestation from decision: %s", err2)
		}

		cr.logger.Infof("Decision %d, with %d AvailableCommonBlocks", block.GetHeader().GetNumber(), len(header.AvailableCommonBlocks))
		for index, acb := range header.AvailableCommonBlocks {

			primary, shard, seq, _, _, _, _, err := ledger.AssemblerBlockMetadataFromBytes(acb.Metadata.Metadata[common.BlockMetadataIndex_ORDERER])
			if err != nil {
				cr.logger.Panicf("Failed extracting info from metadata: %s", err)
			}

			acbBatch := state.NewAvailableBatch(primary, shard, seq, acb.Header.DataHash)

			cr.logger.Infof("BA index: %d; BatchID: %s; Common Block: %s; BA block signers: %+v", index, types.BatchIDToString(acbBatch), types.CommonBlockToString(acb), signersFromSigs(sigs[index]))

			abo := &state.AvailableBatchOrdered{
				AvailableBatch: acbBatch,
				OrderingInformation: &state.OrderingInformation{
					CommonBlock: header.AvailableCommonBlocks[index],
					Signatures:  sigs[index],
					DecisionNum: header.Num,
					BatchIndex:  index,
					BatchCount:  len(header.AvailableCommonBlocks),
				},
			}

			// During recovery, this condition addresses scenarios where a partially committed decision exists in the ledger.
			// For instance, if a decision comprising three batches was interrupted after committing two, only the outstanding third batch should be reprocessed.
			// This skips those batches from a decision that were already committed.
			if abo.OrderingInformation.DecisionNum == initPosition.DecisionNum && abo.OrderingInformation.BatchIndex < initPosition.BatchIndex {
				cr.logger.Infof("Recovery from partial decision commit: AvailableBatchOrdered skipped, already committed; BatchID: %s, OrderingInfo: %s; but initial AssemblerConsensusPosition: %+v",
					types.BatchIDToString(abo.AvailableBatch), abo.OrderingInformation.String(), initPosition)
				continue
			}

			incomingBAsChan <- abo
		}
	}

	onClose := func() {
		close(incomingBAsChan)
	}

	go Pull(cr.cancelCtx, "consensus-ba-replicate", cr.logger, endpoint, requestEnvelopeFactoryFunc, cr.cc, blockHandlerFunc, onClose)

	cr.logger.Infof("Starting to replicate from consenter")

	return incomingBAsChan
}

func (cr *ConsensusBAReplicator) Stop() {
	cr.ctxCancelFunc()
}
