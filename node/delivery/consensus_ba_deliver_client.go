/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
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
	Create(channelID string, tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, signer identity.SignerSerializer, logger *flogging.FabricLogger) ConsensusBringer
}

type DefaultConsensusBringerFactory struct{}

func (f *DefaultConsensusBringerFactory) Create(channelID string, tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, signer identity.SignerSerializer, logger *flogging.FabricLogger) ConsensusBringer {
	return NewConsensusBAReplicator(channelID, tlsCACerts, tlsKey, tlsCert, endpoint, assemblerLedger, signer, logger)
}

// ConsensusBAReplicator replicates decisions from consensus and allows the consumption of `core.BatchAttestation` objects.
type ConsensusBAReplicator struct {
	channelID       string
	assemblerLedger ledger.AssemblerLedgerReaderWriter // TODO instead of using AssemblerLedgerReaderWriter define a more general interface to read the last block
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	// signer signs the deliver request, so consensus can tell which node asks to replicate.
	signer        identity.SignerSerializer
	logger        *flogging.FabricLogger
	cancelCtx     context.Context
	ctxCancelFunc context.CancelFunc
	// tlsCertHash binds a deliver request to the TLS connection it is sent over.
	tlsCertHash []byte
}

func NewConsensusBAReplicator(channelID string, tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, signer identity.SignerSerializer, logger *flogging.FabricLogger) *ConsensusBAReplicator {
	tlsCertHash, err := protoutil.HashTLSCertificate(tlsCert)
	if err != nil {
		logger.Panicf("Failed hashing the TLS certificate: %s", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	baReplicator := &ConsensusBAReplicator{
		channelID:       channelID,
		assemblerLedger: assemblerLedger,
		cc:              clientConfig(tlsCACerts, tlsKey, tlsCert),
		endpoint:        endpoint,
		signer:          signer,
		tlsCertHash:     tlsCertHash,
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

	initOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
	if err != nil {
		cr.logger.Panicf("Failed fetching last ordering info: %v", err)
	}

	// position is the next BA to hand over: after the last BA in the ledger until a BA is handed over,
	// then after the last BA handed over. A reconnect seeks from it, and skips the BAs before it in its
	// decision, so it does not hand over a BA twice, also not the BAs that wait in incomingBAsChan and are
	// not yet in the ledger. It is kept in memory only; after a restart the assembler seeks from its ledger.
	// Pull calls both the request factory and the block handler from its goroutine.
	position := createAssemblerConsensusPosition(initOrderingInfo)
	cr.logger.Infof("Initial OrderingInfo: %s; Initial AssemblerConsensusPosition: %+v", initOrderingInfo.String(), position)

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		cr.logger.Infof("Requesting decisions from AssemblerConsensusPosition: %+v", position)

		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			DecisionChannelName(cr.channelID),
			cr.signer,
			NextSeekInfo(uint64(position.DecisionNum)),
			int32(0),
			uint64(0),
			cr.tlsCertHash,
		)
		if err != nil {
			cr.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	incomingBAsChan := make(chan *state.AvailableBatchOrdered, replicateBAChanSize)

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

			// The seek starts at the decision of position, so the BAs of that decision before position were
			// already handed over: by the run before a restart, which committed part of the decision, or before a
			// reconnect. For instance, if a decision comprising three batches was interrupted after two, only the
			// third batch is handed over again.
			if abo.OrderingInformation.DecisionNum == position.DecisionNum && abo.OrderingInformation.BatchIndex < position.BatchIndex {
				cr.logger.Infof("AvailableBatchOrdered skipped, already handed over; BatchID: %s, OrderingInfo: %s; AssemblerConsensusPosition: %+v",
					types.BatchIDToString(abo.AvailableBatch), abo.OrderingInformation.String(), position)
				continue
			}

			incomingBAsChan <- abo
			position = createAssemblerConsensusPosition(abo.OrderingInformation)
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
