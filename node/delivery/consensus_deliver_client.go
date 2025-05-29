/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"
	"encoding/binary"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	"github.ibm.com/decentralized-trust-research/arma/node/ledger"

	"github.com/pkg/errors"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
)

const (
	replicateStateChanSize = 100
	replicateChanSize      = 100
)

//go:generate counterfeiter -o ./mocks/consensus_bringer.go . ConsensusBringer
type ConsensusBringer interface {
	ReplicateState() <-chan *core.State
	Replicate() <-chan core.OrderedBatchAttestation
	Stop()
}

//go:generate counterfeiter -o ./mocks/consensus_bringer_factory.go . ConsensusBringerFactory
type ConsensusBringerFactory interface {
	Create(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) ConsensusBringer
}

type DefaultConsensusBringerFactory struct{}

func (f *DefaultConsensusBringerFactory) Create(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) ConsensusBringer {
	return NewConsensusReplicator(tlsCACerts, tlsKey, tlsCert, endpoint, assemblerLedger, logger)
}

// ConsensusReplicator replicates decisions from consensus and allows the consumption of `core.state` or `core.BatchAttestation` objects.
type ConsensusReplicator struct {
	assemblerLedger ledger.AssemblerLedgerReaderWriter // TODO instead of using AssemblerLedgerReaderWriter define a more general interface to read the last block
	tlsKey, tlsCert []byte
	endpoint        string
	cc              comm.ClientConfig
	logger          types.Logger
	cancelCtx       context.Context
	ctxCancelFunc   context.CancelFunc
}

func NewConsensusReplicator(tlsCACerts []config.RawBytes, tlsKey config.RawBytes, tlsCert config.RawBytes, endpoint string, assemblerLedger ledger.AssemblerLedgerReaderWriter, logger types.Logger) *ConsensusReplicator {
	ctx, cancelFunc := context.WithCancel(context.Background())
	baReplicator := &ConsensusReplicator{
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

func (cr *ConsensusReplicator) ReplicateState() <-chan *core.State {
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

	go Pull(cr.cancelCtx, "consensus", cr.logger, endpoint, requestEnvelopeFactoryFunc, cr.cc, blockHandlerFunc, onClose)

	return res
}

func (cr *ConsensusReplicator) Replicate() <-chan core.OrderedBatchAttestation {
	endpoint := func() string {
		return cr.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		lastOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
		if err != nil {
			cr.logger.Panicf("Failed fetching last ordering info: %v", err)
		}
		position := createAssemblerConsensusPosition(lastOrderingInfo)
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

	res := make(chan core.OrderedBatchAttestation, replicateChanSize)

	initOrderingInfo, err := cr.assemblerLedger.LastOrderingInfo()
	if err != nil {
		cr.logger.Panicf("Failed fetching last ordering info: %v", err)
	}
	initPosition := createAssemblerConsensusPosition(initOrderingInfo)

	blockHandlerFunc := func(block *common.Block) {
		header, sigs, err2 := extractHeaderAndSigsFromBlock(block)
		if err2 != nil {
			cr.logger.Panicf("Failed extracting ordered batch attestation from decision: %s", err2)
		}

		cr.logger.Infof("Extracted a decision header with %d available blocks", len(header.AvailableBlocks))
		for index, ab := range header.AvailableBlocks {
			cr.logger.Infof("BA index %d with: shard %d, primary %d, seq %d", index, ab.Batch.Shard(), ab.Batch.Primary(), ab.Batch.Seq())
			cr.logger.Infof("BA block header: %+v", ab.Header)
			cr.logger.Infof("BA block signers: %+v", signersFromSigs(sigs[index]))
			cr.logger.Infof("BA digest: %s; block header digest: %s, ", core.ShortDigestString(ab.Batch.Digest()), core.ShortDigestString(ab.Header.Digest))

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
				cr.logger.Debugf("AvailableBatchOrdered %+v is skipped, requested batch index was %d, but current is %d", abo, initPosition.BatchIndex, abo.OrderingInformation.BatchIndex)
				return
			}

			res <- abo
		}
	}

	onClose := func() {
		close(res)
	}

	go Pull(cr.cancelCtx, "consensus", cr.logger, endpoint, requestEnvelopeFactoryFunc, cr.cc, blockHandlerFunc, onClose)

	cr.logger.Infof("Starting to replicate from consenter")

	return res
}

func (cr *ConsensusReplicator) Stop() {
	cr.ctxCancelFunc()
}

func extractHeaderFromBlock(block *common.Block, logger types.Logger) *state.Header {
	decisionAsBytes := block.Data.Data[0]

	headerSize := decisionAsBytes[:4]

	rawHeader := decisionAsBytes[12 : 12+binary.BigEndian.Uint32(headerSize)]

	header := &state.Header{}
	if err := header.Deserialize(rawHeader); err != nil {
		logger.Panicf("Failed parsing rawHeader")
	}
	return header
}

func extractHeaderAndSigsFromBlock(block *common.Block) (*state.Header, [][]smartbft_types.Signature, error) {
	if len(block.GetData().GetData()) == 0 {
		return nil, nil, errors.New("missing data in block")
	}

	// An optimization would be to unmarshal just the header and sigs, skipping the proposal payload and metadata which we don't need here.
	// An even better optimization would be to ask for content type that does not include the proposal payload and metadata.
	proposal, compoundSigs, err := state.BytesToDecision(block.GetData().GetData()[0])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract decision from block: %d", block.GetHeader().GetNumber())
	}

	stateHeader := &state.Header{}
	if err := stateHeader.Deserialize(proposal.Header); err != nil {
		return nil, nil, errors.Wrapf(err, "failed parsing consensus/state.Header from block: %d", block.GetHeader().GetNumber())
	}

	if stateHeader.Num == 0 { // this is the genesis block
		sigs := make([][]smartbft_types.Signature, 1) // no signatures
		return stateHeader, sigs, nil
	}

	sigs, err := state.UnpackBlockHeaderSigs(compoundSigs, len(stateHeader.AvailableBlocks))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract header signatures from compound signature, block %d", block.GetHeader().GetNumber())
	}

	return stateHeader, sigs, nil
}

func signersFromSigs(sigs []smartbft_types.Signature) []uint64 {
	var signers []uint64
	for _, sig := range sigs {
		signers = append(signers, sig.ID)
	}
	return signers
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

func createAssemblerConsensusPosition(oi *state.OrderingInformation) core.AssemblerConsensusPosition {
	if oi.BatchIndex != oi.BatchCount-1 {
		return core.AssemblerConsensusPosition{
			DecisionNum: oi.DecisionNum,
			BatchIndex:  oi.BatchIndex + 1,
		}
	}
	return core.AssemblerConsensusPosition{
		DecisionNum: oi.DecisionNum + 1,
	}
}
