package delivery

import (
	"context"
	"encoding/binary"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/comm"
	"arma/node/config"
	"arma/node/consensus/state"

	"github.com/pkg/errors"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
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

func (bar *ConsensusReplicator) ReplicateState() <-chan *core.State {
	endpoint := func() string {
		return bar.endpoint
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
			bar.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	res := make(chan *core.State, 100)

	blockHandlerFunc := func(block *common.Block) {
		header := extractHeaderFromBlock(block, bar.logger)
		res <- header.State
	}

	go Pull(context.Background(), "consensus", bar.logger, endpoint, requestEnvelopeFactoryFunc, bar.cc, blockHandlerFunc)

	return res
}

func (bar *ConsensusReplicator) Replicate(seq uint64) <-chan core.OrderedBatchAttestation {
	endpoint := func() string {
		return bar.endpoint
	}

	requestEnvelopeFactoryFunc := func() *common.Envelope {
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

		return requestEnvelope
	}

	res := make(chan core.OrderedBatchAttestation, 100)

	blockHandlerFunc := func(block *common.Block) {
		header, sigs, err2 := extractHeaderAndSigsFromBlock(block)
		if err2 != nil {
			bar.logger.Panicf("Failed extracting ordered batch attestation from decision: %s", err2)
		}

		bar.logger.Infof("Extracted a decision header with %d available blocks", len(header.AvailableBlocks))
		for index, ab := range header.AvailableBlocks {
			bar.logger.Infof("BA index %d with: shard %d, primary %d, seq %d", index, ab.Batch.Shard(), ab.Batch.Primary(), ab.Batch.Seq())
			bar.logger.Infof("BA block header: %+v", ab.Header)
			bar.logger.Infof("BA block signers: %+v", signersFromSigs(sigs[index]))
			bar.logger.Infof("BA digest: %s; block header digest: %s, ", core.ShortDigestString(ab.Batch.Digest()), core.ShortDigestString(ab.Header.Digest))

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
			bar.logger.Debugf("AvailableBatchOrdered: %+v", abo)

			res <- abo
		}
	}

	go Pull(context.Background(), "consensus", bar.logger, endpoint, requestEnvelopeFactoryFunc, bar.cc, blockHandlerFunc)

	return res
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
