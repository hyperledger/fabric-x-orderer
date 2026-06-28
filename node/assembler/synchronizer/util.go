/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/pkg/errors"
)

// ledgerInfoAdapter translates from blocksprovider.LedgerInfo in to calls to AssemblerSupport.
type ledgerInfoAdapter struct {
	support AssemblerSupport
}

func (a *ledgerInfoAdapter) LedgerHeight() (uint64, error) {
	return a.support.Height(), nil
}

func (a *ledgerInfoAdapter) GetCurrentBlockHash() ([]byte, error) {
	return nil, errors.New("not implemented: never used in orderer")
}

//go:generate counterfeiter -o mocks/bft_deliverer_factory.go --fake-name BFTDelivererFactory . BFTDelivererFactory
type BFTDelivererFactory interface {
	CreateBFTDeliverer(
		channelID string,
		blockHandler blocksprovider.BlockHandler,
		ledger blocksprovider.LedgerInfo,
		updatableBlockVerifier blocksprovider.UpdatableBlockVerifier,
		dialer blocksprovider.Dialer,
		orderersSourceFactory blocksprovider.OrdererConnectionSourceFactory,
		cryptoProvider bccsp.BCCSP,
		doneC chan struct{},
		signer identity.SignerSerializer,
		deliverStreamer blocksprovider.DeliverStreamer,
		censorshipDetectorFactory blocksprovider.CensorshipDetectorFactory,
		endpointsExtractor blocksprovider.EndpointsExtractor,
		logger *flogging.FabricLogger,
		initialRetryInterval time.Duration,
		maxRetryInterval time.Duration,
		blockCensorshipTimeout time.Duration,
		maxRetryDuration time.Duration,
		maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler,
		tlsCertHash []byte,
	) BFTBlockDeliverer
}

type bftDelivererCreator struct{}

func (*bftDelivererCreator) CreateBFTDeliverer(
	channelID string,
	blockHandler blocksprovider.BlockHandler,
	ledger blocksprovider.LedgerInfo,
	updatableBlockVerifier blocksprovider.UpdatableBlockVerifier,
	dialer blocksprovider.Dialer,
	orderersSourceFactory blocksprovider.OrdererConnectionSourceFactory,
	cryptoProvider bccsp.BCCSP,
	doneC chan struct{},
	signer identity.SignerSerializer,
	deliverStreamer blocksprovider.DeliverStreamer,
	censorshipDetectorFactory blocksprovider.CensorshipDetectorFactory,
	endpointsExtractor blocksprovider.EndpointsExtractor,
	logger *flogging.FabricLogger,
	initialRetryInterval time.Duration,
	maxRetryInterval time.Duration,
	blockCensorshipTimeout time.Duration,
	maxRetryDuration time.Duration,
	maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler,
	tlsCertHash []byte,
) BFTBlockDeliverer {
	bftDeliverer := &blocksprovider.BFTDeliverer{
		ChannelID:                       channelID,
		BlockHandler:                    blockHandler,
		Ledger:                          ledger,
		UpdatableBlockVerifier:          updatableBlockVerifier,
		Dialer:                          dialer,
		OrderersSourceFactory:           orderersSourceFactory,
		CryptoProvider:                  cryptoProvider,
		DoneC:                           doneC,
		Signer:                          signer,
		DeliverStreamer:                 deliverStreamer,
		CensorshipDetectorFactory:       censorshipDetectorFactory,
		ConfigBlockOps:                  &utils.CommonConfigBlockOperations{},
		EndpointsExtractor:              endpointsExtractor,
		Logger:                          logger,
		InitialRetryInterval:            initialRetryInterval,
		MaxRetryInterval:                maxRetryInterval,
		BlockCensorshipTimeout:          blockCensorshipTimeout,
		MaxRetryDuration:                maxRetryDuration,
		MaxRetryDurationExceededHandler: maxRetryDurationExceededHandler,
		TLSCertHash:                     tlsCertHash,
	}
	return bftDeliverer
}

//go:generate counterfeiter -o mocks/bft_block_deliverer.go --fake-name BFTBlockDeliverer . BFTBlockDeliverer
type BFTBlockDeliverer interface {
	Stop()
	DeliverBlocks()
	Initialize(channelConfig *cb.Config, selfPartyID types.PartyID)
}

// AssemblerEndpointsExtractor implements blocksprovider.EndpointsExtractor
type AssemblerEndpointsExtractor struct{}

// ExtractEndpoints extracts consenter endpoints from the given orderer configuration
func (e *AssemblerEndpointsExtractor) ExtractEndpoints(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error) {
	return config.ExtractAssemblerAddresses(ordererConfig)
}
