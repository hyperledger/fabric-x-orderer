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
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

type worker struct {
	work      [][]byte
	f         func([]byte)
	workerNum int
	id        int
}

func (w *worker) doWork() {
	// sanity check
	if w.workerNum == 0 {
		panic("worker number is not defined")
	}

	if w.f == nil {
		panic("worker function is not defined")
	}

	if len(w.work) == 0 {
		panic("work is not defined")
	}

	for i, datum := range w.work {
		if i%w.workerNum != w.id {
			continue
		}

		w.f(datum)
	}
}

// ledgerInfoAdapter translates from blocksprovider.LedgerInfo in to calls to ConsenterSupport.
type ledgerInfoAdapter struct {
	support ConsenterSupport
}

func (a *ledgerInfoAdapter) LedgerHeight() (uint64, error) {
	return a.support.Height(), nil
}

func (a *ledgerInfoAdapter) GetCurrentBlockHash() ([]byte, error) {
	return nil, errors.New("not implemented: never used in orderer")
}

//go:generate counterfeiter -o mocks/verifier_factory.go --fake-name VerifierFactory . VerifierFactory

type VerifierFactory interface {
	CreateBlockVerifier(
		configBlock *cb.Block,
		lastBlock *cb.Block,
		cryptoProvider bccsp.BCCSP,
		lg *flogging.FabricLogger,
	) (deliverclient.CloneableUpdatableBlockVerifier, error)
}

type verifierCreator struct{}

func (*verifierCreator) CreateBlockVerifier(
	configBlock *cb.Block,
	lastBlock *cb.Block,
	cryptoProvider bccsp.BCCSP,
	lg *flogging.FabricLogger,
) (deliverclient.CloneableUpdatableBlockVerifier, error) {
	updatableVerifier, err := deliverclient.NewBlockVerificationAssistant(configBlock, lastBlock, cryptoProvider, lg)
	return updatableVerifier, err
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
		logger *flogging.FabricLogger,
		initialRetryInterval time.Duration,
		maxRetryInterval time.Duration,
		blockCensorshipTimeout time.Duration,
		maxRetryDuration time.Duration,
		maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler,
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
	logger *flogging.FabricLogger,
	initialRetryInterval time.Duration,
	maxRetryInterval time.Duration,
	blockCensorshipTimeout time.Duration,
	maxRetryDuration time.Duration,
	maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler,
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
		Logger:                          logger,
		InitialRetryInterval:            initialRetryInterval,
		MaxRetryInterval:                maxRetryInterval,
		BlockCensorshipTimeout:          blockCensorshipTimeout,
		MaxRetryDuration:                maxRetryDuration,
		MaxRetryDurationExceededHandler: maxRetryDurationExceededHandler,
	}
	return bftDeliverer
}

//go:generate counterfeiter -o mocks/bft_block_deliverer.go --fake-name BFTBlockDeliverer . BFTBlockDeliverer
type BFTBlockDeliverer interface {
	Stop()
	DeliverBlocks()
	Initialize(channelConfig *cb.Config, selfPartyID types.PartyID)
}
