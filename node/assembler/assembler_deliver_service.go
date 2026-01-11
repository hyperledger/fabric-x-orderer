/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"context"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"

	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"

	"github.com/hyperledger/fabric-x-common/protoutil"

	"google.golang.org/protobuf/proto"
)

type AssemblerDeliverService struct {
	blockledger blockledger.Reader
	bundle      channelconfig.Resources
	Logger      types.Logger
}

func NewAssemblerDeliverService(ledger blockledger.Reader, logger types.Logger, config *config.AssemblerNodeConfig) *AssemblerDeliverService {
	return &AssemblerDeliverService{
		blockledger: ledger,
		bundle:      config.Bundle,
		Logger:      logger,
	}
}

func (a AssemblerDeliverService) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (a AssemblerDeliverService) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	handler := &deliver.Handler{
		ChainManager:     &assemblerChainManager{ledger: a.blockledger, bundle: a.bundle},
		BindingInspector: &noopBindingInspector{},
		TimeWindow:       time.Hour,
		Metrics:          deliver.NewMetrics(&disabled.Provider{}),
		ExpirationCheckFunc: func(identityBytes []byte) time.Time {
			return time.Now().Add(time.Hour * 365 * 24)
		},
	}

	policyChecker := func(env *common.Envelope, channelID string) error {
		asf := NewAssemblerSigFilter(a.bundle)
		return asf.Apply(env)
	}

	return handler.Handle(context.Background(), &deliver.Server{
		PolicyChecker:  deliver.PolicyCheckerFunc(policyChecker),
		ResponseSender: &responseSender{stream: stream},
		Receiver:       stream,
	})
}

type responseSender struct {
	stream orderer.AtomicBroadcast_DeliverServer
}

func (r *responseSender) SendStatusResponse(status common.Status) error {
	return r.stream.Send(&orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Status{
			Status: status,
		},
	})
}

func (r *responseSender) SendBlockResponse(block *common.Block, channelID string, chain deliver.Chain, signedData *protoutil.SignedData) error {
	return r.stream.Send(&orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Block{
			Block: block,
		},
	})
}

func (r *responseSender) DataType() string {
	return "block"
}

type assemblerChainManager struct {
	ledger blockledger.Reader
	bundle channelconfig.Resources
}

func (acm *assemblerChainManager) GetChain(chainID string) deliver.Chain {
	return &assemblerChain{ledger: acm.ledger, bundle: acm.bundle}
}

type assemblerChain struct {
	ledger blockledger.Reader
	bundle channelconfig.Resources
}

func (c *assemblerChain) Sequence() uint64 {
	return c.bundle.ConfigtxValidator().Sequence()
}

func (c *assemblerChain) PolicyManager() policies.Manager {
	return c.bundle.PolicyManager()
}

func (c *assemblerChain) Reader() blockledger.Reader {
	return &delayedReader{Reader: c.ledger}
}

func (c *assemblerChain) Errored() <-chan struct{} {
	return make(chan struct{})
}

type delayedReader struct {
	blockledger.Reader
}

func (d *delayedReader) Iterator(startType *orderer.SeekPosition) (blockledger.Iterator, uint64) {
	for d.Height() == 0 {
		time.Sleep(time.Millisecond)
	}
	return d.Reader.Iterator(startType)
}

type noopBindingInspector struct{}

func (nbi *noopBindingInspector) Inspect(context.Context, proto.Message) error {
	// TODO check the TLS binding
	return nil
}

type assemblerSigFilter struct {
	bundle channelconfig.Resources
}

func NewAssemblerSigFilter(bundle channelconfig.Resources) *assemblerSigFilter {
	return &assemblerSigFilter{bundle: bundle}
}

func (asf *assemblerSigFilter) Apply(env *common.Envelope) error {
	signedData, err := protoutil.EnvelopeAsSignedData(env)
	if err != nil {
		return fmt.Errorf("could not convert message to signedData: %s", err)
	}

	policy, ok := asf.bundle.PolicyManager().GetPolicy(policies.ChannelReaders)
	if !ok {
		return fmt.Errorf("could not find policy %s", policies.ChannelReaders)
	}

	err = policy.EvaluateSignedData(signedData)
	if err != nil {
		return fmt.Errorf("AssemblerSigFilter evaluation failed: %s", err)
	}
	return nil
}
