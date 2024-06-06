package node

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
)

type DeliverService map[string]blockledger.ReadWriter

func (d DeliverService) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (d DeliverService) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	handler := &deliver.Handler{
		ChainManager:     &chainManager{ledgersPerChain: d},
		BindingInspector: &noopBindingInspector{},
		TimeWindow:       time.Hour,
		Metrics:          deliver.NewMetrics(&disabled.Provider{}),
		ExpirationCheckFunc: func(identityBytes []byte) time.Time {
			return time.Now().Add(time.Hour * 365 * 24)
		},
	}

	return handler.Handle(context.Background(), &deliver.Server{
		PolicyChecker:  &policyChecker{},
		ResponseSender: &responseSender{stream: stream},
		Receiver:       stream,
	})
}

type responseSender struct {
	stream orderer.AtomicBroadcast_DeliverServer
}

func (r *responseSender) SendStatusResponse(status common.Status) error {
	return nil
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

type policyChecker struct {
}

func (p *policyChecker) CheckPolicy(envelope *common.Envelope, channelID string) error {
	return nil
}

type chainManager struct {
	ledgersPerChain map[string]blockledger.ReadWriter
}

func (c *chainManager) GetChain(chainID string) deliver.Chain {
	return &chain{ledger: c.ledgersPerChain[chainID]}
}

type chain struct {
	ledger blockledger.ReadWriter
}

func (c *chain) Sequence() uint64 {
	return 0
}

func (c *chain) PolicyManager() policies.Manager {
	panic("implement me")
}

func (c *chain) Reader() blockledger.Reader {
	return &delayedReader{Reader: c.ledger}
}

func (c *chain) Errored() <-chan struct{} {
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

type noopBindingInspector struct {
}

func (nbi noopBindingInspector) Inspect(context.Context, proto.Message) error {
	return nil
}
