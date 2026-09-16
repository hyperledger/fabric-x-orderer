/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// DeliverService serves the decisions of consensus over the single channel they are written to.
type DeliverService struct {
	channelName string
	ledger      blockledger.Reader
	// accessControl authorizes a request by the node that signed it.
	accessControl *deliver.NodeVerifier
}

// NewDeliverService serves the decisions of a channel out of the consensus ledger, to the nodes the
// shared configuration of the bundle holds.
func NewDeliverService(channelID string, consensusLedger blockledger.Reader, bundle channelconfig.Resources) (*DeliverService, error) {
	accessControl, err := deliver.NewConsenterDeliverVerifier(bundle)
	if err != nil {
		return nil, errors.Wrap(err, "failed building the access control of the consensus deliver service")
	}

	return &DeliverService{
		channelName:   DecisionChannelName(channelID),
		ledger:        consensusLedger,
		accessControl: accessControl,
	}, nil
}

func (d *DeliverService) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (d *DeliverService) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	handler := &deliver.Handler{
		ChainManager:     &chainManager{channelName: d.channelName, ledger: d.ledger},
		BindingInspector: &noopBindingInspector{},
		TimeWindow:       time.Hour,
		Metrics:          deliver.NewMetrics(&disabled.Provider{}),
		ExpirationCheckFunc: func(identityBytes []byte) time.Time {
			return time.Now().Add(time.Hour * 365 * 24)
		},
		ConfigBlockOps: &state.ConsenterConfigBlockOperations{},
	}

	return handler.Handle(context.Background(), &deliver.Server{
		PolicyChecker:  d.accessControl,
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

type chainManager struct {
	channelName string
	ledger      blockledger.Reader
}

func (c *chainManager) GetChain(chainID string) deliver.Chain {
	if chainID != c.channelName {
		return nil
	}

	return &chain{ledger: c.ledger}
}

type chain struct {
	ledger blockledger.Reader
}

func (c *chain) Sequence() uint64 {
	return 0
}

func (c *chain) PolicyManager() policies.Manager {
	// TODO inplement authorization: channel readers
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

type noopBindingInspector struct{}

func (nbi *noopBindingInspector) Inspect(context.Context, proto.Message) error {
	// TODO check the TLS binding
	return nil
}

// DecisionChannelName returns the name of the channel used for consensus decision replication.
func DecisionChannelName(channelID string) string {
	return fmt.Sprintf("consensus-%s", channelID)
}
