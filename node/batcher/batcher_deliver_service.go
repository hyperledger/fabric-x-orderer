package batcher

import (
	"context"
	"fmt"
	"time"

	"arma/core"
	"arma/node/ledger"

	//lint:ignore SA1019 since we are reusing Fabric's delivery service, we must use the old proto package
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
)

// TODO The deliver service and client (puller) were copied almost as is from Fabric.
// Both the server and side and client side will need to go a revision.

type BatcherDeliverService struct {
	LedgerArray *ledger.BatchLedgerArray
	Logger      core.Logger
}

func (d *BatcherDeliverService) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (d *BatcherDeliverService) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	handler := &deliver.Handler{
		ChainManager:     &chainManager{ledgerArray: d.LedgerArray, logger: d.Logger},
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

type policyChecker struct{}

func (p *policyChecker) CheckPolicy(envelope *common.Envelope, channelID string) error {
	return nil
}

type chainManager struct {
	ledgerArray *ledger.BatchLedgerArray
	logger      core.Logger
}

func (c *chainManager) GetChain(chainID string) deliver.Chain {
	shardID, partyID, err := ledger.ChannelNameToShardParty(chainID)
	if err != nil {
		c.logger.Errorf("Failed to parse channel name to ShardID and PartyID: %s", err)
		return nil
	}
	if shardID != c.ledgerArray.ShardID() {
		c.logger.Errorf("Requested shard does not match this shard: requested=%d, this=%d", shardID, c.ledgerArray.ShardID())
		return nil
	}
	// TODO ledger array should NOT panic if the party is not found
	if ledger := c.ledgerArray.Part(partyID); ledger == nil {
		return nil
	} else {
		return &chainReader{ledger: ledger.Ledger()}
	}
}

type chainReader struct {
	ledger blockledger.Reader
}

func (c *chainReader) Sequence() uint64 {
	return 0
}

func (c *chainReader) PolicyManager() policies.Manager {
	panic("implement me")
}

func (c *chainReader) Reader() blockledger.Reader {
	return &delayedReader{Reader: c.ledger}
}

func (c *chainReader) Errored() <-chan struct{} {
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

func (nbi noopBindingInspector) Inspect(context.Context, proto.Message) error {
	return nil
}
