/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Serve a deliver service that answers every request with a block.
//  2. Pull one block with a request factory that fails the first time it is called.
//  3. Expect the block to be pulled, by a request the factory built on its second call.
func TestPullOneRetriesARequestThatFailedToBeCreated(t *testing.T) {
	endpoint := serveOneBlockPerRequest(t)
	factory, calls := failingOnceRequestFactory()

	block, err := delivery.PullOne(
		context.Background(), "arma", testutil.CreateLogger(t, 1), endpoint, factory, comm.ClientConfig{DialTimeout: time.Second}, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, int32(2), calls.Load())
}

// Scenario:
//  1. Serve a deliver service that answers every request with a block.
//  2. Pull blocks with a request factory that fails the first time it is called.
//  3. Expect a block to be pulled, by a request the factory built on its second call.
func TestPullRetriesARequestThatFailedToBeCreated(t *testing.T) {
	endpoint := serveOneBlockPerRequest(t)
	factory, calls := failingOnceRequestFactory()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	blocks := make(chan *common.Block, 1)
	go delivery.Pull(
		ctx, "arma", testutil.CreateLogger(t, 1), func() string { return endpoint }, factory, comm.ClientConfig{DialTimeout: time.Second},
		func(block *common.Block) {
			select {
			case blocks <- block:
			default:
			}
		},
		nil,
	)

	select {
	case block := <-blocks:
		require.NotNil(t, block)
	case <-time.After(10 * time.Second):
		t.Fatal("no block was pulled")
	}
	require.Equal(t, int32(2), calls.Load())
}

// failingOnceRequestFactory returns a request factory that fails on its first call and builds an empty
// request on every later one, and the number of times it was called.
func failingOnceRequestFactory() (func() (*common.Envelope, error), *atomic.Int32) {
	calls := &atomic.Int32{}
	factory := func() (*common.Envelope, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("the signer is not available")
		}
		return &common.Envelope{}, nil
	}

	return factory, calls
}

// serveOneBlockPerRequest serves a deliver service that answers every request with a block, and
// returns the endpoint it listens on.
func serveOneBlockPerRequest(t *testing.T) string {
	server, err := comm.NewGRPCServer(testutil.AllocateLocalhostAddress(t), comm.ServerConfig{})
	require.NoError(t, err)

	orderer.RegisterAtomicBroadcastServer(server.Server(), &oneBlockPerRequest{})

	go func() {
		if err := server.Start(); err != nil {
			fmt.Printf("deliver service stopped serving: %s\n", err)
		}
	}()
	t.Cleanup(server.Stop)

	return server.Address()
}

// oneBlockPerRequest answers every deliver request with a block.
type oneBlockPerRequest struct {
	orderer.UnimplementedAtomicBroadcastServer
}

func (s *oneBlockPerRequest) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}

	block := &common.Block{
		Header: &common.BlockHeader{Number: 0},
		Data:   &common.BlockData{Data: [][]byte{[]byte("tx")}},
	}

	if err := stream.Send(&orderer.DeliverResponse{Type: &orderer.DeliverResponse_Block{Block: block}}); err != nil {
		return err
	}

	<-stream.Context().Done()
	return nil
}
