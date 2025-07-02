/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/grpclog"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func init() {
	// set the gRPC logger to a logger that discards the log output.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
}

type routerTestSetup struct {
	ca         tlsgen.CA
	batchers   []*stubBatcher
	clientConn *grpc.ClientConn
	router     *router.Router
}

func (r *routerTestSetup) Close() {
	if r.router != nil {
		r.router.Stop()
	}

	if r.clientConn != nil {
		r.clientConn.Close()
	}

	for _, batcher := range r.batchers {
		batcher.server.Stop()
	}
}

func createRouterTestSetup(t *testing.T, partyID types.PartyID, numOfShards int, useTLS bool, clientAuthRequired bool) *routerTestSetup {
	// create a CA that issues a certificate for the router and the batchers
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	// create stub batchers
	var batchers []*stubBatcher
	for i := 0; i < numOfShards; i++ {
		batcher := NewStubBatcher(t, ca, partyID, types.ShardID(i+1))
		batchers = append(batchers, &batcher)
	}

	// start the batchers
	for _, batcher := range batchers {
		batcher.Start()
	}

	// create and start router
	router := createAndStartRouter(t, partyID, ca, batchers, useTLS, clientAuthRequired)

	return &routerTestSetup{
		ca:       ca,
		batchers: batchers,
		router:   router,
	}
}

// Scenario:
// 1. start a client, router and stub batcher
// 2. submit 10 requests by client to router
// 3. broadcast 10 requests by client to router
// 4. check that the batcher received the expected number of requests
func TestStubBatcherReceivesClientRouterRequests(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	err = submitStreamRequests(testSetup.clientConn, 10)
	require.NoError(t, err)

	err = submitBroadcastRequests(testSetup.clientConn, 10)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(20)
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a client, router and stub batcher
// 2. submit a request by client to router
// 3. broadcast a request by client to router
// 4. check that the batcher received one request
func TestStubBatcherReceivesClientRouterSingleRequest(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	err = submitRequest(testSetup.clientConn)
	require.NoError(t, err)

	err = submitBroadcastRequests(testSetup.clientConn, 1)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestClientRouterFailsToSendRequestOnBatcherServerStop(t *testing.T) {
	t.Skip()
	// TODO: check if the reason for error is the connectivity to batcher
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	// send request, should succeed
	err = submitRequest(testSetup.clientConn)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	// stop the batcher and send request, expect to error
	testSetup.batchers[0].Stop()
	err = submitRequest(testSetup.clientConn)
	require.NotNil(t, err)
	require.EqualError(t, err, "receiving response with error: could not establish stream to "+testSetup.batchers[0].server.Address())
}

// Scenario:
// 1. start a client, router and 2 stub batchers (2 shards)
// 2. send a request by client to router
// 3. check that a batcher received one request
func TestClientRouterSubmitSingleRequestAgainstMultipleBatchers(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 2, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	err = submitRequest(testSetup.clientConn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return (testSetup.batchers[0].ReceivedMessageCount() == uint32(1) && testSetup.batchers[1].ReceivedMessageCount() == uint32(0)) || (testSetup.batchers[0].ReceivedMessageCount() == uint32(0) && testSetup.batchers[1].ReceivedMessageCount() == uint32(1))
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a client, router and 2 stub batchers (2 shards)
// 2. send 10 requests by client to router
// 3. check that the batchers received the expected number of requests
func TestClientRouterSubmitStreamRequestsAgainstMultipleBatchers(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	err = submitStreamRequests(testSetup.clientConn, 10)
	require.NoError(t, err)

	recvCond := func() uint32 {
		receivedTxCount := uint32(0)
		for i := 0; i < numOfShards; i++ {
			receivedTxCount += testSetup.batchers[i].ReceivedMessageCount()
		}
		return receivedTxCount
	}

	require.Eventually(t, func() bool {
		return recvCond() == uint32(10)
	}, 60*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a client, router and 2 stub batchers
// 2. broadcast 10 requests by client to router
// 3. check that the batchers received all the requests
func TestClientRouterBroadcastRequestsAgainstMultipleBatchers(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	err = submitBroadcastRequests(testSetup.clientConn, 10)
	require.NoError(t, err)

	recvCond := func() uint32 {
		receivedTxCount := uint32(0)
		for i := 0; i < numOfShards; i++ {
			receivedTxCount += testSetup.batchers[i].ReceivedMessageCount()
		}
		return receivedTxCount
	}

	require.Eventually(t, func() bool {
		return recvCond() == uint32(10)
	}, 60*time.Second, 10*time.Millisecond)
}

func createServerTLSClientConnection(testSetup *routerTestSetup, ca tlsgen.CA) error {
	cc := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:        true,
			ServerRootCAs: [][]byte{ca.CertBytes()},
		},
		DialTimeout:  time.Second,
		AsyncConnect: false,
	}

	var err error
	testSetup.clientConn, err = cc.Dial(testSetup.router.Address())

	return err
}

func submitStreamRequests(conn *grpc.ClientConn, numOfRequests int) error {
	cl := protos.NewRequestTransmitClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cl.SubmitStream(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		buff := make([]byte, 300)
		for j := 0; j < numOfRequests; j++ {
			binary.BigEndian.PutUint32(buff, uint32(j))
			err := stream.Send(&protos.Request{Payload: buff})
			if err != nil {
				return
			}
		}
	}()

	for j := 0; j < numOfRequests; j++ {
		select {
		default:
			resp, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("error receiving response: %w", err)
			}
			if resp.Error != "" {
				return fmt.Errorf("receiving response with error: %s", resp.Error)
			}
		case <-ctx.Done():
			return fmt.Errorf("a time out occured during submitting request: %w", ctx.Err())
		}
	}

	wg.Wait()

	return nil
}

func submitBroadcastRequests(conn *grpc.ClientConn, numOfRequests int) error {
	cl := ab.NewAtomicBroadcastClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cl.Broadcast(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		buff := make([]byte, 300)
		for j := 0; j < numOfRequests; j++ {
			binary.BigEndian.PutUint32(buff, uint32(j))
			err := stream.Send(&common.Envelope{Payload: buff})
			if err != nil {
				return
			}
		}
	}()

	for j := 0; j < numOfRequests; j++ {
		select {
		default:
			resp, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("error receiving response: %w", err)
			}
			if resp.Status != 200 {
				return fmt.Errorf("receiving response with error: %s", resp.Status)
			}
		case <-ctx.Done():
			return fmt.Errorf("a time out occured during submitting request: %w", ctx.Err())
		}
	}

	wg.Wait()

	return nil
}

func submitRequest(conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	buff := make([]byte, 300)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	req := &protos.Request{
		Payload: buff,
	}

	cl := protos.NewRequestTransmitClient(conn)
	resp, err := cl.Submit(ctx, req)
	if err != nil {
		return fmt.Errorf("error receiving response: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("receiving response with error: %s", resp.Error)
	}

	return nil
}

func createAndStartRouter(t *testing.T, partyID types.PartyID, ca tlsgen.CA, batchers []*stubBatcher, useTLS bool, clientAuthRequired bool) *router.Router {
	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	logger := testutil.CreateLogger(t, 0)

	// create router config
	var shards []config.ShardInfo
	for j := 0; j < len(batchers); j++ {
		shards = append(shards, config.ShardInfo{ShardId: types.ShardID(j + 1), Batchers: []config.BatcherInfo{{PartyID: 1, Endpoint: batchers[j].server.Address(), TLSCACerts: []config.RawBytes{ca.CertBytes()}}}})
	}

	conf := &config.RouterNodeConfig{
		PartyID:            partyID,
		TLSCertificateFile: ckp.Cert,
		UseTLS:             useTLS,
		TLSPrivateKeyFile:  ckp.Key,
		ListenAddress:      "127.0.0.1:0",
		ClientAuthRequired: clientAuthRequired,
		Shards:             shards,
	}

	r := router.NewRouter(conf, logger)
	r.StartRouterService()

	return r
}
