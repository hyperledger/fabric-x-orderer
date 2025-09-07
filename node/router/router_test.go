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
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

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

func (r *routerTestSetup) isReconnectComplete() bool {
	return r.router.IsAllStreamsOK()
}

func (r *routerTestSetup) isDisconnectedFromBatcher() bool {
	return r.router.IsAllConnectionsDown()
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

	res := submitStreamRequests(testSetup.clientConn, 10)
	require.NoError(t, res.err)

	res = submitBroadcastRequests(testSetup.clientConn, 10)
	require.NoError(t, res.err)

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

	res := submitBroadcastRequests(testSetup.clientConn, 1)
	require.NoError(t, res.err)

	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a client, router and 1 stub batcher (1 shard)
// 2. stop the batcher
// 3. submit a request. should fail when router tries to send it to batcher
// 4. submit the request again. now the stream is faulty and will try to reconnect, but will fail.
// 5. restart the batcher
// 6. submit the request again. now it should succeed
func TestSubmitOnBatcherStopAndRestart(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	defer testSetup.Close()
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	// stop the batcher
	testSetup.batchers[0].Stop()

	// wait for all streams to become faulty
	require.Eventually(t, func() bool {
		return testSetup.isDisconnectedFromBatcher()
	}, 10*time.Second, 200*time.Millisecond)

	// send 1 request with Submit, should get server error
	err = submitRequest(testSetup.clientConn)
	require.EqualError(t, err, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].server.Address()+" is broken, try again later")

	// send same request again.
	err = submitRequest(testSetup.clientConn)
	require.EqualError(t, err, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].server.Address()+" is broken, try again later")

	// restart the batcher
	testSetup.batchers[0].Restart()

	// wait for reconnection
	require.Eventually(t, func() bool {
		return testSetup.isReconnectComplete()
	}, 10*time.Second, 10*time.Millisecond)

	// send request, should succeed
	err = submitRequest(testSetup.clientConn)
	require.NoError(t, err)
}

// Scenario:
//  1. start a client, router and 1 stub batcher (1 shard)
//  2. stop the batcher
//  3. submit a stream of 20 requests. should all fail, when router tries to send it to
//     batcher, or when it tries to reconnect
//  4. submit the requests again. Now the stream is faulty and will try to reconnect, but all will fail.
//  5. restart the batcher
//  6. submit the requests again. now all should succeed
func TestSubmitStreamOnBatcherStopAndRestart(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	defer testSetup.Close()
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	// stop the batcher
	testSetup.batchers[0].Stop()

	// wait for all streams to become faulty
	require.Eventually(t, func() bool {
		return testSetup.isDisconnectedFromBatcher()
	}, 10*time.Second, 200*time.Millisecond)

	// send 20 requests with SubmitStream, should get 20 server errors
	numOfRequests := 20
	res := submitStreamRequests(testSetup.clientConn, numOfRequests)
	require.Equal(t, 0, res.successRequests)
	require.Equal(t, numOfRequests, res.failRequests)
	require.Equal(t, numOfRequests, len(res.respondsErrors))
	for _, e := range res.respondsErrors {
		require.EqualError(t, e, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].server.Address()+" is broken, try again later")
	}

	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(0)
	}, 10*time.Second, 10*time.Millisecond)

	// send 5 requests (of the previous 20) with SubmitStream, should get 5 server errors again
	numOfRequests = 5
	res = submitStreamRequests(testSetup.clientConn, numOfRequests)
	require.Equal(t, 0, res.successRequests)
	require.Equal(t, numOfRequests, res.failRequests)
	require.Equal(t, numOfRequests, len(res.respondsErrors))
	for _, e := range res.respondsErrors {
		require.EqualError(t, e, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].server.Address()+" is broken, try again later")
	}
	require.Equal(t, uint32(0), testSetup.batchers[0].ReceivedMessageCount())

	// restart the batcher
	testSetup.batchers[0].Restart()

	// wait for reconnection
	require.Eventually(t, func() bool {
		return testSetup.isReconnectComplete()
	}, 10*time.Second, 10*time.Millisecond)

	// send 20 requests, should succeed all requests
	numOfRequests = 20
	res = submitStreamRequests(testSetup.clientConn, numOfRequests)
	require.NoError(t, res.err)
	require.Equal(t, numOfRequests, res.successRequests)
	require.Equal(t, 0, res.failRequests)
	require.Equal(t, 0, len(res.respondsErrors))
	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(numOfRequests)
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. start a client, router and 1 stub batcher (1 shard)
//  2. stop the batcher
//  3. broadcast a stream of 5 requests. some may fail if they are mapped to a known faulty stream
//  5. restart the batcher
//  6. broadcast the requsts again. now all should succeed
func TestBroadcastOnBatcherStopAndRestart(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	defer testSetup.Close()
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	numOfRequests := 5

	// stop the batcher
	testSetup.batchers[0].Stop()

	// wait for all streams to become faulty
	require.Eventually(t, func() bool {
		return testSetup.isDisconnectedFromBatcher()
	}, 10*time.Second, 200*time.Millisecond)

	// Broadcast 5 requests. should get server error
	res := submitBroadcastRequests(testSetup.clientConn, numOfRequests)
	require.Equal(t, 0, res.successRequests)
	require.Equal(t, numOfRequests, res.failRequests)
	require.Equal(t, numOfRequests, len(res.respondsErrors))
	for _, e := range res.respondsErrors {
		require.EqualError(t, e, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].server.Address()+" is broken, try again later")
	}

	// restart the batcher
	testSetup.batchers[0].Restart()

	// wait for reconnection
	require.Eventually(t, func() bool {
		return testSetup.isReconnectComplete()
	}, 10*time.Second, 10*time.Millisecond)

	// Broadcast same 5 requests again. expect success
	res = submitBroadcastRequests(testSetup.clientConn, numOfRequests)
	require.NoError(t, res.err)
	require.Equal(t, numOfRequests, res.successRequests)
	require.Equal(t, 0, res.failRequests)
	require.Equal(t, 0, len(res.respondsErrors))
	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(numOfRequests)
	}, 10*time.Second, 10*time.Millisecond)
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

	res := submitStreamRequests(testSetup.clientConn, 10)
	require.NoError(t, res.err)

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

	res := submitBroadcastRequests(testSetup.clientConn, 10)
	require.NoError(t, res.err)

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

// test request filters
// 1) Start a client, router and stub batcher
// 2) Send valid request, expect no error.
// 3) Send request with empty payload, expect error.
// 4) Send request that exceed the maximal size, expect error.
// 5) ** Not implemented ** send request with bad signature, expect error.
func TestRequestFilters(t *testing.T) {
	// 1) Start a client, router and stub batcher
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	defer testSetup.Close()
	conn := testSetup.clientConn
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	cl := protos.NewRequestTransmitClient(conn)
	defer cancel()

	// 2) send a valid request.
	buff := make([]byte, 300)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	req := tx.CreateStructuredRequest(buff)
	resp, err := cl.Submit(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "", resp.Error)

	// 3) send request with empty payload.
	req = &protos.Request{
		Payload: nil,
	}
	resp, err = cl.Submit(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "request verification error: empty payload field", resp.Error)
	// 4) send request with payload too big. (3000 is more than 1 << 10, the maximal request size in bytes)
	buff = make([]byte, 3000)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	req = &protos.Request{
		Payload: buff,
	}
	resp, err = cl.Submit(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "request verification error: the request's size exceeds the maximum size: actual = 3000, limit = 1024", resp.Error)

	// 5) send request with invalid signature. Not implemented
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

type testStreamResult struct {
	successRequests int
	failRequests    int
	err             error
	respondsErrors  []error
}

func submitStreamRequests(conn *grpc.ClientConn, numOfRequests int) (res testStreamResult) {
	res = testStreamResult{
		failRequests: numOfRequests,
	}

	cl := protos.NewRequestTransmitClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := cl.SubmitStream(ctx)
	if err != nil {
		res.err = err
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		buff := make([]byte, 300)
		for j := 0; j < numOfRequests; j++ {
			binary.BigEndian.PutUint32(buff, uint32(j))
			req := tx.CreateStructuredRequest(buff)
			err := stream.Send(req)
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
				res.err = fmt.Errorf("error receiving response: %s", err)
			}
			if resp.Error != "" {
				requestErr := fmt.Errorf("receiving response with error: %s", resp.Error)
				res.respondsErrors = append(res.respondsErrors, requestErr)
				res.err = requestErr
			} else {
				res.successRequests++
				res.failRequests--
			}
		case <-ctx.Done():
			res.err = fmt.Errorf("a time out occured during submitting request: %w", ctx.Err())
		}
	}

	wg.Wait()

	return
}

func submitBroadcastRequests(conn *grpc.ClientConn, numOfRequests int) (res testStreamResult) {
	res = testStreamResult{
		failRequests: numOfRequests,
	}

	cl := ab.NewAtomicBroadcastClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cl.Broadcast(ctx)
	if err != nil {
		res.err = err
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		buff := make([]byte, 300)
		for j := 0; j < numOfRequests; j++ {
			binary.BigEndian.PutUint32(buff, uint32(j))
			env := tx.CreateStructuredEnvelope(buff)
			err := stream.Send(env)
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
				res.err = fmt.Errorf("error receiving response: %s", err)
			}
			if resp.Status != common.Status_SUCCESS {
				requestErr := fmt.Errorf("receiving response with error: %s", resp.Info)
				res.respondsErrors = append(res.respondsErrors, requestErr)
				res.err = requestErr
			} else {
				res.successRequests++
				res.failRequests--
			}
		case <-ctx.Done():
			res.err = fmt.Errorf("a time out occured during submitting request: %w", ctx.Err())
		}
	}

	wg.Wait()

	return
}

func submitRequest(conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	buff := make([]byte, 300)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	req := tx.CreateStructuredRequest(buff)
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
		RouterFilterConfig: config.NewRouterFilterConfig(1<<10, false, "arma", nil, nil, nil),
	}

	r := router.NewRouter(conf, logger)
	r.StartRouterService()

	return r
}
