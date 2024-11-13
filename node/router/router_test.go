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

	"arma/common/types"
	"arma/node/comm"
	"arma/node/comm/tlsgen"
	"arma/node/config"
	protos "arma/node/protos/comm"
	"arma/node/router"
	"arma/testutil"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func init() {
	// set the gRPC logger to a logger that discards the log output.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
}

type routerTestSetup struct {
	ca         tlsgen.CA
	router     *comm.GRPCServer
	batchers   []*stubBatcher
	clientConn *grpc.ClientConn
}

func createRouterTestSetup(t *testing.T, partyID types.PartyID, numOfShards int) *routerTestSetup {
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
	routerServer := createAndStartRouter(t, partyID, ca, batchers)

	// create a client to the router
	conn := createClientConnToRouter(t, ca, routerServer)

	return &routerTestSetup{
		ca:         ca,
		router:     routerServer,
		batchers:   batchers,
		clientConn: conn,
	}
}

// Scenario:
// 1. start a client, router and stub batcher
// 2. send 10 requests by client to router
// 3. check that the batcher received the expected number of requests
func TestStubBatcherReceivesClientRouterRequests(t *testing.T) {
	routerTestSetup := createRouterTestSetup(t, types.PartyID(1), 1)

	err := submitStreamRequests(routerTestSetup.clientConn, 10)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return routerTestSetup.batchers[0].ReceivedMessageCount() == uint32(10)
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a client, router and stub batcher
// 2. send a request by client to router
// 3. check that the batcher received one request
func TestStubBatcherReceivesClientRouterSingleRequest(t *testing.T) {
	routerTestSetup := createRouterTestSetup(t, types.PartyID(1), 1)

	err := submitRequest(routerTestSetup.clientConn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return routerTestSetup.batchers[0].ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestClientRouterFailsToSendRequestOnBatcherServerStop(t *testing.T) {
	t.Skip()
	// TODO: check if the reason for error is the connectivity to batcher
	routerTestSetup := createRouterTestSetup(t, types.PartyID(1), 1)

	// send request, should succeed
	err := submitRequest(routerTestSetup.clientConn)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return routerTestSetup.batchers[0].ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	// stop the batcher and send request, expect to error
	routerTestSetup.batchers[0].Stop()
	err = submitRequest(routerTestSetup.clientConn)
	require.NotNil(t, err)
	require.EqualError(t, err, "receiving response with error: could not establish stream to "+routerTestSetup.batchers[0].server.Address())
}

// Scenario:
// 1. start a client, router and 2 stub batchers (2 shards)
// 2. send a request by client to router
// 3. check that a batcher received one request
func TestClientRouterSubmitSingleRequestAgainstMultipleBatchers(t *testing.T) {
	routerTestSetup := createRouterTestSetup(t, types.PartyID(1), 2)

	err := submitRequest(routerTestSetup.clientConn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return (routerTestSetup.batchers[0].ReceivedMessageCount() == uint32(1) && routerTestSetup.batchers[1].ReceivedMessageCount() == uint32(0)) || (routerTestSetup.batchers[0].ReceivedMessageCount() == uint32(0) && routerTestSetup.batchers[1].ReceivedMessageCount() == uint32(1))
	}, 10*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a client, router and 2 stub batchers (2 shards)
// 2. send 10 requests by client to router
// 3. check that the batchers received the expected number of requests
func TestClientRouterSubmitStreamRequestsAgainstMultipleBatchers(t *testing.T) {
	numOfShards := 2
	routerTestSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards)

	err := submitStreamRequests(routerTestSetup.clientConn, 10)
	require.NoError(t, err)

	recvCond := func() uint32 {
		receivedTxCount := uint32(0)
		for i := 0; i < numOfShards; i++ {
			receivedTxCount += routerTestSetup.batchers[i].ReceivedMessageCount()
		}
		return receivedTxCount
	}

	require.Eventually(t, func() bool {
		return recvCond() == uint32(10)
	}, 60*time.Second, 10*time.Millisecond)
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

func createAndStartRouter(t *testing.T, partyID types.PartyID, ca tlsgen.CA, batchers []*stubBatcher) *comm.GRPCServer {
	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: ckp.Cert,
			Key:         ckp.Key,
		},
	})
	require.NoError(t, err)

	logger := testutil.CreateLogger(t, 0)

	// create router config
	var shards []config.ShardInfo
	for j := 0; j < len(batchers); j++ {
		shards = append(shards, config.ShardInfo{ShardId: types.ShardID(j + 1), Batchers: []config.BatcherInfo{{PartyID: 1, Endpoint: batchers[j].server.Address(), TLSCACerts: []config.RawBytes{ca.CertBytes()}}}})
	}

	config := config.RouterNodeConfig{
		PartyID:            partyID,
		TLSCertificateFile: ckp.Cert,
		TLSPrivateKeyFile:  ckp.Key,
		ListenAddress:      srv.Address(),
		Shards:             shards,
	}
	router := router.NewRouter(config, logger)

	// start a router
	protos.RegisterRequestTransmitServer(srv.Server(), router)
	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
	}()

	return srv
}

func createClientConnToRouter(t *testing.T, ca tlsgen.CA, routerServer *comm.GRPCServer) *grpc.ClientConn {
	cc := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:        true,
			ServerRootCAs: [][]byte{ca.CertBytes()},
		},
		DialTimeout:  time.Second,
		AsyncConnect: false,
	}

	conn, err := cc.Dial(routerServer.Address())
	require.NoError(t, err)
	return conn
}
