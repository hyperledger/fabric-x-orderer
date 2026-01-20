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
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	fabricx_config "github.com/hyperledger/fabric-x-orderer/config"
	ordererRulesMocks "github.com/hyperledger/fabric-x-orderer/config/verify/mocks"
	"github.com/hyperledger/fabric-x-orderer/internal/pkg/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func init() {
	// set the gRPC logger to a logger that discards the log output.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
}

type routerTestSetup struct {
	ca         tlsgen.CA
	batchers   []*stub.StubBatcher
	consenter  *stub.StubConsenter
	clientConn *grpc.ClientConn
	router     *router.Router
	config     *config.RouterNodeConfig
}

func (r *routerTestSetup) Close() {
	if r.router != nil {
		r.router.Stop()
	}

	if r.clientConn != nil {
		r.clientConn.Close()
	}

	for _, batcher := range r.batchers {
		batcher.Server().Stop()
	}

	if r.consenter != nil {
		r.consenter.Stop()
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
	var batchers []*stub.StubBatcher
	for i := 0; i < numOfShards; i++ {
		batcher := stub.NewStubBatcher(t, ca, partyID, types.ShardID(i+1))
		batchers = append(batchers, &batcher)
	}

	// start the batchers
	for _, batcher := range batchers {
		batcher.Start()
	}

	// create and start stub-consenter
	stubConsenter := stub.NewStubConsenter(t, ca, partyID)
	stubConsenter.Start()

	// create and start router
	router, conf := createAndStartRouter(t, partyID, ca, batchers, &stubConsenter, useTLS, clientAuthRequired)

	return &routerTestSetup{
		ca:        ca,
		batchers:  batchers,
		consenter: &stubConsenter,
		router:    router,
		config:    conf,
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

// TestSubmitToStubBatchersGetMetrics verifies that submitting a series of stream and broadcast requests
// to the router correctly updates the incoming transaction metrics. It sets up a test router with TLS,
// submits 100 stream and 100 broadcast requests, and then asserts that the monitoring service reports
// the expected total number of incoming transactions within a specified timeout.
func TestSubmitToStubBatchersGetMetrics(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, errors.Wrap(err, "during TLS client connection setup"))
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	URL := testSetup.router.MonitoringServiceAddress()
	require.NotEmpty(t, URL, "monitoring service address should not be empty")

	res := submitStreamRequests(testSetup.clientConn, 1000)
	require.NoError(t, res.err)
	time.Sleep(5 * time.Second) // wait a bit before sending the next batch to allow metrics to be logged

	res = submitBroadcastRequests(testSetup.clientConn, 1000)
	require.NoError(t, res.err)

	pattern := fmt.Sprintf(`router_requests_completed\{party_id="%d"\} \d+`, types.PartyID(1))
	re := regexp.MustCompile(pattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, URL) == 2000
	}, 30*time.Second, 100*time.Millisecond)
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
	require.EqualError(t, err, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].Server().Address()+" is broken, try again later")

	// send same request again.
	err = submitRequest(testSetup.clientConn)
	require.EqualError(t, err, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].Server().Address()+" is broken, try again later")
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
		require.EqualError(t, e, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].Server().Address()+" is broken, try again later")
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
		require.EqualError(t, e, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].Server().Address()+" is broken, try again later")
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
		require.EqualError(t, e, "receiving response with error: server error: connection between router and batcher "+testSetup.batchers[0].Server().Address()+" is broken, try again later")
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

// Scenario:
// 1) Start a client, router and stub consenter
// 2) Send valid config request, expect response from stub consenter
func TestConfigSubmitter(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	err = submitConfigRequest(t, testSetup.clientConn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testSetup.consenter.ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestConfigSubmitterConsenterDown(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	// submit one request, and wait for the response
	err = submitConfigRequest(t, testSetup.clientConn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testSetup.consenter.ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	// stop the consenter and wait until it is down
	testSetup.consenter.Stop()
	time.Sleep(250 * time.Millisecond)

	// wait and restart the consenter
	go func() {
		time.Sleep(250 * time.Millisecond)
		testSetup.consenter.Restart()
	}()

	// meanwhile, forward another request
	err = submitConfigRequest(t, testSetup.clientConn)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return testSetup.consenter.ReceivedMessageCount() == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)
}

// TestRouterSendConfigUpdateToConsenterStub tests the end-to-end flow of sending a configuration
// update transaction through the router to a consenter stub.
func TestRouterSendConfigUpdateToConsenterStub(t *testing.T) {
	// 1. Compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// 2. Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	numOfShards := 1
	submittingParty := 1

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")
	require.NoError(t, err)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configStoreDir := t.TempDir()
	defer os.RemoveAll(configStoreDir)

	// 5. Launch the batcher node stub
	batcher := stub.NewStubBatcherFromConfig(t, configStoreDir, filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml"), netInfo["Party13batcher1"].Listener)
	batcher.Start()
	defer batcher.Stop()

	// 6. Launch the router node
	readyChan := make(chan struct{}, 1)

	routerNodeInfo := netInfo["Party14router"]
	routerNodeConfigPath := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
	localConfig, _, err := fabricx_config.LoadLocalConfig(routerNodeConfigPath)
	require.NoError(t, err)

	localConfig.NodeLocalConfig.FileStore.Path = configStoreDir
	localConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	utils.WriteToYAML(localConfig.NodeLocalConfig, routerNodeConfigPath)

	testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, map[string]*testutil.ArmaNodeInfo{"Party14router": routerNodeInfo})
	testutil.WaitReady(t, readyChan, 1, 10)

	// 7. Launch the consenter node stub
	consenterStub := stub.NewStubConsenterFromConfig(t, configStoreDir, filepath.Join(dir, "config", "party1", "local_config_consenter.yaml"), netInfo["Party11consensus"].Listener)
	consenterStub.Start()
	defer consenterStub.Stop()

	// 8. Create a broadcast client
	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// 9. Prepare a Config TX, i.e. an envelope signed by an admin of org1
	// the envelope.Payload contains marshaled bytes of configUpdateEnvelope, which is an envelope with Header.Type = HeaderType_CONFIG_UPDATE, signed by majority of admins
	// Create the config transaction
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)
	defer cleanUp()

	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, cfgutil.NewBatchSizeConfig(cfgutil.BatchSizeConfigName.MaxMessageCount, 500))
	require.NotEmpty(t, configUpdatePbData)

	env := cfgutil.CreateConfigTX(t, dir, numOfParties, submittingParty, configUpdatePbData)
	require.NotNil(t, env)

	// 10. Send the config tx
	err = broadcastClient.SendTx(env)
	require.ErrorContains(t, err, "INTERNAL_SERVER_ERROR, Info: dummy submit config")

	require.Eventually(t, func() bool {
		return consenterStub.ReceivedMessageCount() == 1
	}, 10*time.Second, 100*time.Millisecond)
}

func submitConfigRequest(t *testing.T, conn *grpc.ClientConn) error {
	cl := ab.NewAtomicBroadcastClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cl.Broadcast(ctx)
	require.NoError(t, err)

	configEnvelope := protoutil.MarshalOrPanic(&common.ConfigUpdateEnvelope{
		ConfigUpdate: nil,
		Signatures:   nil,
	})

	env := tx.CreateStructuredConfigUpdateEnvelope(configEnvelope)
	err = stream.Send(env)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, common.Status_INTERNAL_SERVER_ERROR, resp.Status)
	require.Equal(t, "dummy submit config", resp.Info)

	return nil
}

// Scenario:
// 1) Start a client, router and stub consenter
// 2) Send and pull a decision with a config block
// 3) Verify that the config block is stored in the router config store
// 4) Verify that the router performed a (soft) stop.
func TestConfigPullFromConsensus(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	sc := testSetup.consenter
	defer testSetup.Close()

	initialConfigStoreSize := testSetup.router.GetConfigStoreSize()
	require.Equal(t, 1, initialConfigStoreSize, "expected genesis block in config store")

	// create a decision with a config block
	configBlock := tx.CreateConfigBlock(999, []byte("config block data"))
	require.True(t, protoutil.IsConfigBlock(configBlock))
	acb := make([]*common.Block, 2)

	acb[len(acb)-1] = configBlock
	err = sc.DeliverDecisionFromHeader(&state.Header{Num: 1, DecisionNumOfLastConfigBlock: 1, AvailableCommonBlocks: acb})
	require.NoError(t, err)

	// check if config block is stored in router's config store
	require.Eventually(t, func() bool {
		return testSetup.router.GetConfigStoreSize() == initialConfigStoreSize+1
	}, 10*time.Second, 10*time.Millisecond)

	// verify that the router performed a (soft) stop, by checking that it is disconnected from the batcher
	require.Eventually(t, func() bool {
		return testSetup.isDisconnectedFromBatcher()
	}, 10*time.Second, 200*time.Millisecond)
}

// Scenario:
// 1) Start a client, router and stub consenter
// 2) Pull a decision with no config block
// 3) Verify that no new config block is stored in the router config store
func TestConfigPullNoConfigBlocks(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	sc := testSetup.consenter
	defer testSetup.Close()

	initialConfigStoreSize := testSetup.router.GetConfigStoreSize()
	require.Equal(t, 1, initialConfigStoreSize, "expected genesis block in config store")

	// create a decision, with no config block
	configBlock := tx.CreateConfigBlock(1000, []byte("config block data"))
	acb := make([]*common.Block, 6)
	acb[len(acb)-1] = configBlock
	err = sc.DeliverDecisionFromHeader(&state.Header{Num: 2, DecisionNumOfLastConfigBlock: 1, AvailableCommonBlocks: acb})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, initialConfigStoreSize, testSetup.router.GetConfigStoreSize(), "no new config block should be stored")
}

// Scenario:
// 1) Start a client, router and stub batcher
// 2) Send requests using submitStream, expect success
// 3) set the stub batcher to not answer the router requests
// 4) in parallel, send requests using submitStream and perform a soft stop on the router
// 5) expect the requests to fail with relevant error
// 6) expect the soft stop to complete
// 7) try to connect to the router, expect to fail with relevant error
func TestRouterSoftStop(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false)
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	defer testSetup.Close()

	// send requests to the router, and expect to succeed
	numOfRequests := 5
	res := submitStreamRequests(testSetup.clientConn, numOfRequests)
	require.NoError(t, res.err)
	require.Equal(t, numOfRequests, res.successRequests)

	// tell the batcher to drop requests
	testSetup.batchers[0].SetDropRequests(true)

	// next, we send additional requests, and simultaniously perform a soft stop.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res = submitStreamRequests(testSetup.clientConn, numOfRequests)
		require.ErrorContains(t, res.err, "router is stopping, cannot process request")
		require.Equal(t, 0, res.successRequests)
	}()

	// wait until all requests are sent, and perform a soft stop
	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(2*numOfRequests)
	}, 10*time.Second, 10*time.Millisecond)

	err = testSetup.router.SoftStop()

	wg.Wait()

	// verify that the router has stopped
	require.NoError(t, err, "soft stop should complete successfully")

	// try to connect to the router, expect error
	err = createServerTLSClientConnection(testSetup, testSetup.ca)
	require.ErrorContains(t, err, "failed to create new connection")
	require.Nil(t, testSetup.clientConn)
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
		return res
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

	return res
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
		return res
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

	return res
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

func createAndStartRouter(t *testing.T, partyID types.PartyID, ca tlsgen.CA, batchers []*stub.StubBatcher, consenter *stub.StubConsenter, useTLS bool, clientAuthRequired bool) (*router.Router, *config.RouterNodeConfig) {
	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	logger := testutil.CreateLogger(t, 0)

	// create router config
	var shards []config.ShardInfo
	for j := 0; j < len(batchers); j++ {
		shards = append(shards, config.ShardInfo{ShardId: types.ShardID(j + 1), Batchers: []config.BatcherInfo{{PartyID: partyID, Endpoint: batchers[j].Server().Address(), TLSCACerts: []config.RawBytes{ca.CertBytes()}}}})
	}

	clientRootCAs := node.TLSCAcertsFromShards(shards)

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	configEnvelope := &common.ConfigEnvelope{
		Config:     nil,
		LastUpdate: nil,
	}
	configtxValidator.ProposeConfigUpdateReturns(configEnvelope, nil)
	bundle.ConfigtxValidatorReturns(configtxValidator)

	policy := &policyMocks.FakePolicyEvaluator{}
	policy.EvaluateSignedDataReturns(nil)
	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	bundle.PolicyManagerReturns(policyManager)

	fakeSigner := &mocks.SignerSerializer{}

	stubConsenterInfo := config.ConsenterInfo{PartyID: partyID, Endpoint: consenter.GetConsenterEndpoint(), TLSCACerts: []config.RawBytes{ca.CertBytes()}}

	configStorePath := t.TempDir()
	cs, err := configstore.NewStore(configStorePath)
	require.NoError(t, err)
	// add dummy genesis block
	block := tx.CreateConfigBlock(0, []byte("genesis block data"))
	require.NoError(t, cs.Add(block))

	conf := &config.RouterNodeConfig{
		PartyID:                             partyID,
		TLSCertificateFile:                  ckp.Cert,
		UseTLS:                              useTLS,
		ClientRootCAs:                       clientRootCAs,
		TLSPrivateKeyFile:                   ckp.Key,
		ListenAddress:                       "127.0.0.1:0",
		ConfigStorePath:                     configStorePath,
		ClientAuthRequired:                  clientAuthRequired,
		Shards:                              shards,
		Consenter:                           stubConsenterInfo,
		RequestMaxBytes:                     1 << 10,
		ClientSignatureVerificationRequired: false,
		Bundle:                              bundle,
		MonitoringListenAddress:             "127.0.0.1:0",
		MetricsLogInterval:                  1 * time.Second,
	}

	configUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	req := &protos.Request{}
	configUpdateProposer.ProposeConfigUpdateReturns(req, nil)

	configRulesVerifier := &ordererRulesMocks.FakeOrdererRules{}
	configRulesVerifier.ValidateNewConfigReturns(nil)

	r := router.NewRouter(conf, logger, fakeSigner, configUpdateProposer, configRulesVerifier)
	r.StartRouterService()

	return r, conf
}
