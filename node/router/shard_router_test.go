package router_test

import (
	"encoding/binary"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/grpclog"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	"github.ibm.com/decentralized-trust-research/arma/node/router"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/stretchr/testify/require"
)

func init() {
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stdout, os.Stderr, os.Stderr))
}

type TestSetup struct {
	ca          tlsgen.CA
	shardRouter *router.ShardRouter
	stubBatcher *stubBatcher
}

func TestShardRouterConnectivityToBatcherByForward(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	trace := make([]byte, 16)
	binary.BigEndian.PutUint16(trace, math.MaxInt16)

	responses := make(chan router.Response, 1)
	reqID, payload := createRequestAndRequestId(1, uint32(10000))
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)

	response := <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)
	require.Equal(t, uint32(1), testSetup.stubBatcher.ReceivedMessageCount())
}

func TestShardRouterConnectivityToBatcherByForwardBestEffort(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	reqID, payload := createRequestAndRequestId(1, uint32(10000))
	err := testSetup.shardRouter.ForwardBestEffort(reqID, payload)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return testSetup.stubBatcher.ReceivedMessageCount() == uint32(1) }, 60*time.Second, 10*time.Millisecond)
}

func TestShardRouterRetryConnectToBatcherAndForwardReq(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	trace := make([]byte, 16)
	binary.BigEndian.PutUint16(trace, math.MaxInt16)

	responses := make(chan router.Response, 1)
	reqID, payload := createRequestAndRequestId(1, uint32(10000))
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)

	var response router.Response
	response = <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)

	t.Logf("Stop the batcher")
	testSetup.stubBatcher.Stop()
	time.Sleep(20 * time.Second)

	// prepare future request
	var wg sync.WaitGroup
	wg.Add(1)
	responses = make(chan router.Response, 1)
	reqID, payload = createRequestAndRequestId(1, uint32(10000))

	// wait on future response
	go func() {
		response = <-responses
		require.NotNil(t, response)
		require.Nil(t, response.GetResponseError())
		require.Equal(t, trace, response.SubmitResponse.TraceId)
		wg.Done()
	}()

	t.Logf("Restart the batcher")
	go func() {
		time.Sleep(10 * time.Second)
		testSetup.stubBatcher.Restart()
	}()

	t.Logf("Send request") // expect to have retries
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)

	wg.Wait()

	t.Logf("Send request again") // expect to reconnection
	responses = make(chan router.Response, 1)
	reqID, payload = createRequestAndRequestId(1, uint32(90000))
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)
	response = <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)
}

func TestShardRouterRetryConnectToBatcherAndForwardBestEffortReq(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	reqID1, payload1 := createRequestAndRequestId(1, uint32(10000))
	err := testSetup.shardRouter.ForwardBestEffort(reqID1, payload1)
	require.NoError(t, err)

	t.Logf("Stop the batcher")
	testSetup.stubBatcher.Stop()
	time.Sleep(20 * time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	reqID2, payload2 := createRequestAndRequestId(1, uint32(10001))
	go func() {
		// send request, expect to retries
		err := testSetup.shardRouter.ForwardBestEffort(reqID2, payload2)
		require.NoError(t, err)
	}()

	t.Logf("Restart the batcher")
	go func() {
		time.Sleep(10 * time.Second)
		testSetup.stubBatcher.Restart()
	}()

	t.Logf("Send request again") // expect to reconnection
	reqID3, payload3 := createRequestAndRequestId(1, uint32(10002))
	err = testSetup.shardRouter.ForwardBestEffort(reqID3, payload3)
	require.NoError(t, err)
}

func createTestSetup(t *testing.T, partyID types.PartyID) *TestSetup {
	// create a CA that issues a certificate for the router and the primary stub batcher
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	// create cert and key for the router
	logger := testutil.CreateLogger(t, 0)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	// create stub batcher
	batcher := NewStubBatcher(t, ca, partyID, types.ShardID(1))

	// create shard router
	shardRouter := router.NewShardRouter(logger, batcher.GetBatcherEndpoint(), [][]byte{ca.CertBytes()}, ckp.Cert, ckp.Key, 10, 20)

	// start the batcher
	batcher.Start()

	return &TestSetup{
		ca:          ca,
		shardRouter: shardRouter,
		stubBatcher: &batcher,
	}
}

func createRequestAndRequestId(shardCount uint16, content uint32) ([]byte, []byte) {
	payload := make([]byte, 300)
	binary.BigEndian.PutUint32(payload, content)
	reqID, _ := core.CRC64RequestToShard(shardCount)(payload)
	return reqID, payload
}
