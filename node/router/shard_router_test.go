package router_test

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/comm/tlsgen"
	"arma/node/router"
	"arma/testutil"

	"github.com/stretchr/testify/require"
)

type TestSetup struct {
	ca          tlsgen.CA
	shardRouter *router.ShardRouter
	stubBatcher *stubBatcher
}

func TestShardRouterConnectivityToBatcher(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	trace := make([]byte, 16)
	binary.BigEndian.PutUint16(trace, math.MaxInt16)

	responses := make(chan router.Response, 1)
	reqID, payload := createRequestAndRequestId(1)
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)

	response := <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)
	require.Equal(t, uint32(1), testSetup.stubBatcher.ReceivedMessageCount())
}

func TestShardRouterConnectivityToBatcherOnBatcherStop(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	trace := make([]byte, 16)
	binary.BigEndian.PutUint16(trace, math.MaxInt16)

	responses := make(chan router.Response, 1)
	reqID, payload := createRequestAndRequestId(1)
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)

	response := <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)

	testSetup.stubBatcher.Stop()
	time.Sleep(1 * time.Second)

	responses = make(chan router.Response, 1)
	reqID, payload = createRequestAndRequestId(1)
	testSetup.shardRouter.Forward(reqID, payload, responses, trace)
	response = <-responses
	require.NotNil(t, response.GetResponseError())
	require.Equal(t, "could not establish stream to "+testSetup.stubBatcher.GetBatcherEndpoint(), response.GetResponseError().Error())
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

func createRequestAndRequestId(shardCount uint16) ([]byte, []byte) {
	payload := make([]byte, 300)
	binary.BigEndian.PutUint32(payload, uint32(10000))
	reqID, _ := core.CRC64RequestToShard(shardCount)(payload)
	return reqID, payload
}
