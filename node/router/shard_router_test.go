/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"encoding/binary"
	"math"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/grpclog"

	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	"github.com/hyperledger/fabric-x-orderer/testutil"

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
	reqID, request := createRequestAndRequestId(1, uint32(10000))
	tr := router.CreateTrackedRequest(request, responses, reqID, trace)
	testSetup.shardRouter.Forward(tr)

	response := <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)
	require.Equal(t, uint32(1), testSetup.stubBatcher.ReceivedMessageCount())
}

func TestShardRouterReconnectToBatcherAndForwardReq(t *testing.T) {
	testSetup := createTestSetup(t, 1)
	testSetup.shardRouter.MaybeInit()

	trace := make([]byte, 16)
	binary.BigEndian.PutUint16(trace, math.MaxInt16)

	responses := make(chan router.Response, 1)
	reqID, request := createRequestAndRequestId(1, uint32(10000))
	tr := router.CreateTrackedRequest(request, responses, reqID, trace)
	testSetup.shardRouter.Forward(tr)

	var response router.Response
	response = <-responses
	require.NotNil(t, response)
	require.Equal(t, trace, response.SubmitResponse.TraceId)

	// stop the batcher
	testSetup.stubBatcher.Stop()

	// wait for the streams to become faulty
	require.Eventually(t, func() bool {
		return testSetup.shardRouter.IsConnectionsToBatcherDown()
	}, 10*time.Second, 200*time.Millisecond)

	// send a request, expect failure
	responses = make(chan router.Response, 1)
	reqID, request = createRequestAndRequestId(1, uint32(10000))
	tr = router.CreateTrackedRequest(request, responses, reqID, trace)
	testSetup.shardRouter.Forward(tr)
	response = <-responses
	require.NotNil(t, response)
	require.EqualError(t, response.GetResponseError(), "server error: connection between router and batcher "+testSetup.stubBatcher.server.Address()+" is broken, try again later")

	// restart the batcher
	testSetup.stubBatcher.Restart()

	// wait for reconnection
	require.Eventually(t, func() bool {
		return testSetup.shardRouter.IsAllStreamsOKinSR()
	}, 10*time.Second, 200*time.Millisecond)

	// send a request, expect success
	responses = make(chan router.Response, 1)
	reqID, request = createRequestAndRequestId(1, uint32(10000))
	tr = router.CreateTrackedRequest(request, responses, reqID, trace)
	testSetup.shardRouter.Forward(tr)
	response = <-responses
	require.NotNil(t, response)
	require.Nil(t, response.GetResponseError())
	require.Equal(t, trace, response.SubmitResponse.TraceId)
}

func createTestSetup(t *testing.T, partyID types.PartyID) *TestSetup {
	// create a CA that issues a certificate for the router and the primary stub batcher
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	// create cert and key for the router
	logger := testutil.CreateLogger(t, 0)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	verifier := requestfilter.NewRulesVerifier(nil)
	verifier.AddRule(requestfilter.AcceptRule{})

	// create stub batcher
	batcher := NewStubBatcher(t, ca, partyID, types.ShardID(1))

	// create shard router
	shardRouter := router.NewShardRouter(logger, batcher.GetBatcherEndpoint(), [][]byte{ca.CertBytes()}, ckp.Cert, ckp.Key, 10, 20, verifier, nil)

	// start the batcher
	batcher.Start()

	return &TestSetup{
		ca:          ca,
		shardRouter: shardRouter,
		stubBatcher: &batcher,
	}
}

func createRequestAndRequestId(shardCount uint16, content uint32) ([]byte, *protos.Request) {
	payload := make([]byte, 300)
	binary.BigEndian.PutUint32(payload, content)
	reqID, _ := router.CRC64RequestToShard(shardCount)(payload)
	return reqID, &protos.Request{Payload: payload}
}
