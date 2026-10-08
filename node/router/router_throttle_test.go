/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	fabricx_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/stretchr/testify/require"
)

// withThrottling returns an option that sets the router's throttling config.
func withThrottling(policy string, rate, burst int) func(*config.RouterNodeConfig) {
	return func(c *config.RouterNodeConfig) {
		c.Throttling = config.RouterThrottlingConfig{Policy: policy, Rate: rate, Burst: burst}
	}
}

// TestBroadcastThrottled verifies that, under the global throttling policy with a
// tight rate, a rapid burst of Broadcast requests yields some SERVICE_UNAVAILABLE
// rejections carrying the throttle message while the initial burst succeeds, and
// that the router_requests_throttled metric is incremented.
func TestBroadcastThrottled(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false, withThrottling(fabricx_config.ThrottlingPolicyGlobal, 1, 1))
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	defer testSetup.Close()

	const numRequests = 50
	res := submitBroadcast(t, testSetup.clientConn, numRequests, 0)

	require.GreaterOrEqual(t, res.success, 1, "the initial burst should be admitted")
	require.Positive(t, res.throttled, "requests over the rate should be throttled")
	require.Zero(t, res.other, "the only rejections should be throttling rejections")

	URL := testSetup.router.MonitoringServiceAddress()
	re := regexp.MustCompile(fmt.Sprintf(`router_requests_throttled\{party_id="%d"\} \d+`, types.PartyID(1)))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, URL) > 0
	}, 30*time.Second, 100*time.Millisecond)
}

// TestSubmitThrottled verifies that unary Submit calls over the global rate limit
// are rejected with the throttle error, and that each throttled response carries
// its ReqID for client correlation.
func TestSubmitThrottled(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false, withThrottling(fabricx_config.ThrottlingPolicyGlobal, 1, 1))
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	defer testSetup.Close()

	cl := protos.NewRequestTransmitClient(testSetup.clientConn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	throttleRe := regexp.MustCompile("throttled")
	throttled := 0
	buff := make([]byte, 300)
	for i := 0; i < 50; i++ {
		binary.BigEndian.PutUint32(buff, uint32(i))
		resp, submitErr := cl.Submit(ctx, tx.CreateStructuredRequest(buff))
		require.NoError(t, submitErr)
		if throttleRe.MatchString(resp.Error) {
			throttled++
			require.NotEmpty(t, resp.ReqID, "a throttled Submit response must carry its ReqID for client correlation")
		}
	}
	require.Positive(t, throttled, "unary Submit over the rate should be throttled")
}

// TestSubmitStreamThrottled verifies that SubmitStream requests over the global
// rate limit are rejected with the throttle error, and that each throttled
// response carries its ReqID so a client can correlate it on the stream.
func TestSubmitStreamThrottled(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false, withThrottling(fabricx_config.ThrottlingPolicyGlobal, 1, 1))
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	defer testSetup.Close()

	cl := protos.NewRequestTransmitClient(testSetup.clientConn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := cl.SubmitStream(ctx)
	require.NoError(t, err)

	const numRequests = 50
	go func() {
		buff := make([]byte, 300)
		for j := 0; j < numRequests; j++ {
			binary.BigEndian.PutUint32(buff, uint32(j))
			if sendErr := stream.Send(tx.CreateStructuredRequest(buff)); sendErr != nil {
				return
			}
		}
	}()

	throttleRe := regexp.MustCompile("throttled")
	throttled := 0
	for j := 0; j < numRequests; j++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		if throttleRe.MatchString(resp.Error) {
			throttled++
			require.NotEmpty(t, resp.ReqID, "a throttled SubmitStream response must carry its ReqID for client correlation")
		}
	}
	require.Positive(t, throttled, "requests over the rate should be throttled")
}

// TestThrottlingDisabledPassThrough verifies that with the disabled policy, all
// requests pass through and none are throttled.
func TestThrottlingDisabledPassThrough(t *testing.T) {
	testSetup := createRouterTestSetup(t, types.PartyID(1), 1, true, false, withThrottling(fabricx_config.ThrottlingPolicyDisabled, 0, 0))
	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)
	defer testSetup.Close()

	res := submitBroadcast(t, testSetup.clientConn, 100, 0)
	require.Equal(t, 100, res.success)
	require.Zero(t, res.throttled)
	require.Zero(t, res.other)
}

// withRouterThrottling returns a local-config mutator that turns on the router's global request
// throttling. It is applied to the generated router local config before it is loaded, so the real
// router boots with the throttler installed.
func withRouterThrottling(policy string, rate, burst int) func(*fabricx_config.NodeLocalConfig) {
	return func(nlc *fabricx_config.NodeLocalConfig) {
		nlc.RouterParams.Throttling = &fabricx_config.ThrottlingParams{Policy: policy, Rate: rate, Burst: burst}
	}
}

// Scenario:
//  1. Start a real router (booted from generated config) with a stub batcher and stub consenter,
//     with global request throttling enabled at Rate=100/s, Burst=10.
//  2. Wait for the router to be running and connected to the batcher.
//  3. Under the rate: submit requests paced below the limit and verify they are all accepted.
//  4. Over the rate: submit a fast burst and verify some requests are rejected with
//     SERVICE_UNAVAILABLE while the initial burst is accepted, and the throttling metric fires.
func TestRouterThrottling(t *testing.T) {
	partyId := types.PartyID(1)

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId, withRouterThrottling(fabricx_config.ThrottlingPolicyGlobal, 100, 10))
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// make sure the router is connected to the batcher before submitting requests.
	require.Eventually(t, func() bool {
		return testSetup.routerNode.IsAllStreamsOK()
	}, 20*time.Second, 100*time.Millisecond)

	clientConn, err := testSetup.CreateRouterClient()
	require.NoError(t, err)
	require.NotNil(t, clientConn)
	defer clientConn.Close()

	// Under the rate: 20 requests paced at 50ms (~20/s, 5x below the 100/s limit). Client pacing does
	// not pace the limiter: if the server's Recv loop stalls and then drains queued envelopes at once,
	// they hit the bucket together. At 50ms a stall must exceed ~500ms to queue more than Burst=10
	// envelopes, which leaves ample headroom on a slow -race runner.
	underRate := submitBroadcast(t, clientConn, 20, 50*time.Millisecond)
	require.Equal(t, 20, underRate.success, "all under-rate requests should be accepted")
	require.Zero(t, underRate.throttled, "no requests should be throttled under the rate")
	require.Zero(t, underRate.other, "under-rate requests should not fail for any other reason")

	// Over the rate: a fast burst of 200 requests. The initial burst is admitted and the rest,
	// arriving far faster than the token refill, are rejected with SERVICE_UNAVAILABLE.
	overRate := submitBroadcast(t, clientConn, 200, 0)
	require.GreaterOrEqual(t, overRate.success, 1, "the initial burst should be admitted")
	require.Positive(t, overRate.throttled, "requests over the rate should be throttled")
	require.Zero(t, overRate.other, "the only rejections should be throttling rejections")
	require.NotEmpty(t, overRate.infos)
	for _, info := range overRate.infos {
		require.Contains(t, info, "request throttled by router rate limiter", "throttled responses should carry the rate-limiter reason")
	}

	// The server-side throttling counter should have been incremented.
	re := regexp.MustCompile(fmt.Sprintf(`router_requests_throttled\{party_id="%d"\} \d+`, partyId))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, testSetup.routerNode.MonitoringServiceAddress()) > 0
	}, 30*time.Second, 100*time.Millisecond)
}
