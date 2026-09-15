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

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	fabricx_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
	res := submitBroadcastRequests(testSetup.clientConn, numRequests)

	require.GreaterOrEqual(t, res.successRequests, 1, "the initial burst should be admitted")
	require.NotEmpty(t, res.respondsErrors, "requests over the rate should be rejected")
	throttled := 0
	for _, e := range res.respondsErrors {
		if regexp.MustCompile("throttled").MatchString(e.Error()) {
			throttled++
		}
	}
	require.Positive(t, throttled, "rejections should carry the throttle message")

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

	res := submitBroadcastRequests(testSetup.clientConn, 100)
	require.NoError(t, res.err)
	require.Equal(t, 100, res.successRequests)
}

// broadcastOutcome tallies the responses a Broadcast client received.
type broadcastOutcome struct {
	success   int    // responses with common.Status_SUCCESS
	throttled int    // responses with common.Status_SERVICE_UNAVAILABLE (rate limited)
	other     int    // responses with any other status
	lastInfo  string // Info of the last non-success response, for diagnostics
}

// submitBroadcast opens a Broadcast stream and sends numRequests envelopes, sleeping interval
// between consecutive sends (interval == 0 sends them as a fast burst). It reads back one response
// per request and tallies accepted vs. throttled vs. other, asserting that every throttling
// rejection carries the SERVICE_UNAVAILABLE status and the throttle message.
func submitBroadcast(t *testing.T, conn *grpc.ClientConn, numRequests int, interval time.Duration) broadcastOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl := ab.NewAtomicBroadcastClient(conn)
	stream, err := cl.Broadcast(ctx)
	require.NoError(t, err)

	go func() {
		buff := make([]byte, 300)
		for i := 0; i < numRequests; i++ {
			binary.BigEndian.PutUint32(buff, uint32(i))
			if sendErr := stream.Send(tx.CreateStructuredEnvelope(buff)); sendErr != nil {
				return
			}
			if interval > 0 {
				time.Sleep(interval)
			}
		}
	}()

	var out broadcastOutcome
	for i := 0; i < numRequests; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		switch resp.Status {
		case common.Status_SUCCESS:
			out.success++
		case common.Status_SERVICE_UNAVAILABLE:
			out.throttled++
			out.lastInfo = resp.Info
			require.Contains(t, resp.Info, "throttled", "a throttling rejection must carry the throttle message")
		default:
			out.other++
			out.lastInfo = resp.Info
		}
	}
	return out
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

	// Under the rate: 30 requests paced at 20ms (~50/s, below the 100/s limit and above the 10ms
	// token refill interval), so every request is admitted and none is throttled.
	underRate := submitBroadcast(t, clientConn, 30, 20*time.Millisecond)
	require.Equal(t, 30, underRate.success, "all under-rate requests should be accepted")
	require.Zero(t, underRate.throttled, "no requests should be throttled under the rate")
	require.Zero(t, underRate.other, "under-rate requests should not fail for any other reason")

	// Over the rate: a fast burst of 200 requests. The initial burst is admitted and the rest,
	// arriving far faster than the token refill, are rejected with SERVICE_UNAVAILABLE.
	overRate := submitBroadcast(t, clientConn, 200, 0)
	require.GreaterOrEqual(t, overRate.success, 1, "the initial burst should be admitted")
	require.Positive(t, overRate.throttled, "requests over the rate should be throttled")
	require.Zero(t, overRate.other, "the only rejections should be throttling rejections")
	require.Contains(t, overRate.lastInfo, "request throttled by router rate limiter", "throttled responses should carry the rate-limiter reason")

	// The server-side throttling counter should have been incremented.
	re := regexp.MustCompile(fmt.Sprintf(`router_requests_throttled\{party_id="%d"\} \d+`, partyId))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, testSetup.routerNode.MonitoringServiceAddress()) > 0
	}, 30*time.Second, 100*time.Millisecond)
}
