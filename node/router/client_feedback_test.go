/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"errors"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newTestClientFeedback returns a clientFeedback of the given capacity, with fresh
// disabled-provider metrics, and a reader for any of its counters.
func newTestClientFeedback(t *testing.T, capacity int, clientDone, routerDraining <-chan struct{}) (
	*clientFeedback,
	func(metrics.Counter) float64,
) {
	t.Helper()

	logger := testutil.CreateLogger(t, 0)
	routerMetrics := NewRouterMetrics(&config.RouterNodeConfig{Metrics: &operations.Metrics{Provider: "disabled"}}, logger)
	cc := newClientFeedback(capacity, "127.0.0.1:12345", clientDone, routerDraining, routerMetrics, logger)

	read := func(c metrics.Counter) float64 {
		return monitoring.GetMetricValue(c.(prometheus.Metric), logger)
	}

	return cc, read
}

// Scenario:
//  1. Create a client feedback with room for one response.
//  2. Reply once and expect the response to be queued.
//  3. Expect the queued response to be the one that was sent.
func TestClientFeedbackReplyQueuesResponse(t *testing.T) {
	cc, read := newTestClientFeedback(t, 1, nil, nil)

	require.True(t, cc.reply(Response{reqID: []byte("req1")}))

	resp := <-cc.responses
	require.Equal(t, []byte("req1"), resp.reqID)
	require.Zero(t, read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespClientBehind))
	require.Zero(t, read(cc.metrics.droppedRespRouterDraining))
}

// Scenario:
//  1. Create a client feedback with room for one response and fill it.
//  2. Close the client's done channel, as a disconnect does.
//  3. Reply again and expect the response to be dropped.
//  4. Expect the drop to be counted as the client being gone.
func TestClientFeedbackReplyDropsWhenClientIsGone(t *testing.T) {
	clientDone := make(chan struct{})
	cc, read := newTestClientFeedback(t, 1, clientDone, nil)

	require.True(t, cc.reply(Response{}))
	close(clientDone)

	require.False(t, cc.reply(Response{}))
	require.Equal(t, float64(1), read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespClientBehind))
	require.Zero(t, read(cc.metrics.droppedRespRouterDraining))
}

// Scenario:
//  1. Create a client feedback with room for one response and fill it.
//  2. Close the router's drain channel, as a soft stop does.
//  3. Reply again and expect the response to be dropped.
//  4. Expect the drop to be counted as the router draining.
func TestClientFeedbackReplyDropsWhenRouterIsDraining(t *testing.T) {
	routerDraining := make(chan struct{})
	cc, read := newTestClientFeedback(t, 1, nil, routerDraining)

	require.True(t, cc.reply(Response{}))
	close(routerDraining)

	require.False(t, cc.reply(Response{}))
	require.Equal(t, float64(1), read(cc.metrics.droppedRespRouterDraining))
	require.Zero(t, read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespClientBehind))
}

// Scenario:
//  1. Create a client feedback with room for one response and fill it.
//  2. Leave both the client and the router signals open.
//  3. Reply again and expect the response to be dropped.
//  4. Expect the drop to be counted as the client being behind.
func TestClientFeedbackReplyDropsWhenClientIsBehind(t *testing.T) {
	cc, read := newTestClientFeedback(t, 1, make(chan struct{}), make(chan struct{}))

	require.True(t, cc.reply(Response{}))

	require.False(t, cc.reply(Response{}))
	require.Equal(t, float64(1), read(cc.metrics.droppedRespClientBehind))
	require.Zero(t, read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespRouterDraining))
}

// Scenario:
//  1. Create a client feedback with both signals open and nothing reading it.
//  2. Reply more times than the buffer can hold, from a goroutine.
//  3. Expect every reply to return rather than block.
func TestClientFeedbackReplyNeverBlocks(t *testing.T) {
	const capacity = 8
	cc, read := newTestClientFeedback(t, capacity, make(chan struct{}), make(chan struct{}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < capacity*4; i++ {
			cc.reply(Response{})
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "reply blocked on a full channel that no one is reading")
	}

	require.Len(t, cc.responses, capacity)
	require.Equal(t, float64(capacity*3), read(cc.metrics.droppedRespClientBehind))
}

// Scenario:
//  1. Create a client feedback with room for one response and fill it.
//  2. Reply with a server error so that the response is dropped.
//  3. Expect the request to still be counted as rejected with code 500.
func TestClientFeedbackDroppedErrorIsStillCountedAsRejected(t *testing.T) {
	cc, read := newTestClientFeedback(t, 1, make(chan struct{}), make(chan struct{}))

	require.True(t, cc.reply(Response{}))

	require.False(t, cc.reply(Response{err: serverError(errors.New("server error: batcher is unreachable"))}))
	require.Equal(t, float64(1), read(cc.metrics.rejectedTxsWithCode500))
	require.Zero(t, read(cc.metrics.rejectedTxsWithCode400))
}

// Scenario:
//  1. Create a client feedback with room for one response and fill it.
//  2. Drop two responses while the client is connected.
//  3. Close the client's done channel and drop one more.
//  4. Expect only the two drops while connected to be counted for the client.
func TestClientFeedbackCountsDropsOnlyWhileTheClientIsConnected(t *testing.T) {
	clientDone := make(chan struct{})
	cc, _ := newTestClientFeedback(t, 1, clientDone, nil)

	require.True(t, cc.reply(Response{}))
	require.False(t, cc.reply(Response{}))
	require.False(t, cc.reply(Response{}))
	close(clientDone)
	require.False(t, cc.reply(Response{}))

	require.Equal(t, uint64(2), cc.dropped.Load())
}

// Scenario:
//  1. Create a client feedback with room for one response and fill it.
//  2. Drop responses one at a time while the client is connected.
//  3. Expect the next report to move to 10 after the 1st drop, to 100 after the
//     10th, and to 200 after the 100th, and not before.
func TestClientFeedbackAdvancesTheNextReportAtEachStep(t *testing.T) {
	cc, _ := newTestClientFeedback(t, 1, make(chan struct{}), make(chan struct{}))
	require.True(t, cc.reply(Response{}))

	expected := map[uint64]uint64{1: 10, 9: 10, 10: 100, 99: 100, 100: 200}
	for n := uint64(1); n <= 100; n++ {
		require.False(t, cc.reply(Response{}))
		if want, ok := expected[n]; ok {
			require.Equal(t, want, cc.nextReport.Load(), "after drop %d", n)
		}
	}
}

// Scenario:
//  1. Create a client feedback with the stream buffer size.
//  2. Walk the report steps from the first drop.
//  3. Expect them to grow tenfold up to ten times the buffer size, then by that much.
func TestClientFeedbackReportStepsForAStreamBuffer(t *testing.T) {
	cc, _ := newTestClientFeedback(t, clientFeedbackBufferSize, nil, nil)

	var steps []uint64
	for next := uint64(0); len(steps) < 7; {
		next = cc.nextDropReport(next)
		steps = append(steps, next)
	}

	require.Equal(t, []uint64{10, 100, 1000, 10000, 20000, 30000, 40000}, steps)
}

// Scenario:
//  1. Create a client feedback with room for one response.
//  2. Walk the report steps from the first drop.
//  3. Expect the buffer to count as size 10, so the steps settle at every 100.
func TestClientFeedbackReportStepsForASmallBuffer(t *testing.T) {
	cc, _ := newTestClientFeedback(t, 1, nil, nil)

	var steps []uint64
	for next := uint64(0); len(steps) < 5; {
		next = cc.nextDropReport(next)
		steps = append(steps, next)
	}

	require.Equal(t, []uint64{10, 100, 200, 300, 400}, steps)
}
