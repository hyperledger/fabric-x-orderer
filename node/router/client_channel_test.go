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
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newTestClientChannel returns a clientChannel of the given capacity, with all
// of the counters recordDrop touches, and a reader for any of them.
func newTestClientChannel(t *testing.T, capacity int, clientDone, routerDraining <-chan struct{}) (
	*clientChannel,
	func(metrics.Counter) float64,
) {
	t.Helper()

	logger := testutil.CreateLogger(t, 0)
	provider := monitoring.NewProvider("prometheus", logger)
	dropped := provider.NewCounter(droppedResponses)
	rejected := provider.NewCounter(rejectedTxs)

	cc := newClientChannel(capacity, clientDone, routerDraining, &RouterMetrics{
		droppedRespClientGone:       dropped.With([]string{"client_gone", "1"}...),
		droppedRespClientNotReading: dropped.With([]string{"client_not_reading", "1"}...),
		droppedRespRouterDraining:   dropped.With([]string{"router_draining", "1"}...),
		rejectedTxsWithCode400:      rejected.With([]string{"400", "1"}...),
		rejectedTxsWithCode500:      rejected.With([]string{"500", "1"}...),
	})

	read := func(c metrics.Counter) float64 {
		return monitoring.GetMetricValue(c.(prometheus.Metric), logger)
	}

	return cc, read
}

// Scenario:
//  1. Create a client channel with room for one response.
//  2. Reply once and expect the response to be queued.
//  3. Expect the queued response to be the one that was sent.
func TestClientChannelReplyQueuesResponse(t *testing.T) {
	cc, read := newTestClientChannel(t, 1, nil, nil)

	require.True(t, cc.reply(Response{reqID: []byte("req1")}))

	resp := <-cc.responses
	require.Equal(t, []byte("req1"), resp.reqID)
	require.Zero(t, read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespClientNotReading))
	require.Zero(t, read(cc.metrics.droppedRespRouterDraining))
}

// Scenario:
//  1. Create a client channel with room for one response and fill it.
//  2. Close the client's done channel, as a disconnect does.
//  3. Reply again and expect the response to be dropped.
//  4. Expect the drop to be counted as the client being gone.
func TestClientChannelReplyDropsWhenClientIsGone(t *testing.T) {
	clientDone := make(chan struct{})
	cc, read := newTestClientChannel(t, 1, clientDone, nil)

	require.True(t, cc.reply(Response{}))
	close(clientDone)

	require.False(t, cc.reply(Response{}))
	require.Equal(t, float64(1), read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespClientNotReading))
	require.Zero(t, read(cc.metrics.droppedRespRouterDraining))
}

// Scenario:
//  1. Create a client channel with room for one response and fill it.
//  2. Close the router's drain channel, as a soft stop does.
//  3. Reply again and expect the response to be dropped.
//  4. Expect the drop to be counted as the router draining.
func TestClientChannelReplyDropsWhenRouterIsDraining(t *testing.T) {
	routerDraining := make(chan struct{})
	cc, read := newTestClientChannel(t, 1, nil, routerDraining)

	require.True(t, cc.reply(Response{}))
	close(routerDraining)

	require.False(t, cc.reply(Response{}))
	require.Equal(t, float64(1), read(cc.metrics.droppedRespRouterDraining))
	require.Zero(t, read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespClientNotReading))
}

// Scenario:
//  1. Create a client channel with room for one response and fill it.
//  2. Leave both the client and the router signals open.
//  3. Reply again and expect the response to be dropped.
//  4. Expect the drop to be counted as the client not reading.
func TestClientChannelReplyDropsWhenClientIsNotReading(t *testing.T) {
	cc, read := newTestClientChannel(t, 1, make(chan struct{}), make(chan struct{}))

	require.True(t, cc.reply(Response{}))

	require.False(t, cc.reply(Response{}))
	require.Equal(t, float64(1), read(cc.metrics.droppedRespClientNotReading))
	require.Zero(t, read(cc.metrics.droppedRespClientGone))
	require.Zero(t, read(cc.metrics.droppedRespRouterDraining))
}

// Scenario:
//  1. Create a client channel with both signals open and nothing reading it.
//  2. Reply more times than the buffer can hold, from a goroutine.
//  3. Expect every reply to return rather than block.
func TestClientChannelReplyNeverBlocks(t *testing.T) {
	const capacity = 8
	cc, read := newTestClientChannel(t, capacity, make(chan struct{}), make(chan struct{}))

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
	require.Equal(t, float64(capacity*3), read(cc.metrics.droppedRespClientNotReading))
}

// Scenario:
//  1. Create a client channel with room for one response and fill it.
//  2. Reply with a server error so that the response is dropped.
//  3. Expect the request to still be counted as rejected with code 500.
func TestClientChannelDroppedErrorIsStillCountedAsRejected(t *testing.T) {
	cc, read := newTestClientChannel(t, 1, make(chan struct{}), make(chan struct{}))

	require.True(t, cc.reply(Response{}))

	require.False(t, cc.reply(Response{err: errors.New("server error: batcher is unreachable")}))
	require.Equal(t, float64(1), read(cc.metrics.rejectedTxsWithCode500))
	require.Zero(t, read(cc.metrics.rejectedTxsWithCode400))
}
