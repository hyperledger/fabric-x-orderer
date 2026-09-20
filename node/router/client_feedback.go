/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"sync/atomic"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
)

// clientFeedbackBufferSize is the number of responses buffered for a client stream.
const clientFeedbackBufferSize = 1000

// clientFeedback is the response path of a single client stream.
//
// Responses are sent from goroutines shared by every client: the per-stream
// sendRequests and readResponses goroutines, and the single config submitter
// goroutine. A send that blocked on one client would stall the others, so every
// response goes through reply, which never blocks.
type clientFeedback struct {
	responses  chan Response
	clientAddr string
	// dropped counts the responses dropped while the client was connected.
	dropped atomic.Uint64
	// nextReport is the drop count at which dropped is next logged. Zero means
	// the first drop.
	nextReport atomic.Uint64
	// clientDone is the done channel of the client stream's context, closed both
	// when the client disconnects and when the stream handler returns.
	clientDone <-chan struct{}
	// routerDraining is the router's drain signal, after which the goroutine
	// forwarding responses to this client stops reading them.
	routerDraining <-chan struct{}
	metrics        *RouterMetrics
	logger         *flogging.FabricLogger
}

func newClientFeedback(
	capacity int,
	clientAddr string,
	clientDone <-chan struct{},
	routerDraining <-chan struct{},
	metrics *RouterMetrics,
	logger *flogging.FabricLogger,
) *clientFeedback {
	return &clientFeedback{
		responses:      make(chan Response, capacity),
		clientAddr:     clientAddr,
		clientDone:     clientDone,
		routerDraining: routerDraining,
		metrics:        metrics,
		logger:         logger,
	}
}

// reply queues resp for the client without blocking, and reports whether it was
// queued. A response that does not fit is dropped and counted.
func (c *clientFeedback) reply(resp Response) bool {
	select {
	case c.responses <- resp:
		return true
	default:
	}

	c.recordDrop(resp)

	return false
}

// recordDrop counts a dropped response by what stopped reading the channel. It
// runs only once the buffer is full, so these checks stay off the steady path.
func (c *clientFeedback) recordDrop(resp Response) {
	// The request was rejected whether or not the client heard about it, and only
	// the delivery path would otherwise count it.
	c.metrics.increaseErrorCount(resp.err)

	switch {
	case isClosed(c.clientDone):
		c.metrics.droppedRespClientGone.Add(1)
	case isClosed(c.routerDraining):
		c.metrics.droppedRespRouterDraining.Add(1)
	default:
		// The client is connected but its buffer is full: it is either not reading
		// or reading slower than responses arrive, which a burst of replies also causes.
		c.metrics.droppedRespClientBehind.Add(1)
		c.reportDrop()
	}
}

// reportDrop counts a drop for a connected client, and logs the count at the 1st,
// 10th and 100th drop, growing tenfold up to ten times the channel size and then by
// that much, so a client that stays behind keeps being reported at a bounded rate.
func (c *clientFeedback) reportDrop() {
	n := c.dropped.Add(1)
	next := c.nextReport.Load()
	if n < next || !c.nextReport.CompareAndSwap(next, c.nextDropReport(next)) {
		return
	}

	c.logger.Warnf("dropped %d responses so far for client %s, which is connected but behind on reading them", n, c.clientAddr)
}

// nextDropReport returns the drop count to log at after the one at reported. The
// channel size counts as at least 10, so a small channel is not logged on every drop.
func (c *clientFeedback) nextDropReport(reported uint64) uint64 {
	ceiling := 10 * max(uint64(cap(c.responses)), 10)

	switch {
	case reported == 0:
		return 10
	case reported < ceiling:
		return reported * 10
	default:
		return reported + ceiling
	}
}

// isClosed reports whether ch is closed. A nil channel is reported as open.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
