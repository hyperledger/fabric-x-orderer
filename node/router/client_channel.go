/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

// clientFeedbackBuffer is the number of responses buffered for a client stream.
const clientFeedbackBuffer = 1000

// clientChannel is the response path of a single client stream.
//
// Responses are sent from goroutines shared by every client: the per-stream
// sendRequests and readResponses goroutines, and the single config submitter
// goroutine. A send that blocked on one client would stall the others, so every
// response goes through reply, which never blocks.
type clientChannel struct {
	responses chan Response
	// clientDone is the done channel of the client stream's context, closed both
	// when the client disconnects and when the stream handler returns.
	clientDone <-chan struct{}
	// routerDraining is the router's drain signal, after which the goroutine
	// forwarding responses to this client stops reading them.
	routerDraining <-chan struct{}
	metrics        *RouterMetrics
}

func newClientChannel(
	capacity int,
	clientDone <-chan struct{},
	routerDraining <-chan struct{},
	metrics *RouterMetrics,
) *clientChannel {
	return &clientChannel{
		responses:      make(chan Response, capacity),
		clientDone:     clientDone,
		routerDraining: routerDraining,
		metrics:        metrics,
	}
}

// reply queues resp for the client without blocking, and reports whether it was
// queued. A response that does not fit is dropped and counted.
func (c *clientChannel) reply(resp Response) bool {
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
func (c *clientChannel) recordDrop(resp Response) {
	// The request was rejected whether or not the client heard about it, and only
	// the delivery path would otherwise count it.
	c.metrics.increaseErrorCount(resp.err)

	switch {
	case isClosed(c.clientDone):
		c.metrics.droppedRespClientGone.Add(1)
	case isClosed(c.routerDraining):
		c.metrics.droppedRespRouterDraining.Add(1)
	default:
		// The client is connected and has stopped reading, so it is losing
		// acknowledgements it is waiting for. The only case that is not expected.
		c.metrics.droppedRespClientNotReading.Add(1)
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
