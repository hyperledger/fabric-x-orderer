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
}

func newClientChannel(capacity int) *clientChannel {
	return &clientChannel{responses: make(chan Response, capacity)}
}

// reply queues resp for the client without blocking, and reports whether it was
// queued. A response that does not fit is dropped.
func (c *clientChannel) reply(resp Response) bool {
	select {
	case c.responses <- resp:
		return true
	default:
		return false
	}
}
