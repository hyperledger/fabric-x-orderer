/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Create a client channel with room for one response.
//  2. Reply once and expect the response to be queued.
//  3. Expect the queued response to be the one that was sent.
func TestClientChannelReplyQueuesResponse(t *testing.T) {
	cc := newClientChannel(1)

	require.True(t, cc.reply(Response{reqID: []byte("req1")}))

	resp := <-cc.responses
	require.Equal(t, []byte("req1"), resp.reqID)
}

// Scenario:
//  1. Create a client channel with room for one response and fill it.
//  2. Reply again and expect the response to be dropped.
func TestClientChannelReplyDropsWhenFull(t *testing.T) {
	cc := newClientChannel(1)

	require.True(t, cc.reply(Response{}))
	require.False(t, cc.reply(Response{}))
}

// Scenario:
//  1. Create a client channel with nothing reading it.
//  2. Reply more times than the buffer can hold, from a goroutine.
//  3. Expect every reply to return rather than block.
func TestClientChannelReplyNeverBlocks(t *testing.T) {
	const capacity = 8
	cc := newClientChannel(capacity)

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
}
