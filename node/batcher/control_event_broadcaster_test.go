/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/batcher/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestBroadcastControlEventQuorumScenarios(t *testing.T) {
	type testCase struct {
		name        string
		f           int
		n           int
		succ        int
		fail        int
		expectError bool
	}

	tests := []testCase{
		{name: "n=4 f=1 all succeed", n: 4, f: 1, succ: 4, fail: 0, expectError: false},
		{name: "n=4 f=1 one fails", n: 4, f: 1, succ: 3, fail: 1, expectError: false},
		{name: "n=4 f=1 two fail (no quorum)", n: 4, f: 1, succ: 2, fail: 2, expectError: true},

		{name: "n=7 f=2 all succeed", n: 7, f: 2, succ: 7, fail: 0, expectError: false},
		{name: "n=7 f=2 two fail", n: 7, f: 2, succ: 5, fail: 2, expectError: false},
		{name: "n=7 f=2 three fail (no quorum)", n: 7, f: 2, succ: 4, fail: 3, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := testutil.CreateLogger(t, 1)

			var senders []batcher.ConsenterControlEventSender
			for i := 0; i < tt.succ; i++ {
				s := &mocks.FakeConsenterControlEventSender{}
				s.SendControlEventReturns(nil)
				senders = append(senders, s)
			}

			for i := 0; i < tt.fail; i++ {
				s := &mocks.FakeConsenterControlEventSender{}
				s.SendControlEventReturns(errors.New(""))
				senders = append(senders, s)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			b := batcher.NewControlEventBroadcaster(senders, tt.n, tt.f, 10*time.Millisecond, 100*time.Millisecond, logger, ctx, cancel)
			defer b.Stop()

			err := b.BroadcastControlEvent(state.ControlEvent{})
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestControlEventBroadcasterSenderRetry(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)
	var senders []batcher.ConsenterControlEventSender

	// two senders succeed
	for i := 0; i < 2; i++ {
		s := &mocks.FakeConsenterControlEventSender{}
		s.SendControlEventReturns(nil)
		senders = append(senders, s)
	}

	// sender fails once, then succeeds
	sRetry := &mocks.FakeConsenterControlEventSender{}
	sRetry.SendControlEventReturnsOnCall(1, errors.New(""))
	sRetry.SendControlEventReturnsOnCall(2, nil)
	senders = append(senders, sRetry)

	// sender always fail
	sFail := &mocks.FakeConsenterControlEventSender{}
	sFail.SendControlEventReturns(errors.New(""))
	senders = append(senders, sFail)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	b := batcher.NewControlEventBroadcaster(senders, 4, 1, 10*time.Millisecond, 100*time.Millisecond, logger, ctx, cancel)
	defer b.Stop()

	err := b.BroadcastControlEvent(state.ControlEvent{})
	require.NoError(t, err)
}
