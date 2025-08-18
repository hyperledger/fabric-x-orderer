/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestAcker(t *testing.T) {
	// Test ack thresholds and constraints in secondaries wait scenario
	acker := batcher.NewAcker(100, 10, 4, 2, testutil.CreateLogger(t, 0))

	var done uint32

	wait := func() {
		ch := acker.WaitForSecondaries(110)
		for range ch {
		}
		atomic.AddUint32(&done, 1)
	}

	go wait()

	require.Equal(t, uint32(0), atomic.LoadUint32(&done))
	acker.HandleAck(100, 3)
	acker.HandleAck(90, 2) // Old ack
	require.Equal(t, uint32(0), atomic.LoadUint32(&done))

	acker.HandleAck(110, 2) // Invalid ack, exceeds the confirmed gap
	require.Equal(t, uint32(0), atomic.LoadUint32(&done))

	acker.HandleAck(100, 3) // Duplicate ack
	require.Equal(t, uint32(0), atomic.LoadUint32(&done))

	acker.HandleAck(100, 2)
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()

	// Test correct stop handling
	acker = batcher.NewAcker(100, 10, 4, 2, testutil.CreateLogger(t, 0))

	var done2 uint32

	waitStop := func() {
		atomic.AddUint32(&done2, 1)
		ch := acker.WaitForSecondaries(110)
		for range ch {
		}
		atomic.AddUint32(&done2, 1)
	}

	go waitStop()

	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done2) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
	acker.HandleAck(100, 2)
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done2) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()

	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done2) == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()

	// Test scenario without waiting for secondaries
	acker = batcher.NewAcker(100, 10, 4, 2, testutil.CreateLogger(t, 0))

	var done3 uint32

	withoutWait := func() {
		ch := acker.WaitForSecondaries(101)
		for range ch {
		}
		atomic.AddUint32(&done3, 1)
	}

	go withoutWait()

	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done3) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()
}
