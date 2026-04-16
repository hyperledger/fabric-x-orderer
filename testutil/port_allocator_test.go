/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type testListener struct {
	closed atomic.Bool
}

func (l *testListener) Accept() (net.Conn, error) {
	return nil, errors.New("test listener does not accept")
}

func (l *testListener) Close() error {
	l.closed.Store(true)
	return nil
}

func (l *testListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

type allocationResult struct {
	port     string
	listener *testListener
}

func scriptedAllocator(t *testing.T, results []allocationResult) func(t *testing.T) (string, net.Listener) {
	t.Helper()

	var lock sync.Mutex
	idx := 0

	return func(t *testing.T) (string, net.Listener) {
		t.Helper()

		lock.Lock()
		defer lock.Unlock()

		require.Less(t, idx, len(results), "scripted allocator exhausted")
		result := results[idx]
		idx++
		return result.port, result.listener
	}
}

// Scenario: two allocations in the same test receive a duplicate candidate port (41000),
// so the allocator retries and returns a different unique port.
func TestScopedPortAllocator_RetriesOnDuplicatePort(t *testing.T) {
	allocator := NewTestScopedPortAllocator()
	results := []allocationResult{
		{port: "41000", listener: &testListener{}},
		{port: "41000", listener: &testListener{}},
		{port: "41001", listener: &testListener{}},
	}
	allocator.allocatePort = scriptedAllocator(t, results)

	port1, ll1 := allocator.Allocate(t)
	require.Equal(t, "41000", port1)
	require.NoError(t, ll1.Close())

	port2, ll2 := allocator.Allocate(t)
	require.Equal(t, "41001", port2)
	require.NoError(t, ll2.Close())

	require.True(t, results[1].listener.closed.Load(), "duplicate candidate listener should be closed")
	require.Equal(t, t.Name(), allocator.ownerByPort["41000"])
	require.Equal(t, t.Name(), allocator.ownerByPort["41001"])
}

// Scenario: a child test allocates a port and exits, and cleanup must release
// all ownership state for that child test.
func TestScopedPortAllocator_ReleasesPortsOnCleanup(t *testing.T) {
	allocator := NewTestScopedPortAllocator()
	results := []allocationResult{{port: "42000", listener: &testListener{}}}
	allocator.allocatePort = scriptedAllocator(t, results)

	var childName string
	var childPort string
	t.Run("child", func(t *testing.T) {
		childName = t.Name()

		port, ll := allocator.Allocate(t)
		childPort = port
		require.Equal(t, "42000", port)
		require.NoError(t, ll.Close())

		require.Equal(t, childName, allocator.ownerByPort[childPort])
		require.Equal(t, []string{childPort}, allocator.portsByTest[childName])
		require.True(t, allocator.cleanupByTest[childName])
	})

	require.NotContains(t, allocator.ownerByPort, childPort)
	require.NotContains(t, allocator.portsByTest, childName)
	require.NotContains(t, allocator.cleanupByTest, childName)
}

// Scenario: two coordinated parallel subtests contend on the same first
// candidate port (43000) and must end up with different allocated ports.
func TestScopedPortAllocator_ParallelSubtestsDoNotSharePort(t *testing.T) {
	allocator := NewTestScopedPortAllocator()
	results := []allocationResult{
		{port: "43000", listener: &testListener{}},
		{port: "43000", listener: &testListener{}},
		{port: "43001", listener: &testListener{}},
	}
	allocator.allocatePort = scriptedAllocator(t, results)

	ready := make(chan struct{})
	release := make(chan struct{})
	ports := make(chan string, 2)

	t.Run("parallel", func(t *testing.T) {
		t.Run("holder", func(t *testing.T) {
			t.Parallel()

			port, ll := allocator.Allocate(t)
			require.NoError(t, ll.Close())
			ports <- port
			close(ready)
			<-release
		})

		t.Run("contender", func(t *testing.T) {
			t.Parallel()

			<-ready
			port, ll := allocator.Allocate(t)
			require.NoError(t, ll.Close())
			ports <- port
			close(release)
		})
	})

	portA := <-ports
	portB := <-ports
	require.NotEqual(t, portA, portB, "parallel subtests should receive different ports")
	require.True(t, results[0].listener.closed.Load(), "first candidate listener should be closed")
	require.True(t, results[1].listener.closed.Load(), "duplicate candidate listener should be closed")
}
