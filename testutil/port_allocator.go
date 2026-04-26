/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// PortAllocator allocates a TCP port and returns both its numeric string and
// the listener that currently holds it.
type PortAllocator interface {
	Allocate(t *testing.T) (port string, ll net.Listener)
}

// DefaultPortAllocator preserves the current behavior by delegating every
// allocation to GetAvailablePort.
type DefaultPortAllocator struct{}

func (a *DefaultPortAllocator) Allocate(t *testing.T) (string, net.Listener) {
	return GetAvailablePort(t)
}

// TestScopedPortAllocator tracks allocated ports by test name and prevents
// assigning the same port to multiple tests in the same process.
type TestScopedPortAllocator struct {
	lock          sync.Mutex
	portsByTest   map[string][]string
	ownerByPort   map[string]string
	cleanupByTest map[string]bool
	allocatePort  func(t *testing.T) (string, net.Listener)
	retryAttempts int
}

func NewTestScopedPortAllocator() *TestScopedPortAllocator {
	return &TestScopedPortAllocator{
		portsByTest:   make(map[string][]string),
		ownerByPort:   make(map[string]string),
		cleanupByTest: make(map[string]bool),
		allocatePort:  GetAvailablePort,
		retryAttempts: 32,
	}
}

func (a *TestScopedPortAllocator) Allocate(t *testing.T) (string, net.Listener) {
	testName := t.Name()

	a.lock.Lock()
	if !a.cleanupByTest[testName] {
		a.cleanupByTest[testName] = true
		t.Cleanup(func() {
			a.releaseTest(testName)
		})
	}
	a.lock.Unlock()

	for attempt := 0; attempt < a.retryAttempts; attempt++ {
		port, ll := a.allocatePort(t)

		a.lock.Lock()
		_, exists := a.ownerByPort[port]
		if !exists {
			a.ownerByPort[port] = testName
			a.portsByTest[testName] = append(a.portsByTest[testName], port)
			a.lock.Unlock()
			return port, ll
		}
		a.lock.Unlock()

		_ = ll.Close()
	}

	t.Fatalf("failed allocating a unique port for test %s after %d attempts", testName, a.retryAttempts)
	return "", nil
}

func (a *TestScopedPortAllocator) releaseTest(testName string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	ports := a.portsByTest[testName]
	for _, port := range ports {
		if owner, exists := a.ownerByPort[port]; exists && owner == testName {
			delete(a.ownerByPort, port)
		}
	}
	delete(a.portsByTest, testName)
	delete(a.cleanupByTest, testName)
}

var sharedTestPortAllocator = NewTestScopedPortAllocator()

func SharedTestPortAllocator() *TestScopedPortAllocator {
	return sharedTestPortAllocator
}

// AllocateLocalhostAddress reserves a unique port via the shared test port
// allocator, closes the listener, and returns a localhost TCP
// address using that port.
func AllocateLocalhostAddress(t *testing.T) string {
	t.Helper()

	port, ll := SharedTestPortAllocator().Allocate(t)
	require.NoError(t, ll.Close())

	return net.JoinHostPort("127.0.0.1", port)
}
