/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

// fakeCommunicator is a no-op comm.Communicator used to exercise RPC.Reconfigure
// without establishing real connections.
type fakeCommunicator struct{}

func (f *fakeCommunicator) Remote(id uint64) (*comm.RemoteContext, error) { return nil, nil }
func (f *fakeCommunicator) Configure(members []comm.RemoteNode)           {}
func (f *fakeCommunicator) Shutdown()                                     {}

func newTestRPC() *comm.RPC {
	return &comm.RPC{
		StreamsByType: comm.NewStreamsByType(),
		Timeout:       time.Minute,
		Comm:          &fakeCommunicator{},
	}
}

func member(id uint64) comm.RemoteNode {
	return comm.RemoteNode{NodeAddress: comm.NodeAddress{ID: id}}
}

func TestRPCNodesAfterReconfigure(t *testing.T) {
	rpc := newTestRPC()

	if got := rpc.Nodes(); len(got) != 0 {
		t.Fatalf("expected no nodes before reconfigure, got %v", got)
	}

	rpc.Reconfigure([]comm.RemoteNode{member(1), member(2), member(3)})

	got := rpc.Nodes()
	slices.Sort(got)
	want := []uint64{1, 2, 3}
	if !slices.Equal(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestRPCNodesReturnsCopy(t *testing.T) {
	rpc := newTestRPC()
	rpc.Reconfigure([]comm.RemoteNode{member(1), member(2)})

	first := rpc.Nodes()
	first[0] = 99 // mutate the returned slice

	second := rpc.Nodes()
	for _, id := range second {
		if id == 99 {
			t.Fatalf("mutating a returned slice affected internal state: %v", second)
		}
	}
}

func TestRPCNodesReconfigureRace(t *testing.T) {
	rpc := newTestRPC()
	rpc.Reconfigure([]comm.RemoteNode{member(1)})

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = rpc.Nodes()
		}()
		go func(n uint64) {
			defer wg.Done()
			rpc.Reconfigure([]comm.RemoteNode{member(n), member(n + 1)})
		}(uint64(i))
	}
	wg.Wait()
}
