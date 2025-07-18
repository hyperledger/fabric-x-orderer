// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"

	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
)

type FakeConsensusBringer struct {
	ReplicateStub        func() <-chan core.OrderedBatchAttestation
	replicateMutex       sync.RWMutex
	replicateArgsForCall []struct {
	}
	replicateReturns struct {
		result1 <-chan core.OrderedBatchAttestation
	}
	replicateReturnsOnCall map[int]struct {
		result1 <-chan core.OrderedBatchAttestation
	}
	StopStub        func()
	stopMutex       sync.RWMutex
	stopArgsForCall []struct {
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeConsensusBringer) Replicate() <-chan core.OrderedBatchAttestation {
	fake.replicateMutex.Lock()
	ret, specificReturn := fake.replicateReturnsOnCall[len(fake.replicateArgsForCall)]
	fake.replicateArgsForCall = append(fake.replicateArgsForCall, struct {
	}{})
	stub := fake.ReplicateStub
	fakeReturns := fake.replicateReturns
	fake.recordInvocation("Replicate", []interface{}{})
	fake.replicateMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeConsensusBringer) ReplicateCallCount() int {
	fake.replicateMutex.RLock()
	defer fake.replicateMutex.RUnlock()
	return len(fake.replicateArgsForCall)
}

func (fake *FakeConsensusBringer) ReplicateCalls(stub func() <-chan core.OrderedBatchAttestation) {
	fake.replicateMutex.Lock()
	defer fake.replicateMutex.Unlock()
	fake.ReplicateStub = stub
}

func (fake *FakeConsensusBringer) ReplicateReturns(result1 <-chan core.OrderedBatchAttestation) {
	fake.replicateMutex.Lock()
	defer fake.replicateMutex.Unlock()
	fake.ReplicateStub = nil
	fake.replicateReturns = struct {
		result1 <-chan core.OrderedBatchAttestation
	}{result1}
}

func (fake *FakeConsensusBringer) ReplicateReturnsOnCall(i int, result1 <-chan core.OrderedBatchAttestation) {
	fake.replicateMutex.Lock()
	defer fake.replicateMutex.Unlock()
	fake.ReplicateStub = nil
	if fake.replicateReturnsOnCall == nil {
		fake.replicateReturnsOnCall = make(map[int]struct {
			result1 <-chan core.OrderedBatchAttestation
		})
	}
	fake.replicateReturnsOnCall[i] = struct {
		result1 <-chan core.OrderedBatchAttestation
	}{result1}
}

func (fake *FakeConsensusBringer) Stop() {
	fake.stopMutex.Lock()
	fake.stopArgsForCall = append(fake.stopArgsForCall, struct {
	}{})
	stub := fake.StopStub
	fake.recordInvocation("Stop", []interface{}{})
	fake.stopMutex.Unlock()
	if stub != nil {
		fake.StopStub()
	}
}

func (fake *FakeConsensusBringer) StopCallCount() int {
	fake.stopMutex.RLock()
	defer fake.stopMutex.RUnlock()
	return len(fake.stopArgsForCall)
}

func (fake *FakeConsensusBringer) StopCalls(stub func()) {
	fake.stopMutex.Lock()
	defer fake.stopMutex.Unlock()
	fake.StopStub = stub
}

func (fake *FakeConsensusBringer) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.replicateMutex.RLock()
	defer fake.replicateMutex.RUnlock()
	fake.stopMutex.RLock()
	defer fake.stopMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeConsensusBringer) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ delivery.ConsensusBringer = new(FakeConsensusBringer)
