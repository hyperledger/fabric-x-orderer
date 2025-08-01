// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
)

type FakeBatchedRequestsVerifier struct {
	VerifyBatchedRequestsStub        func(types.BatchedRequests) error
	verifyBatchedRequestsMutex       sync.RWMutex
	verifyBatchedRequestsArgsForCall []struct {
		arg1 types.BatchedRequests
	}
	verifyBatchedRequestsReturns struct {
		result1 error
	}
	verifyBatchedRequestsReturnsOnCall map[int]struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeBatchedRequestsVerifier) VerifyBatchedRequests(arg1 types.BatchedRequests) error {
	fake.verifyBatchedRequestsMutex.Lock()
	ret, specificReturn := fake.verifyBatchedRequestsReturnsOnCall[len(fake.verifyBatchedRequestsArgsForCall)]
	fake.verifyBatchedRequestsArgsForCall = append(fake.verifyBatchedRequestsArgsForCall, struct {
		arg1 types.BatchedRequests
	}{arg1})
	stub := fake.VerifyBatchedRequestsStub
	fakeReturns := fake.verifyBatchedRequestsReturns
	fake.recordInvocation("VerifyBatchedRequests", []interface{}{arg1})
	fake.verifyBatchedRequestsMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeBatchedRequestsVerifier) VerifyBatchedRequestsCallCount() int {
	fake.verifyBatchedRequestsMutex.RLock()
	defer fake.verifyBatchedRequestsMutex.RUnlock()
	return len(fake.verifyBatchedRequestsArgsForCall)
}

func (fake *FakeBatchedRequestsVerifier) VerifyBatchedRequestsCalls(stub func(types.BatchedRequests) error) {
	fake.verifyBatchedRequestsMutex.Lock()
	defer fake.verifyBatchedRequestsMutex.Unlock()
	fake.VerifyBatchedRequestsStub = stub
}

func (fake *FakeBatchedRequestsVerifier) VerifyBatchedRequestsArgsForCall(i int) types.BatchedRequests {
	fake.verifyBatchedRequestsMutex.RLock()
	defer fake.verifyBatchedRequestsMutex.RUnlock()
	argsForCall := fake.verifyBatchedRequestsArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeBatchedRequestsVerifier) VerifyBatchedRequestsReturns(result1 error) {
	fake.verifyBatchedRequestsMutex.Lock()
	defer fake.verifyBatchedRequestsMutex.Unlock()
	fake.VerifyBatchedRequestsStub = nil
	fake.verifyBatchedRequestsReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeBatchedRequestsVerifier) VerifyBatchedRequestsReturnsOnCall(i int, result1 error) {
	fake.verifyBatchedRequestsMutex.Lock()
	defer fake.verifyBatchedRequestsMutex.Unlock()
	fake.VerifyBatchedRequestsStub = nil
	if fake.verifyBatchedRequestsReturnsOnCall == nil {
		fake.verifyBatchedRequestsReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.verifyBatchedRequestsReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeBatchedRequestsVerifier) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.verifyBatchedRequestsMutex.RLock()
	defer fake.verifyBatchedRequestsMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeBatchedRequestsVerifier) recordInvocation(key string, args []interface{}) {
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

var _ core.BatchedRequestsVerifier = new(FakeBatchedRequestsVerifier)
