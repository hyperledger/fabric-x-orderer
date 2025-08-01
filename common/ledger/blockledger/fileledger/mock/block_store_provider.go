// Code generated by counterfeiter. DO NOT EDIT.
package mock

import (
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/blkstorage"
)

type BlockStoreProvider struct {
	CloseStub        func()
	closeMutex       sync.RWMutex
	closeArgsForCall []struct {
	}
	DropStub        func(string) error
	dropMutex       sync.RWMutex
	dropArgsForCall []struct {
		arg1 string
	}
	dropReturns struct {
		result1 error
	}
	dropReturnsOnCall map[int]struct {
		result1 error
	}
	ListStub        func() ([]string, error)
	listMutex       sync.RWMutex
	listArgsForCall []struct {
	}
	listReturns struct {
		result1 []string
		result2 error
	}
	listReturnsOnCall map[int]struct {
		result1 []string
		result2 error
	}
	OpenStub        func(string) (*blkstorage.BlockStore, error)
	openMutex       sync.RWMutex
	openArgsForCall []struct {
		arg1 string
	}
	openReturns struct {
		result1 *blkstorage.BlockStore
		result2 error
	}
	openReturnsOnCall map[int]struct {
		result1 *blkstorage.BlockStore
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *BlockStoreProvider) Close() {
	fake.closeMutex.Lock()
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if fake.CloseStub != nil {
		fake.CloseStub()
	}
}

func (fake *BlockStoreProvider) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *BlockStoreProvider) CloseCalls(stub func()) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *BlockStoreProvider) Drop(arg1 string) error {
	fake.dropMutex.Lock()
	ret, specificReturn := fake.dropReturnsOnCall[len(fake.dropArgsForCall)]
	fake.dropArgsForCall = append(fake.dropArgsForCall, struct {
		arg1 string
	}{arg1})
	fake.recordInvocation("Drop", []interface{}{arg1})
	fake.dropMutex.Unlock()
	if fake.DropStub != nil {
		return fake.DropStub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.dropReturns
	return fakeReturns.result1
}

func (fake *BlockStoreProvider) DropCallCount() int {
	fake.dropMutex.RLock()
	defer fake.dropMutex.RUnlock()
	return len(fake.dropArgsForCall)
}

func (fake *BlockStoreProvider) DropCalls(stub func(string) error) {
	fake.dropMutex.Lock()
	defer fake.dropMutex.Unlock()
	fake.DropStub = stub
}

func (fake *BlockStoreProvider) DropArgsForCall(i int) string {
	fake.dropMutex.RLock()
	defer fake.dropMutex.RUnlock()
	argsForCall := fake.dropArgsForCall[i]
	return argsForCall.arg1
}

func (fake *BlockStoreProvider) DropReturns(result1 error) {
	fake.dropMutex.Lock()
	defer fake.dropMutex.Unlock()
	fake.DropStub = nil
	fake.dropReturns = struct {
		result1 error
	}{result1}
}

func (fake *BlockStoreProvider) DropReturnsOnCall(i int, result1 error) {
	fake.dropMutex.Lock()
	defer fake.dropMutex.Unlock()
	fake.DropStub = nil
	if fake.dropReturnsOnCall == nil {
		fake.dropReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.dropReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *BlockStoreProvider) List() ([]string, error) {
	fake.listMutex.Lock()
	ret, specificReturn := fake.listReturnsOnCall[len(fake.listArgsForCall)]
	fake.listArgsForCall = append(fake.listArgsForCall, struct {
	}{})
	fake.recordInvocation("List", []interface{}{})
	fake.listMutex.Unlock()
	if fake.ListStub != nil {
		return fake.ListStub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	fakeReturns := fake.listReturns
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *BlockStoreProvider) ListCallCount() int {
	fake.listMutex.RLock()
	defer fake.listMutex.RUnlock()
	return len(fake.listArgsForCall)
}

func (fake *BlockStoreProvider) ListCalls(stub func() ([]string, error)) {
	fake.listMutex.Lock()
	defer fake.listMutex.Unlock()
	fake.ListStub = stub
}

func (fake *BlockStoreProvider) ListReturns(result1 []string, result2 error) {
	fake.listMutex.Lock()
	defer fake.listMutex.Unlock()
	fake.ListStub = nil
	fake.listReturns = struct {
		result1 []string
		result2 error
	}{result1, result2}
}

func (fake *BlockStoreProvider) ListReturnsOnCall(i int, result1 []string, result2 error) {
	fake.listMutex.Lock()
	defer fake.listMutex.Unlock()
	fake.ListStub = nil
	if fake.listReturnsOnCall == nil {
		fake.listReturnsOnCall = make(map[int]struct {
			result1 []string
			result2 error
		})
	}
	fake.listReturnsOnCall[i] = struct {
		result1 []string
		result2 error
	}{result1, result2}
}

func (fake *BlockStoreProvider) Open(arg1 string) (*blkstorage.BlockStore, error) {
	fake.openMutex.Lock()
	ret, specificReturn := fake.openReturnsOnCall[len(fake.openArgsForCall)]
	fake.openArgsForCall = append(fake.openArgsForCall, struct {
		arg1 string
	}{arg1})
	fake.recordInvocation("Open", []interface{}{arg1})
	fake.openMutex.Unlock()
	if fake.OpenStub != nil {
		return fake.OpenStub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	fakeReturns := fake.openReturns
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *BlockStoreProvider) OpenCallCount() int {
	fake.openMutex.RLock()
	defer fake.openMutex.RUnlock()
	return len(fake.openArgsForCall)
}

func (fake *BlockStoreProvider) OpenCalls(stub func(string) (*blkstorage.BlockStore, error)) {
	fake.openMutex.Lock()
	defer fake.openMutex.Unlock()
	fake.OpenStub = stub
}

func (fake *BlockStoreProvider) OpenArgsForCall(i int) string {
	fake.openMutex.RLock()
	defer fake.openMutex.RUnlock()
	argsForCall := fake.openArgsForCall[i]
	return argsForCall.arg1
}

func (fake *BlockStoreProvider) OpenReturns(result1 *blkstorage.BlockStore, result2 error) {
	fake.openMutex.Lock()
	defer fake.openMutex.Unlock()
	fake.OpenStub = nil
	fake.openReturns = struct {
		result1 *blkstorage.BlockStore
		result2 error
	}{result1, result2}
}

func (fake *BlockStoreProvider) OpenReturnsOnCall(i int, result1 *blkstorage.BlockStore, result2 error) {
	fake.openMutex.Lock()
	defer fake.openMutex.Unlock()
	fake.OpenStub = nil
	if fake.openReturnsOnCall == nil {
		fake.openReturnsOnCall = make(map[int]struct {
			result1 *blkstorage.BlockStore
			result2 error
		})
	}
	fake.openReturnsOnCall[i] = struct {
		result1 *blkstorage.BlockStore
		result2 error
	}{result1, result2}
}

func (fake *BlockStoreProvider) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.dropMutex.RLock()
	defer fake.dropMutex.RUnlock()
	fake.listMutex.RLock()
	defer fake.listMutex.RUnlock()
	fake.openMutex.RLock()
	defer fake.openMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *BlockStoreProvider) recordInvocation(key string, args []interface{}) {
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
