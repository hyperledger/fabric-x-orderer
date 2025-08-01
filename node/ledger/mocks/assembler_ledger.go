// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
)

type FakeAssemblerLedgerReaderWriter struct {
	AppendStub        func(core.Batch, core.OrderingInfo)
	appendMutex       sync.RWMutex
	appendArgsForCall []struct {
		arg1 core.Batch
		arg2 core.OrderingInfo
	}
	AppendConfigStub        func(*common.Block, types.DecisionNum)
	appendConfigMutex       sync.RWMutex
	appendConfigArgsForCall []struct {
		arg1 *common.Block
		arg2 types.DecisionNum
	}
	BatchFrontierStub        func([]types.ShardID, []types.PartyID, time.Duration) (map[types.ShardID]map[types.PartyID]types.BatchSequence, error)
	batchFrontierMutex       sync.RWMutex
	batchFrontierArgsForCall []struct {
		arg1 []types.ShardID
		arg2 []types.PartyID
		arg3 time.Duration
	}
	batchFrontierReturns struct {
		result1 map[types.ShardID]map[types.PartyID]types.BatchSequence
		result2 error
	}
	batchFrontierReturnsOnCall map[int]struct {
		result1 map[types.ShardID]map[types.PartyID]types.BatchSequence
		result2 error
	}
	CloseStub        func()
	closeMutex       sync.RWMutex
	closeArgsForCall []struct {
	}
	GetTxCountStub        func() uint64
	getTxCountMutex       sync.RWMutex
	getTxCountArgsForCall []struct {
	}
	getTxCountReturns struct {
		result1 uint64
	}
	getTxCountReturnsOnCall map[int]struct {
		result1 uint64
	}
	LastOrderingInfoStub        func() (*state.OrderingInformation, error)
	lastOrderingInfoMutex       sync.RWMutex
	lastOrderingInfoArgsForCall []struct {
	}
	lastOrderingInfoReturns struct {
		result1 *state.OrderingInformation
		result2 error
	}
	lastOrderingInfoReturnsOnCall map[int]struct {
		result1 *state.OrderingInformation
		result2 error
	}
	LedgerReaderStub        func() blockledger.Reader
	ledgerReaderMutex       sync.RWMutex
	ledgerReaderArgsForCall []struct {
	}
	ledgerReaderReturns struct {
		result1 blockledger.Reader
	}
	ledgerReaderReturnsOnCall map[int]struct {
		result1 blockledger.Reader
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeAssemblerLedgerReaderWriter) Append(arg1 core.Batch, arg2 core.OrderingInfo) {
	fake.appendMutex.Lock()
	fake.appendArgsForCall = append(fake.appendArgsForCall, struct {
		arg1 core.Batch
		arg2 core.OrderingInfo
	}{arg1, arg2})
	stub := fake.AppendStub
	fake.recordInvocation("Append", []interface{}{arg1, arg2})
	fake.appendMutex.Unlock()
	if stub != nil {
		fake.AppendStub(arg1, arg2)
	}
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendCallCount() int {
	fake.appendMutex.RLock()
	defer fake.appendMutex.RUnlock()
	return len(fake.appendArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendCalls(stub func(core.Batch, core.OrderingInfo)) {
	fake.appendMutex.Lock()
	defer fake.appendMutex.Unlock()
	fake.AppendStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendArgsForCall(i int) (core.Batch, core.OrderingInfo) {
	fake.appendMutex.RLock()
	defer fake.appendMutex.RUnlock()
	argsForCall := fake.appendArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendConfig(arg1 *common.Block, arg2 types.DecisionNum) {
	fake.appendConfigMutex.Lock()
	fake.appendConfigArgsForCall = append(fake.appendConfigArgsForCall, struct {
		arg1 *common.Block
		arg2 types.DecisionNum
	}{arg1, arg2})
	stub := fake.AppendConfigStub
	fake.recordInvocation("AppendConfig", []interface{}{arg1, arg2})
	fake.appendConfigMutex.Unlock()
	if stub != nil {
		fake.AppendConfigStub(arg1, arg2)
	}
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendConfigCallCount() int {
	fake.appendConfigMutex.RLock()
	defer fake.appendConfigMutex.RUnlock()
	return len(fake.appendConfigArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendConfigCalls(stub func(*common.Block, types.DecisionNum)) {
	fake.appendConfigMutex.Lock()
	defer fake.appendConfigMutex.Unlock()
	fake.AppendConfigStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) AppendConfigArgsForCall(i int) (*common.Block, types.DecisionNum) {
	fake.appendConfigMutex.RLock()
	defer fake.appendConfigMutex.RUnlock()
	argsForCall := fake.appendConfigArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeAssemblerLedgerReaderWriter) BatchFrontier(arg1 []types.ShardID, arg2 []types.PartyID, arg3 time.Duration) (map[types.ShardID]map[types.PartyID]types.BatchSequence, error) {
	var arg1Copy []types.ShardID
	if arg1 != nil {
		arg1Copy = make([]types.ShardID, len(arg1))
		copy(arg1Copy, arg1)
	}
	var arg2Copy []types.PartyID
	if arg2 != nil {
		arg2Copy = make([]types.PartyID, len(arg2))
		copy(arg2Copy, arg2)
	}
	fake.batchFrontierMutex.Lock()
	ret, specificReturn := fake.batchFrontierReturnsOnCall[len(fake.batchFrontierArgsForCall)]
	fake.batchFrontierArgsForCall = append(fake.batchFrontierArgsForCall, struct {
		arg1 []types.ShardID
		arg2 []types.PartyID
		arg3 time.Duration
	}{arg1Copy, arg2Copy, arg3})
	stub := fake.BatchFrontierStub
	fakeReturns := fake.batchFrontierReturns
	fake.recordInvocation("BatchFrontier", []interface{}{arg1Copy, arg2Copy, arg3})
	fake.batchFrontierMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeAssemblerLedgerReaderWriter) BatchFrontierCallCount() int {
	fake.batchFrontierMutex.RLock()
	defer fake.batchFrontierMutex.RUnlock()
	return len(fake.batchFrontierArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) BatchFrontierCalls(stub func([]types.ShardID, []types.PartyID, time.Duration) (map[types.ShardID]map[types.PartyID]types.BatchSequence, error)) {
	fake.batchFrontierMutex.Lock()
	defer fake.batchFrontierMutex.Unlock()
	fake.BatchFrontierStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) BatchFrontierArgsForCall(i int) ([]types.ShardID, []types.PartyID, time.Duration) {
	fake.batchFrontierMutex.RLock()
	defer fake.batchFrontierMutex.RUnlock()
	argsForCall := fake.batchFrontierArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeAssemblerLedgerReaderWriter) BatchFrontierReturns(result1 map[types.ShardID]map[types.PartyID]types.BatchSequence, result2 error) {
	fake.batchFrontierMutex.Lock()
	defer fake.batchFrontierMutex.Unlock()
	fake.BatchFrontierStub = nil
	fake.batchFrontierReturns = struct {
		result1 map[types.ShardID]map[types.PartyID]types.BatchSequence
		result2 error
	}{result1, result2}
}

func (fake *FakeAssemblerLedgerReaderWriter) BatchFrontierReturnsOnCall(i int, result1 map[types.ShardID]map[types.PartyID]types.BatchSequence, result2 error) {
	fake.batchFrontierMutex.Lock()
	defer fake.batchFrontierMutex.Unlock()
	fake.BatchFrontierStub = nil
	if fake.batchFrontierReturnsOnCall == nil {
		fake.batchFrontierReturnsOnCall = make(map[int]struct {
			result1 map[types.ShardID]map[types.PartyID]types.BatchSequence
			result2 error
		})
	}
	fake.batchFrontierReturnsOnCall[i] = struct {
		result1 map[types.ShardID]map[types.PartyID]types.BatchSequence
		result2 error
	}{result1, result2}
}

func (fake *FakeAssemblerLedgerReaderWriter) Close() {
	fake.closeMutex.Lock()
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	stub := fake.CloseStub
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if stub != nil {
		fake.CloseStub()
	}
}

func (fake *FakeAssemblerLedgerReaderWriter) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) CloseCalls(stub func()) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) GetTxCount() uint64 {
	fake.getTxCountMutex.Lock()
	ret, specificReturn := fake.getTxCountReturnsOnCall[len(fake.getTxCountArgsForCall)]
	fake.getTxCountArgsForCall = append(fake.getTxCountArgsForCall, struct {
	}{})
	stub := fake.GetTxCountStub
	fakeReturns := fake.getTxCountReturns
	fake.recordInvocation("GetTxCount", []interface{}{})
	fake.getTxCountMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeAssemblerLedgerReaderWriter) GetTxCountCallCount() int {
	fake.getTxCountMutex.RLock()
	defer fake.getTxCountMutex.RUnlock()
	return len(fake.getTxCountArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) GetTxCountCalls(stub func() uint64) {
	fake.getTxCountMutex.Lock()
	defer fake.getTxCountMutex.Unlock()
	fake.GetTxCountStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) GetTxCountReturns(result1 uint64) {
	fake.getTxCountMutex.Lock()
	defer fake.getTxCountMutex.Unlock()
	fake.GetTxCountStub = nil
	fake.getTxCountReturns = struct {
		result1 uint64
	}{result1}
}

func (fake *FakeAssemblerLedgerReaderWriter) GetTxCountReturnsOnCall(i int, result1 uint64) {
	fake.getTxCountMutex.Lock()
	defer fake.getTxCountMutex.Unlock()
	fake.GetTxCountStub = nil
	if fake.getTxCountReturnsOnCall == nil {
		fake.getTxCountReturnsOnCall = make(map[int]struct {
			result1 uint64
		})
	}
	fake.getTxCountReturnsOnCall[i] = struct {
		result1 uint64
	}{result1}
}

func (fake *FakeAssemblerLedgerReaderWriter) LastOrderingInfo() (*state.OrderingInformation, error) {
	fake.lastOrderingInfoMutex.Lock()
	ret, specificReturn := fake.lastOrderingInfoReturnsOnCall[len(fake.lastOrderingInfoArgsForCall)]
	fake.lastOrderingInfoArgsForCall = append(fake.lastOrderingInfoArgsForCall, struct {
	}{})
	stub := fake.LastOrderingInfoStub
	fakeReturns := fake.lastOrderingInfoReturns
	fake.recordInvocation("LastOrderingInfo", []interface{}{})
	fake.lastOrderingInfoMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeAssemblerLedgerReaderWriter) LastOrderingInfoCallCount() int {
	fake.lastOrderingInfoMutex.RLock()
	defer fake.lastOrderingInfoMutex.RUnlock()
	return len(fake.lastOrderingInfoArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) LastOrderingInfoCalls(stub func() (*state.OrderingInformation, error)) {
	fake.lastOrderingInfoMutex.Lock()
	defer fake.lastOrderingInfoMutex.Unlock()
	fake.LastOrderingInfoStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) LastOrderingInfoReturns(result1 *state.OrderingInformation, result2 error) {
	fake.lastOrderingInfoMutex.Lock()
	defer fake.lastOrderingInfoMutex.Unlock()
	fake.LastOrderingInfoStub = nil
	fake.lastOrderingInfoReturns = struct {
		result1 *state.OrderingInformation
		result2 error
	}{result1, result2}
}

func (fake *FakeAssemblerLedgerReaderWriter) LastOrderingInfoReturnsOnCall(i int, result1 *state.OrderingInformation, result2 error) {
	fake.lastOrderingInfoMutex.Lock()
	defer fake.lastOrderingInfoMutex.Unlock()
	fake.LastOrderingInfoStub = nil
	if fake.lastOrderingInfoReturnsOnCall == nil {
		fake.lastOrderingInfoReturnsOnCall = make(map[int]struct {
			result1 *state.OrderingInformation
			result2 error
		})
	}
	fake.lastOrderingInfoReturnsOnCall[i] = struct {
		result1 *state.OrderingInformation
		result2 error
	}{result1, result2}
}

func (fake *FakeAssemblerLedgerReaderWriter) LedgerReader() blockledger.Reader {
	fake.ledgerReaderMutex.Lock()
	ret, specificReturn := fake.ledgerReaderReturnsOnCall[len(fake.ledgerReaderArgsForCall)]
	fake.ledgerReaderArgsForCall = append(fake.ledgerReaderArgsForCall, struct {
	}{})
	stub := fake.LedgerReaderStub
	fakeReturns := fake.ledgerReaderReturns
	fake.recordInvocation("LedgerReader", []interface{}{})
	fake.ledgerReaderMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeAssemblerLedgerReaderWriter) LedgerReaderCallCount() int {
	fake.ledgerReaderMutex.RLock()
	defer fake.ledgerReaderMutex.RUnlock()
	return len(fake.ledgerReaderArgsForCall)
}

func (fake *FakeAssemblerLedgerReaderWriter) LedgerReaderCalls(stub func() blockledger.Reader) {
	fake.ledgerReaderMutex.Lock()
	defer fake.ledgerReaderMutex.Unlock()
	fake.LedgerReaderStub = stub
}

func (fake *FakeAssemblerLedgerReaderWriter) LedgerReaderReturns(result1 blockledger.Reader) {
	fake.ledgerReaderMutex.Lock()
	defer fake.ledgerReaderMutex.Unlock()
	fake.LedgerReaderStub = nil
	fake.ledgerReaderReturns = struct {
		result1 blockledger.Reader
	}{result1}
}

func (fake *FakeAssemblerLedgerReaderWriter) LedgerReaderReturnsOnCall(i int, result1 blockledger.Reader) {
	fake.ledgerReaderMutex.Lock()
	defer fake.ledgerReaderMutex.Unlock()
	fake.LedgerReaderStub = nil
	if fake.ledgerReaderReturnsOnCall == nil {
		fake.ledgerReaderReturnsOnCall = make(map[int]struct {
			result1 blockledger.Reader
		})
	}
	fake.ledgerReaderReturnsOnCall[i] = struct {
		result1 blockledger.Reader
	}{result1}
}

func (fake *FakeAssemblerLedgerReaderWriter) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.appendMutex.RLock()
	defer fake.appendMutex.RUnlock()
	fake.appendConfigMutex.RLock()
	defer fake.appendConfigMutex.RUnlock()
	fake.batchFrontierMutex.RLock()
	defer fake.batchFrontierMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.getTxCountMutex.RLock()
	defer fake.getTxCountMutex.RUnlock()
	fake.lastOrderingInfoMutex.RLock()
	defer fake.lastOrderingInfoMutex.RUnlock()
	fake.ledgerReaderMutex.RLock()
	defer fake.ledgerReaderMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeAssemblerLedgerReaderWriter) recordInvocation(key string, args []interface{}) {
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

var _ ledger.AssemblerLedgerReaderWriter = new(FakeAssemblerLedgerReaderWriter)
