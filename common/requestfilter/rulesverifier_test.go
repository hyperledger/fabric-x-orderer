/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
)

func TestRulesVerifier(t *testing.T) {
	v := requestfilter.NewRulesVerifier(nil)
	r := &mocks.FakeRule{}

	t.Run("Verify Test", func(t *testing.T) {
		r.VerifyReturns(nil)
		v.AddRule(r)
		err := v.Verify(&comm.Request{})
		require.NoError(t, err)
	})

	t.Run("Verify Fail Test", func(t *testing.T) {
		r.VerifyReturns(errors.New("verify error"))
		v.AddRule(r)
		err := v.Verify(&comm.Request{})
		require.EqualError(t, err, "verify error")
	})

	t.Run("Update Test", func(t *testing.T) {
		r.UpdateReturns(nil)
		v.AddRule(r)
		err := v.Update(&mocks.FakeFilterConfig{})
		require.NoError(t, err)
	})

	t.Run("Verify Fail Test", func(t *testing.T) {
		r.UpdateReturns(errors.New("update error"))
		v.AddRule(r)
		err := v.Update(&mocks.FakeFilterConfig{})
		require.EqualError(t, err, "update error")
	})

	t.Run("Verify Structure error", func(t *testing.T) {
		ver := requestfilter.NewRulesVerifier(nil)
		sr := &mocks.FakeStructureRule{}
		sr.VerifyAndClassifyReturns(common.HeaderType_MESSAGE, errors.New("some error"))
		ver.AddStructureRule(sr)
		_, err := ver.VerifyStructureAndClassify(&comm.Request{})
		require.EqualError(t, err, "some error")
	})

	t.Run("Verify Structure Request type", func(t *testing.T) {
		ver := requestfilter.NewRulesVerifier(nil)
		sr := &mocks.FakeStructureRule{}
		sr.VerifyAndClassifyReturns(common.HeaderType_MESSAGE, nil)
		ver.AddStructureRule(sr)
		reqType, err := ver.VerifyStructureAndClassify(&comm.Request{})
		require.NoError(t, err)
		require.Equal(t, common.HeaderType_MESSAGE, reqType)
	})
}

// scenario: test that verifiers will execute verify in parallel.
func TestConcurrentVeirfy(t *testing.T) {
	v := requestfilter.NewRulesVerifier(nil)
	fr := &mocks.FakeRule{}
	var activeVerifiers int64 = 0
	var maxVerifiers int64 = 0
	fr.VerifyStub = func(r *comm.Request) error {
		atomic.AddInt64(&activeVerifiers, 1)
		time.Sleep(50 * time.Millisecond)
		atomic.StoreInt64(&maxVerifiers, max(atomic.LoadInt64(&activeVerifiers), atomic.LoadInt64(&maxVerifiers)))
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&activeVerifiers, -1)
		return nil
	}
	v.AddRule(fr)

	var wg sync.WaitGroup
	start := make(chan struct{})
	verifiers := 8

	for i := 0; i < verifiers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := v.Verify(&comm.Request{})
			require.NoError(t, err)
		}()
	}

	close(start)
	wg.Wait()
	require.True(t, maxVerifiers > 1)
}

// scenario: multiple goroutines call verify and update. chech with -race flag.
func TestConcurrentUpdateAndVerify(t *testing.T) {
	v := requestfilter.NewRulesVerifier(nil)
	fc := &mocks.FakeFilterConfig{}
	fc.GetRequestMaxBytesReturns(1000)
	v.AddRule(requestfilter.NewMaxSizeFilter(fc))

	var wg sync.WaitGroup
	start := make(chan struct{})
	n := 30

	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			err := v.Verify(&comm.Request{})
			require.NoError(t, err)
		}()
		go func(i int) {
			defer wg.Done()
			fc2 := &mocks.FakeFilterConfig{}
			fc2.GetRequestMaxBytesReturns(uint64(100 + i))
			<-start
			err := v.Update(fc2)
			require.NoError(t, err)
		}(i)
	}

	close(start)
	wg.Wait()
}
