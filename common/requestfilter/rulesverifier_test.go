/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"errors"
	"sync"
	"testing"
	"time"

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
}

// scenario: test that verifiers can run in parallel.
func TestConcurrentVeirfy(t *testing.T) {
	v := requestfilter.NewRulesVerifier(nil)
	fc := &mocks.FakeFilterConfig{}
	fc.GetMaxSizeBytesReturns(1000, nil)
	v.AddRule(requestfilter.NewMaxSizeFilter(fc))

	var wg sync.WaitGroup
	start := make(chan struct{})
	end := make(chan struct{})
	verifiers := 8

	for i := 0; i < verifiers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := v.Verify(&comm.Request{})
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
		}()
	}

	go func() {
		close(start)
		wg.Wait()
		close(end)
	}()

	select {
	case <-end:
	case <-time.After(7 * time.Second):
		t.Error("concurrent verify took too long")
	}
}

// scenario: multiple goroutines call verify and update. chech with -race flag.
func TestConcurrentUpdateAndVerify(t *testing.T) {
	v := requestfilter.NewRulesVerifier(nil)
	fc := &mocks.FakeFilterConfig{}
	fc.GetMaxSizeBytesReturns(1000, nil)
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
			fc2.GetMaxSizeBytesReturns(uint64(100+i), nil)
			<-start
			err := v.Update(fc2)
			require.NoError(t, err)
		}(i)
	}

	close(start)
	wg.Wait()
}
