/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"encoding/binary"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
)

func TestNonEmptyFilter(t *testing.T) {
	var v requestfilter.RulesVerifier
	v.AddRule(requestfilter.PayloadNotEmptyRule{})
	request := &comm.Request{Payload: nil}
	_, err := v.Verify(request)
	require.EqualError(t, err, "empty payload field")

	buff := make([]byte, 300)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	request = &comm.Request{Payload: buff}
	_, err = v.Verify(request)
	require.NoError(t, err)
}

func TestMaxSizeFilter(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	fc.GetRequestMaxBytesReturns(1000)
	v.AddRule(requestfilter.NewMaxSizeFilter(fc))

	t.Run("Filter Test", func(t *testing.T) {
		buff := make([]byte, 3000)
		binary.BigEndian.PutUint32(buff, uint32(12345))
		request := &comm.Request{Payload: buff}
		_, err := v.Verify(request)
		require.EqualError(t, err, "the request's size exceeds the maximum size: actual = 3000, limit = 1000")

		buff = make([]byte, 300)
		binary.BigEndian.PutUint32(buff, uint32(12345))
		request = &comm.Request{Payload: buff}
		_, err = v.Verify(request)
		require.NoError(t, err)
	})

	t.Run("Update Size Test", func(t *testing.T) {
		request := &comm.Request{Payload: make([]byte, 300)}
		_, err := v.Verify(request)
		require.NoError(t, err)

		fc2 := &mocks.FakeFilterConfig{}
		fc2.GetRequestMaxBytesReturns(100)
		v.Update(fc2)
		_, err = v.Verify(request)
		require.EqualError(t, err, "the request's size exceeds the maximum size: actual = 300, limit = 100")
	})
}
