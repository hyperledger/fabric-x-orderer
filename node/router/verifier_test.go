/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"encoding/binary"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
)

func TestNonEmptyFilter(t *testing.T) {
	var v Verifier
	v.AddRule(PayloadNotEmptyRule)
	request := &comm.Request{Payload: nil}
	err := v.Verify(request)
	require.EqualError(t, err, "empty payload field")

	buff := make([]byte, 300)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	request = &comm.Request{Payload: buff}
	err = v.Verify(request)
	require.NoError(t, err)
}

func TestAcceptFilter(t *testing.T) {
	var v Verifier
	v.AddRule(AcceptRule)
	request := &comm.Request{Payload: nil}
	err := v.Verify(request)
	require.NoError(t, err)
}

func TestMaxSizeFilter(t *testing.T) {
	var v Verifier
	var tfs testfiltersupport
	v.AddRule(NewMaxSizeRule(&tfs))

	buff := make([]byte, 3000)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	request := &comm.Request{Payload: buff}
	err := v.Verify(request)
	require.EqualError(t, err, "the request's size exceeds the maximum size")

	buff = make([]byte, 300)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	request = &comm.Request{Payload: buff}
	err = v.Verify(request)
	require.NoError(t, err)
}

func TestSigVerifyFilter(t *testing.T) {
	// signature verification is not implemented yet
	t.Skip()
	var v Verifier
	var tfs testfiltersupport
	v.AddRule(NewSigVerifier(&tfs))
}

type testfiltersupport struct{}

func (tfs *testfiltersupport) GetMaxSizeBytes() (uint64, error) {
	return 1 << 10, nil
}
