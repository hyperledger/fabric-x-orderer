/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// check that a protos.request and comm.envelope are always compatible: the payload and signature fields are consistent.
func TestCompatibilityOfRequestAndEnvelope(t *testing.T) {
	payload := []byte("My Message 123123")
	signature := []byte("3021020e54180cf5bcbb6133e32ac30f967a020f00c1275eb696184dbce7835413ad7f")
	identity := []byte("My Identity")
	identityid := uint32(7)
	traceID := []byte("trace")

	t.Run("Request To Envelope", func(t *testing.T) {
		req := &protos.Request{Payload: payload, Signature: signature, Identity: identity, IdentityId: identityid, TraceId: traceID}
		data, err := proto.Marshal(req)
		require.NoError(t, err)
		env := &common.Envelope{}
		err = proto.Unmarshal(data, env)
		require.NoError(t, err)
		require.True(t, bytes.Equal(env.Payload, payload))
		require.True(t, bytes.Equal(env.Signature, signature))
	})

	t.Run("Envelope To Request", func(t *testing.T) {
		env := &common.Envelope{Payload: payload, Signature: signature}
		data, err := proto.Marshal(env)
		require.NoError(t, err)
		req := &protos.Request{}
		err = proto.Unmarshal(data, req)
		require.NoError(t, err)
		require.True(t, bytes.Equal(req.Payload, payload))
		require.True(t, bytes.Equal(req.Signature, signature))
	})
}
