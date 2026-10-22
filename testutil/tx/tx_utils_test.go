/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPrepareUnsignedEnvelopeSize(t *testing.T) {
	sessionNumber := make([]byte, 16)
	minSize := proto.Size(PrepareUnsignedEnvelope(0, 0, sessionNumber))

	for envSize := minSize; envSize <= 20000; envSize++ {
		size := proto.Size(PrepareUnsignedEnvelope(envSize, envSize, sessionNumber))
		// a size that would need a varint length to grow by a byte cannot be met exactly, and is one byte short.
		require.True(t, size == envSize || size == envSize-1, "requested %d, got %d", envSize, size)
	}
}

func TestStructuredEnvelopeTxID(t *testing.T) {
	for _, env := range []*common.Envelope{
		CreateStructuredEnvelope([]byte{1}),
		CreateStructuredConfigUpdateEnvelope([]byte{1}),
		CreateStructuredConfigEnvelope([]byte{1}),
		{Payload: CreateStructuredRequest([]byte{1}).Payload},
		{Payload: CreateStructuredConfigUpdateRequest([]byte{1}).Payload},
	} {
		payload, err := protoutil.UnmarshalPayload(env.Payload)
		require.NoError(t, err)
		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		require.NoError(t, err)
		shdr, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
		require.NoError(t, err)
		require.NoError(t, protoutil.CheckTxID(chdr.TxId, shdr.Nonce, shdr.Creator))
	}
}
