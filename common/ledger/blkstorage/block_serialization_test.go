/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
)

func TestBlockSerialization(t *testing.T) {
	block := testutil.ConstructTestBlock(t, 1, 10, 100)

	// malformed Payload
	block.Data.Data[1] = protoutil.MarshalOrPanic(&common.Envelope{
		Signature: []byte{1, 2, 3},
		Payload:   []byte("Malformed Payload"),
	})

	// empty TxID
	block.Data.Data[2] = protoutil.MarshalOrPanic(&common.Envelope{
		Signature: []byte{1, 2, 3},
		Payload: protoutil.MarshalOrPanic(&common.Payload{
			Header: &common.Header{
				ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
					TxId: "",
				}),
			},
		}),
	})

	bb := serializeBlock(block)
	deserializedBlock, err := deserializeBlock(bb)
	require.NoError(t, err)
	require.Equal(t, block, deserializedBlock)
}

func Test_SerializeBlockSpeed(t *testing.T) {
	nTXs := 1000
	txSize := 256
	N := int64(100)

	t.Logf("No.-TXs=%d, TX-size=%d; ", nTXs, txSize)

	t.Run("serialize", func(t *testing.T) {
		block := testutil.ConstructTestBlock(t, 0, nTXs, txSize)
		t1 := time.Now()

		for i := int64(0); i < N; i++ {
			block.Header.Number = uint64(i)
			bb := serializeBlock(block)
			require.NotNil(t, bb)
		}
		d := time.Duration(time.Since(t1).Nanoseconds() / N)
		t.Logf("Duration: %s; serializeBlock", d)
	})

	t.Run("deserialize", func(t *testing.T) {
		block := testutil.ConstructTestBlock(t, 0, nTXs, txSize)

		bb := serializeBlock(block)
		t1 := time.Now()

		for i := int64(0); i < N; i++ {
			b1, err := deserializeBlock(bb)
			require.NoError(t, err)
			require.NotNil(t, b1)
		}
		d := time.Duration(time.Since(t1).Nanoseconds() / N)
		t.Logf("Duration: %s; deserializeBlock ", d)
	})

	t.Run("extract header", func(t *testing.T) {
		block := testutil.ConstructTestBlock(t, 0, nTXs, txSize)

		bb := serializeBlock(block)
		t1 := time.Now()

		for i := int64(0); i < N; i++ {
			b1, err := extractSerializedBlockHeader(bb)
			require.NoError(t, err)
			require.NotNil(t, b1)
		}
		d := time.Duration(time.Since(t1).Nanoseconds() / N)
		t.Logf("Duration: %s; extractSerializedBlockHeader ", d)
	})
}
