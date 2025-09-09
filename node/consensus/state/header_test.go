/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestHeaderBytes(t *testing.T) {
	hdr := Header{
		State: &State{AppContext: []byte{}},
		Num:   100,
		AvailableBlocks: []AvailableBlock{
			{Header: &BlockHeader{10, make([]byte, 32), make([]byte, 32)}, Batch: NewAvailableBatch(3, 2, 1, make([]byte, 32))},
			{Header: &BlockHeader{11, make([]byte, 32), make([]byte, 32)}, Batch: NewAvailableBatch(6, 5, 4, make([]byte, 32))},
		},
	}

	var hdr2 Header
	require.NoError(t, hdr2.Deserialize(hdr.Serialize()))

	require.Equal(t, hdr, hdr2)

	hdr.State = nil
	require.NoError(t, hdr2.Deserialize(hdr.Serialize()))

	require.Equal(t, hdr, hdr2)

	block1 := &common.Block{Header: &common.BlockHeader{Number: 1, PreviousHash: []byte{1}, DataHash: []byte{11}}}
	block2 := &common.Block{Header: &common.BlockHeader{Number: 2, PreviousHash: []byte{2}, DataHash: []byte{22}}}

	m1, err := proto.Marshal(block1)
	require.NoError(t, err)
	m2, err := proto.Marshal(block2)
	require.NoError(t, err)

	b1 := &common.Block{}
	b2 := &common.Block{}

	require.NoError(t, proto.Unmarshal(m1, b1))
	require.NoError(t, proto.Unmarshal(m2, b2))

	require.Equal(t, uint64(1), b1.Header.Number)
	require.Equal(t, []byte{1}, b1.Header.PreviousHash)
	require.Equal(t, []byte{11}, b1.Header.DataHash)
	require.Equal(t, uint64(2), b2.Header.Number)
	require.Equal(t, []byte{2}, b2.Header.PreviousHash)
	require.Equal(t, []byte{22}, b2.Header.DataHash)

	bm1, err := proto.Marshal(b1)
	require.NoError(t, err)
	bm2, err := proto.Marshal(b2)
	require.NoError(t, err)

	require.Equal(t, m1, bm1)
	require.Equal(t, m2, bm2)
	require.NotEqual(t, bm1, bm2)

	hdr.AvailableCommonBlocks = []*common.Block{{Header: &common.BlockHeader{Number: 1}}, {Header: &common.BlockHeader{Number: 2}}}
	require.NoError(t, hdr2.Deserialize(hdr.Serialize()))

	require.Equal(t, hdr.Num, hdr2.Num)
	require.Equal(t, hdr.State, hdr2.State)
	require.Equal(t, hdr.AvailableBlocks, hdr2.AvailableBlocks)
	require.Equal(t, hdr.AvailableCommonBlocks[0].Header.Number, hdr2.AvailableCommonBlocks[0].Header.Number)
	require.Equal(t, hdr.AvailableCommonBlocks[1].Header.Number, hdr2.AvailableCommonBlocks[1].Header.Number)
	require.True(t, bytes.Equal(protoutil.MarshalOrPanic(hdr.AvailableCommonBlocks[0]), protoutil.MarshalOrPanic(hdr2.AvailableCommonBlocks[0])))
	require.True(t, bytes.Equal(protoutil.MarshalOrPanic(hdr.AvailableCommonBlocks[1]), protoutil.MarshalOrPanic(hdr2.AvailableCommonBlocks[1])))

	require.Equal(t, hdr.Serialize(), hdr2.Serialize())

	err = hdr.Deserialize([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7})
	require.Error(t, err)
	t.Log(err)

	// len of bytes is just 8 which is not enough
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, 100)
	err = hdr.Deserialize(bytes)
	require.Error(t, err)
	t.Log(err)

	// len of bytes is just 12 which is not enough to read the available blocks
	bytes = make([]byte, 12)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	err = hdr.Deserialize(bytes)
	require.Error(t, err)
	t.Log(err)

	// no error (nil state)
	bytes = make([]byte, 8+4+availableBlockSerializedSize+4)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	ab := AvailableBlock{Header: &BlockHeader{10, make([]byte, 32), make([]byte, 32)}, Batch: NewAvailableBatch(3, 2, 1, make([]byte, 32))}
	copy(bytes[12:], ab.Serialize())
	binary.BigEndian.PutUint32(bytes[12+availableBlockSerializedSize:], 0)
	err = hdr.Deserialize(bytes)
	require.NoError(t, err)
	require.Nil(t, hdr.State)
	t.Log(err)
}
