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
		State:                        &State{AppContext: []byte{}},
		Num:                          100,
		DecisionNumOfLastConfigBlock: 10,
		PrevHash:                     []byte{123},
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
	require.Equal(t, hdr.PrevHash, hdr2.PrevHash)
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

	// len of bytes is just 22 which is not enough to read the available blocks
	bytes = make([]byte, 22)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint64(bytes[8:16], 10)
	binary.BigEndian.PutUint32(bytes[16:20], 0)
	binary.BigEndian.PutUint16(bytes[20:], 1)
	err = hdr.Deserialize(bytes)
	require.Error(t, err)
	t.Log(err)

	// no error (nil state)
	bytes = make([]byte, 8+8+4+4+4+len(m1))
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint64(bytes[8:16], 10)
	binary.BigEndian.PutUint32(bytes[16:20], 0)
	binary.BigEndian.PutUint32(bytes[20:], 1)
	binary.BigEndian.PutUint32(bytes[24:], uint32(len(m1)))
	copy(bytes[28:], m1)
	err = hdr.Deserialize(bytes)
	require.NoError(t, err)
	require.Nil(t, hdr.State)
	t.Log(err)
}

func TestHeaderSerializeDeserialize(t *testing.T) {
	t.Run("serialize and deserialize with full state", func(t *testing.T) {
		hdr := Header{
			State: &State{
				AppContext: []byte("test-context"),
			},
			Num:                          42,
			DecisionNumOfLastConfigBlock: 5,
			PrevHash:                     []byte{1, 2, 3, 4},
			AvailableCommonBlocks: []*common.Block{
				{Header: &common.BlockHeader{Number: 10, PreviousHash: []byte{10}, DataHash: []byte{100}}},
				{Header: &common.BlockHeader{Number: 11, PreviousHash: []byte{11}, DataHash: []byte{110}}},
			},
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)
		require.Greater(t, len(serialized), 0)

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, hdr.DecisionNumOfLastConfigBlock, hdr2.DecisionNumOfLastConfigBlock)
		require.Equal(t, hdr.PrevHash, hdr2.PrevHash)
		require.Equal(t, hdr.State.AppContext, hdr2.State.AppContext)
		require.Equal(t, len(hdr.AvailableCommonBlocks), len(hdr2.AvailableCommonBlocks))
		for i := range hdr.AvailableCommonBlocks {
			require.True(t, bytes.Equal(
				protoutil.MarshalOrPanic(hdr.AvailableCommonBlocks[i]),
				protoutil.MarshalOrPanic(hdr2.AvailableCommonBlocks[i]),
			))
		}
	})

	t.Run("serialize and deserialize with nil state", func(t *testing.T) {
		hdr := Header{
			State:                        nil,
			Num:                          100,
			DecisionNumOfLastConfigBlock: 20,
			PrevHash:                     []byte{5, 6, 7},
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, hdr.DecisionNumOfLastConfigBlock, hdr2.DecisionNumOfLastConfigBlock)
		require.Equal(t, hdr.PrevHash, hdr2.PrevHash)
		require.Nil(t, hdr2.State)
	})

	t.Run("serialize and deserialize with nil prev hash", func(t *testing.T) {
		hdr := Header{
			State: &State{
				AppContext: []byte("test-nil-prevhash"),
			},
			Num:                          150,
			DecisionNumOfLastConfigBlock: 30,
			PrevHash:                     nil,
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, hdr.DecisionNumOfLastConfigBlock, hdr2.DecisionNumOfLastConfigBlock)
		require.Nil(t, hdr2.PrevHash)
		require.Equal(t, hdr.State.AppContext, hdr2.State.AppContext)
	})

	t.Run("serialize and deserialize with empty available blocks", func(t *testing.T) {
		hdr := Header{
			State: &State{
				AppContext: []byte("empty-blocks"),
			},
			Num:                          50,
			DecisionNumOfLastConfigBlock: 10,
			PrevHash:                     []byte{8, 9},
			AvailableCommonBlocks:        []*common.Block{},
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, hdr.DecisionNumOfLastConfigBlock, hdr2.DecisionNumOfLastConfigBlock)
		require.Equal(t, hdr.PrevHash, hdr2.PrevHash)
		require.Equal(t, 0, len(hdr2.AvailableCommonBlocks))
	})

	t.Run("serialize and deserialize multiple times", func(t *testing.T) {
		hdr := Header{
			State: &State{
				AppContext: []byte("multi-test"),
			},
			Num:                          75,
			DecisionNumOfLastConfigBlock: 15,
			PrevHash:                     []byte{11, 12, 13},
		}

		serialized1 := hdr.Serialize()
		var hdr2 Header
		require.NoError(t, hdr2.Deserialize(serialized1))

		serialized2 := hdr2.Serialize()
		var hdr3 Header
		require.NoError(t, hdr3.Deserialize(serialized2))

		require.Equal(t, serialized1, serialized2)
		require.Equal(t, hdr.Num, hdr3.Num)
		require.Equal(t, hdr.DecisionNumOfLastConfigBlock, hdr3.DecisionNumOfLastConfigBlock)
		require.Equal(t, hdr.PrevHash, hdr3.PrevHash)
		require.Equal(t, hdr.State.AppContext, hdr3.State.AppContext)
	})

	t.Run("serialize with large available blocks", func(t *testing.T) {
		blocks := make([]*common.Block, 10)
		for i := 0; i < 10; i++ {
			blocks[i] = &common.Block{
				Header: &common.BlockHeader{
					Number:       uint64(i),
					PreviousHash: []byte{byte(i)},
					DataHash:     []byte{byte(i * 10)},
				},
			}
		}

		hdr := Header{
			State: &State{
				AppContext: []byte("large-blocks"),
			},
			Num:                          200,
			DecisionNumOfLastConfigBlock: 50,
			PrevHash:                     []byte{20, 21, 22},
			AvailableCommonBlocks:        blocks,
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, len(hdr.AvailableCommonBlocks), len(hdr2.AvailableCommonBlocks))
		for i := range hdr.AvailableCommonBlocks {
			require.Equal(t, hdr.AvailableCommonBlocks[i].Header.Number, hdr2.AvailableCommonBlocks[i].Header.Number)
		}
	})
}
