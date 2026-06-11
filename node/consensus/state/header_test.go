/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestHeaderSerializeDeserialize(t *testing.T) {
	t.Run("basic serialization with state", func(t *testing.T) {
		hdr := Header{
			State:                        &State{N: 4, Threshold: 2, Quorum: 3, AppContext: []byte{}},
			Num:                          100,
			DecisionNumOfLastConfigBlock: 10,
			PrevHash:                     []byte{123},
		}

		var hdr2 Header
		require.NoError(t, hdr2.Deserialize(hdr.Serialize()))
		require.Equal(t, hdr, hdr2)
	})

	t.Run("serialization with nil state", func(t *testing.T) {
		hdr := Header{
			State:                        nil,
			Num:                          100,
			DecisionNumOfLastConfigBlock: 10,
			PrevHash:                     []byte{123},
		}

		var hdr2 Header
		require.NoError(t, hdr2.Deserialize(hdr.Serialize()))
		require.Equal(t, hdr, hdr2)
	})

	t.Run("block marshaling and unmarshaling", func(t *testing.T) {
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
	})

	t.Run("serialization with available blocks", func(t *testing.T) {
		hdr := Header{
			State:                        nil,
			Num:                          100,
			DecisionNumOfLastConfigBlock: 10,
			PrevHash:                     []byte{123},
			AvailableCommonBlocks:        []*common.Block{{Header: &common.BlockHeader{Number: 1}}, {Header: &common.BlockHeader{Number: 2}}},
		}

		var hdr2 Header
		require.NoError(t, hdr2.Deserialize(hdr.Serialize()))

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, hdr.State, hdr2.State)
		require.Equal(t, hdr.PrevHash, hdr2.PrevHash)
		require.Equal(t, hdr.AvailableCommonBlocks[0].Header.Number, hdr2.AvailableCommonBlocks[0].Header.Number)
		require.Equal(t, hdr.AvailableCommonBlocks[1].Header.Number, hdr2.AvailableCommonBlocks[1].Header.Number)
		require.True(t, bytes.Equal(protoutil.MarshalOrPanic(hdr.AvailableCommonBlocks[0]), protoutil.MarshalOrPanic(hdr2.AvailableCommonBlocks[0])))
		require.True(t, bytes.Equal(protoutil.MarshalOrPanic(hdr.AvailableCommonBlocks[1]), protoutil.MarshalOrPanic(hdr2.AvailableCommonBlocks[1])))
		require.Equal(t, hdr.Serialize(), hdr2.Serialize())
	})

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

	t.Run("deserialize error cases", func(t *testing.T) {
		var hdr Header

		// Test nil bytes
		err := hdr.Deserialize(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nil bytes")

		// Test invalid proto bytes
		err = hdr.Deserialize([]byte{1, 2, 3, 4, 5})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to unmarshal header")

		// Test short invalid bytes
		err = hdr.Deserialize([]byte{1, 2, 3})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to unmarshal header")
	})

	t.Run("serialize deterministic", func(t *testing.T) {
		hdr := Header{
			State: &State{
				N:          4,
				Threshold:  2,
				Quorum:     3,
				Shards:     []ShardTerm{{Shard: 1, Term: 1}},
				AppContext: []byte("test"),
			},
			Num:                          42,
			DecisionNumOfLastConfigBlock: 5,
			PrevHash:                     []byte{1, 2, 3, 4},
			AvailableCommonBlocks: []*common.Block{
				{Header: &common.BlockHeader{Number: 10}},
			},
		}

		// Serialize multiple times and verify deterministic output
		serialized1 := hdr.Serialize()
		serialized2 := hdr.Serialize()
		serialized3 := hdr.Serialize()

		require.Equal(t, serialized1, serialized2)
		require.Equal(t, serialized2, serialized3)
	})

	t.Run("empty and nil fields handling", func(t *testing.T) {
		testCases := []struct {
			name   string
			header Header
		}{
			{
				name: "all nil",
				header: Header{
					Num:                          1,
					DecisionNumOfLastConfigBlock: 1,
					PrevHash:                     nil,
					AvailableCommonBlocks:        nil,
					State:                        nil,
				},
			},
			{
				name: "empty slices",
				header: Header{
					Num:                          1,
					DecisionNumOfLastConfigBlock: 1,
					PrevHash:                     []byte{},
					AvailableCommonBlocks:        []*common.Block{},
					State:                        nil,
				},
			},
			{
				name: "state with empty app context",
				header: Header{
					Num:   10,
					State: &State{AppContext: []byte{}},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				serialized := tc.header.Serialize()
				require.NotNil(t, serialized)

				var hdr2 Header
				err := hdr2.Deserialize(serialized)
				require.NoError(t, err)

				// Verify basic fields
				require.Equal(t, tc.header.Num, hdr2.Num)
				require.Equal(t, tc.header.DecisionNumOfLastConfigBlock, hdr2.DecisionNumOfLastConfigBlock)
			})
		}
	})

	t.Run("large header with many blocks", func(t *testing.T) {
		blocks := make([]*common.Block, 100)
		for i := 0; i < 100; i++ {
			blocks[i] = &common.Block{
				Header: &common.BlockHeader{
					Number:       uint64(i),
					PreviousHash: []byte{byte(i)},
					DataHash:     []byte{byte(i * 2)},
				},
			}
		}

		hdr := Header{
			State: &State{
				N:          10,
				Threshold:  4,
				Quorum:     7,
				Shards:     []ShardTerm{{Shard: 1, Term: 1}, {Shard: 2, Term: 2}},
				AppContext: make([]byte, 1024), // 1KB
			},
			Num:                          1000,
			DecisionNumOfLastConfigBlock: 500,
			PrevHash:                     make([]byte, 32), // 32 byte hash
			AvailableCommonBlocks:        blocks,
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)
		require.Greater(t, len(serialized), 1000) // Should be fairly large

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, hdr.DecisionNumOfLastConfigBlock, hdr2.DecisionNumOfLastConfigBlock)
		require.Equal(t, len(hdr.AvailableCommonBlocks), len(hdr2.AvailableCommonBlocks))
		require.Equal(t, hdr.State.N, hdr2.State.N)
	})

	t.Run("state with complaints and pending", func(t *testing.T) {
		hdr := Header{
			State: &State{
				N:         4,
				Threshold: 2,
				Quorum:    3,
				Shards:    []ShardTerm{{Shard: 1, Term: 1}},
				Complaints: []Complaint{
					{
						ShardTerm: ShardTerm{Shard: 1, Term: 1},
						Signer:    2,
						Signature: []byte{1, 2, 3, 4},
						Reason:    "test complaint",
						ConfigSeq: 10,
					},
				},
				AppContext: []byte("with-complaints"),
			},
			Num:                          200,
			DecisionNumOfLastConfigBlock: 100,
			PrevHash:                     []byte{10, 20, 30},
		}

		serialized := hdr.Serialize()
		require.NotNil(t, serialized)

		var hdr2 Header
		err := hdr2.Deserialize(serialized)
		require.NoError(t, err)

		require.Equal(t, hdr.Num, hdr2.Num)
		require.Equal(t, len(hdr.State.Complaints), len(hdr2.State.Complaints))
		if len(hdr2.State.Complaints) > 0 {
			require.Equal(t, hdr.State.Complaints[0].Signer, hdr2.State.Complaints[0].Signer)
			require.Equal(t, hdr.State.Complaints[0].Reason, hdr2.State.Complaints[0].Reason)
		}
	})
}
