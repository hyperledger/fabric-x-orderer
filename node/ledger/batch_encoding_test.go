/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/assert"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/stretchr/testify/require"
)

func TestShardPartyToChannelName(t *testing.T) {
	s := ledger.ShardPartyToChannelName(2, 3)
	require.Equal(t, "shard2party3", s)
}

func TestChannelNameToShardParty(t *testing.T) {
	t.Run("good", func(t *testing.T) {
		shardID, partyID, err := ledger.ChannelNameToShardParty("shard2party3")
		require.NoError(t, err)
		require.Equal(t, types.ShardID(2), shardID)
		require.Equal(t, types.PartyID(3), partyID)
	})

	t.Run("bad", func(t *testing.T) {
		type testCase struct {
			in     string
			expErr string
		}

		for _, tc := range []testCase{
			{
				in:     "shard",
				expErr: "channel name does not contain 'party': shard",
			},
			{
				in:     "shard2",
				expErr: "channel name does not contain 'party': shard2",
			},
			{
				in:     "shard2x",
				expErr: "channel name does not contain 'party': shard2x",
			},
			{
				in:     "shard2party",
				expErr: "cannot extract 'partyID' from channel name: shard2party, err: strconv.Atoi: parsing \"\": invalid syntax",
			},
			{
				in:     "shard2party3x",
				expErr: "cannot extract 'partyID' from channel name: shard2party3x, err: strconv.Atoi: parsing \"3x\": invalid syntax",
			},
			{
				in:     "shard2party3.5",
				expErr: "cannot extract 'partyID' from channel name: shard2party3.5, err: strconv.Atoi: parsing \"3.5\": invalid syntax",
			},
			{
				in:     "shard2.5party3",
				expErr: "cannot extract 'shardID' from channel name: shard2.5party3, err: strconv.Atoi: parsing \"2.5\": invalid syntax",
			},
		} {
			shardID, partyID, err := ledger.ChannelNameToShardParty(tc.in)
			require.EqualError(t, err, tc.expErr)
			require.Equal(t, types.ShardID(0), shardID)
			require.Equal(t, types.PartyID(0), partyID)
		}
	})
}

func TestNewFabricBatchFromBlock(t *testing.T) {
	type testCase struct {
		name        string
		block       *common.Block
		expectedErr string
	}
	header := &common.BlockHeader{
		Number:       7,
		PreviousHash: []byte{1, 2, 3, 4},
		DataHash:     []byte{5, 6, 7, 8},
	}
	data := &common.BlockData{
		Data: [][]byte{{1, 2}, {3, 4}},
	}

	for _, tc := range []testCase{
		{
			name:        "empty block",
			block:       nil,
			expectedErr: "empty block",
		},
		{
			name:        "empty block header",
			block:       &common.Block{},
			expectedErr: "empty block header",
		},
		{
			name: "empty block data",
			block: &common.Block{
				Header:   header,
				Data:     nil,
				Metadata: nil,
			},
			expectedErr: "empty block data",
		},
		{
			name: "empty block metadata",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: nil,
			},
			expectedErr: "empty block metadata",
		},
		{
			name: "missing shard party metadata 2",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}},
			},
			expectedErr: "missing shard party metadata",
		},
		{
			name: "bad shard party metadata",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}, {}}},
			},
			expectedErr: "bad shard party metadata",
		},
		{
			name: "good",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}, {0x01, 0x02, 0x03, 0x04}}},
			},
			expectedErr: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fb, err := ledger.NewFabricBatchFromBlock(tc.block)
			if len(tc.expectedErr) != 0 {
				assert.Nil(t, fb)
				assert.EqualError(t, err, tc.expectedErr)
			} else {
				assert.NotNil(t, fb)
				assert.NoError(t, err)
				assert.Equal(t, types.BatchSequence(7), fb.Seq())
				assert.Equal(t, types.PartyID(0x102), fb.Primary())
				assert.Equal(t, types.ShardID(0x304), fb.Shard())
				assert.Len(t, fb.Requests(), 2)
				assert.Equal(t, header.DataHash, fb.Digest())
			}
		})
	}
}
