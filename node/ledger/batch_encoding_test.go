/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardPartyChannelIDToChannelName(t *testing.T) {
	require.Equal(t, "shard2party3-arma", ledger.ShardPartyChannelIDToChannelName(2, 3, "arma"))
	require.Equal(t, "shard2party3-my-channel", ledger.ShardPartyChannelIDToChannelName(2, 3, "my-channel"))
}

func TestChannelNameToShardPartyChannelID(t *testing.T) {
	t.Run("good", func(t *testing.T) {
		shardID, partyID, channelID, err := ledger.ChannelNameToShardPartyChannelID("shard2party3-arma")
		require.NoError(t, err)
		require.Equal(t, types.ShardID(2), shardID)
		require.Equal(t, types.PartyID(3), partyID)
		require.Equal(t, "arma", channelID)
	})

	t.Run("good with dashes in channelID", func(t *testing.T) {
		shardID, partyID, channelID, err := ledger.ChannelNameToShardPartyChannelID("shard2party3-my-channel")
		require.NoError(t, err)
		require.Equal(t, types.ShardID(2), shardID)
		require.Equal(t, types.PartyID(3), partyID)
		require.Equal(t, "my-channel", channelID)
	})

	t.Run("round trip", func(t *testing.T) {
		name := ledger.ShardPartyChannelIDToChannelName(7, 4, "arma")
		shardID, partyID, channelID, err := ledger.ChannelNameToShardPartyChannelID(name)
		require.NoError(t, err)
		require.Equal(t, types.ShardID(7), shardID)
		require.Equal(t, types.PartyID(4), partyID)
		require.Equal(t, "arma", channelID)
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
				in:     "shard2.5party3-arma",
				expErr: "cannot extract 'shardID' from channel name: shard2.5party3-arma, err: strconv.ParseUint: parsing \"2.5\": invalid syntax",
			},
			{
				in:     "shard-1party3-arma",
				expErr: "cannot extract 'shardID' from channel name: shard-1party3-arma, err: strconv.ParseUint: parsing \"-1\": invalid syntax",
			},
			{
				in:     "shard65536party3-arma",
				expErr: "cannot extract 'shardID' from channel name: shard65536party3-arma, err: strconv.ParseUint: parsing \"65536\": value out of range",
			},
			{
				in:     "shard2party3",
				expErr: "channel name does not contain the '-<channelID>' suffix: shard2party3",
			},
			{
				in:     "shard2party",
				expErr: "channel name does not contain the '-<channelID>' suffix: shard2party",
			},
			{
				in:     "shard2party3x-arma",
				expErr: "cannot extract 'partyID' from channel name: shard2party3x-arma, err: strconv.ParseUint: parsing \"3x\": invalid syntax",
			},
			{
				in:     "shard2party-arma",
				expErr: "cannot extract 'partyID' from channel name: shard2party-arma, err: strconv.ParseUint: parsing \"\": invalid syntax",
			},
			{
				in:     "shard2party65536-arma",
				expErr: "cannot extract 'partyID' from channel name: shard2party65536-arma, err: strconv.ParseUint: parsing \"65536\": value out of range",
			},
			{
				in:     "shard2party3-",
				expErr: "cannot extract 'channelID' from channel name: shard2party3-, err: channel ID illegal, cannot be empty",
			},
			{
				in:     "shard2party3-Arma",
				expErr: "cannot extract 'channelID' from channel name: shard2party3-Arma, err: 'Arma' contains illegal characters",
			},
			{
				in:     "shard2party3-9arma",
				expErr: "cannot extract 'channelID' from channel name: shard2party3-9arma, err: '9arma' contains illegal characters",
			},
		} {
			shardID, partyID, channelID, err := ledger.ChannelNameToShardPartyChannelID(tc.in)
			require.EqualError(t, err, tc.expErr)
			require.Equal(t, types.ShardID(0), shardID)
			require.Equal(t, types.PartyID(0), partyID)
			require.Equal(t, "", channelID)
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
			name: "missing orderer metadata",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}}},
			},
			expectedErr: "missing orderer metadata",
		},
		{
			name: "bad orderer metadata",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}, {}}},
			},
			expectedErr: "bad orderer metadata",
		},
		{
			name: "bad orderer metadata 2",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {0x01}, {}, {}}},
			},
			expectedErr: "bad orderer metadata",
		},
		{
			name: "good",
			block: &common.Block{
				Header:   header,
				Data:     data,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}, {}}},
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
				assert.Equal(t, types.ShardID(0x102), fb.Shard())
				assert.Equal(t, types.PartyID(0x304), fb.Primary())
				assert.Equal(t, types.ConfigSequence(0x05), fb.ConfigSequence())
				assert.Len(t, fb.Requests(), 2)
				assert.Equal(t, header.DataHash, fb.Digest())
			}
		})
	}
}

func TestNewFabricBatchFromRequests(t *testing.T) {
	bReqs := types.BatchedRequests([][]byte{{0x08}, {0x09}})
	primarySig := []byte{0x01, 0x02, 0x03}
	fb := ledger.NewFabricBatchFromRequests(2, 3, 4, bReqs, 5, []byte{0x06}, primarySig)
	require.NotNil(t, fb)
	require.Equal(t, types.ShardID(2), fb.Shard())
	require.Equal(t, types.PartyID(3), fb.Primary())
	require.Equal(t, types.BatchSequence(4), fb.Seq())
	require.True(t, bytes.Equal(bReqs.Digest(), fb.Digest()))
	require.Equal(t, types.ConfigSequence(5), fb.ConfigSequence())
	require.Equal(t, bReqs, fb.Requests())
	require.True(t, bytes.Equal(primarySig, fb.PrimarySignature()))

	require.True(t, bytes.Equal([]byte{0x06}, fb.Header.GetPreviousHash()))

	require.Equal(t, "Sh,Pr,Sq,Dg: <2,3,4,f99be8ba3f263229e64cd89aded97556d208a7650bfd06be5979fbf748f94cbe>", types.BatchIDToString(fb))
	require.True(t, types.BatchIDEqual(fb, state.NewAvailableBatch(3, 2, 4, bReqs.Digest())))
}
