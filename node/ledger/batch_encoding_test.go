package ledger_test

import (
	"testing"

	"arma/common/types"
	"arma/node/ledger"

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
