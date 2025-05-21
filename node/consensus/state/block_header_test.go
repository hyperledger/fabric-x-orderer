/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockHeader_Serialize(t *testing.T) {
	t.Run("nil hash", func(t *testing.T) {
		var bh BlockHeader
		var bh2 BlockHeader
		require.NoError(t, bh2.FromBytes(bh.Bytes()))
		require.Equal(t, bh, bh2)
		require.Equal(t, bh.Hash(), bh2.Hash())
		require.True(t, bh.Equal(&bh2))
		require.True(t, bh2.Equal(&bh))
	})

	t.Run("non nil hash", func(t *testing.T) {
		var bh BlockHeader
		bh.Number = 0
		bh.PrevHash = make([]byte, 32)
		bh.Digest = make([]byte, 32)

		var bh2 BlockHeader
		bh2.deserialize(bh.serialize())

		require.Equal(t, bh, bh2)

		require.Equal(t, bh.Hash(), bh2.Hash())

		require.True(t, bh.Equal(&bh2))
		require.True(t, bh2.Equal(&bh))

		var bh3 BlockHeader
		require.NoError(t, bh3.FromBytes(bh.Bytes()))

		require.Equal(t, bh, bh3)

		require.Equal(t, bh.Hash(), bh3.Hash())

		require.True(t, bh.Equal(&bh3))
		require.True(t, bh3.Equal(&bh))
	})
}
