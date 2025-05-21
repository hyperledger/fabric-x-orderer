/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailableBlockBytes(t *testing.T) {
	block := AvailableBlock{
		Header: &BlockHeader{10, make([]byte, 32), make([]byte, 2)},
		Batch:  NewAvailableBatch(3, 2, 1, make([]byte, 3)),
	}
	require.Panics(t, func() { block.Serialize() }) // digests are not equal

	block = AvailableBlock{
		Header: &BlockHeader{0, make([]byte, 32), make([]byte, 32)},
		Batch:  NewAvailableBatch(0, 0, 0, make([]byte, 32)),
	}
	block2 := AvailableBlock{}
	require.NoError(t, block2.Deserialize(block.Serialize()))
	require.Equal(t, block, block2)

	block = AvailableBlock{
		Header: &BlockHeader{10, make([]byte, 32), make([]byte, 32)},
		Batch:  NewAvailableBatch(3, 2, 1, make([]byte, 32)),
	}
	block2 = AvailableBlock{}
	require.NoError(t, block2.Deserialize(block.Serialize()))
	require.Equal(t, block, block2)

	err := block.Deserialize(nil)
	require.Error(t, err)
	t.Log(err)

	err = block.Deserialize([]byte{})
	require.Error(t, err)
	t.Log(err)

	require.NoError(t, block.Deserialize(make([]byte, availableBatchSerializedSize+blockHeaderBytesSize)))
}
