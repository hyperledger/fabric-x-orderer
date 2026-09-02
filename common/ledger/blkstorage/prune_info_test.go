/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// Scenario:
// 1. Marshal a prune info record.
// 2. Unmarshal the bytes into a fresh record.
// 3. Expect it to equal the original, for zero, small and large field values.
func TestPruneInfoMarshalUnmarshal(t *testing.T) {
	for _, info := range []*pruneInfo{
		{firstReadableBlockNum: 0, firstStoredBlockfileNum: 0},
		{firstReadableBlockNum: 1, firstStoredBlockfileNum: 1},
		{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2},
		{firstReadableBlockNum: 1 << 40, firstStoredBlockfileNum: 1 << 20},
	} {
		t.Run(info.String(), func(t *testing.T) {
			decoded := &pruneInfo{}
			require.NoError(t, decoded.unmarshal(info.marshal()))
			require.Equal(t, info, decoded)
		})
	}
}

// Scenario:
//  1. Unmarshal a record carrying only the first field, as one written before the second field existed
//     would, and expect the first field to be read and the second to be zero.
//  2. Unmarshal an empty record and a record whose varint claims more continuation bytes than are present,
//     and expect both to fail.
func TestPruneInfoUnmarshalIncompleteRecord(t *testing.T) {
	t.Run("a field missing from the end reads as zero", func(t *testing.T) {
		decoded := &pruneInfo{}
		require.NoError(t, decoded.unmarshal(protowire.AppendVarint(nil, 7)))
		require.Equal(t, &pruneInfo{firstReadableBlockNum: 7, firstStoredBlockfileNum: 0}, decoded)
	})

	t.Run("a truncated field is an error", func(t *testing.T) {
		for _, b := range [][]byte{{}, {0xff}, append(protowire.AppendVarint(nil, 7), 0xff)} {
			require.Error(t, (&pruneInfo{}).unmarshal(b))
		}
	})
}
