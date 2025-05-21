/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucketBasic(t *testing.T) {
	m := new(sync.Map)
	b := newBucket(m, 1)
	require.Zero(t, b.getSize())
	require.False(t, b.isDummyBucket())
	require.True(t, b.tryInsert("1", []byte{1}))
	require.Equal(t, uint32(1), b.size)
	require.True(t, b.tryInsert("1", []byte{1}))
	require.Equal(t, uint32(1), b.size)
	require.True(t, b.tryInsert("2", []byte{2}))
	require.Equal(t, uint32(2), b.size)
	require.False(t, b.delete("3"))
	require.Equal(t, uint32(2), b.size)
	require.True(t, b.delete("2"))
	require.Equal(t, uint32(1), b.size)

	require.Zero(t, b.getFirstStrikeTimestamp())
	b.setFirstStrikeTimestamp(time.Now())
	require.NotZero(t, b.getFirstStrikeTimestamp())
	b.resetTimestamp(time.Now())
	require.Zero(t, b.getFirstStrikeTimestamp())

	b2 := b.seal(time.Now())
	require.False(t, b.isDummyBucket())
	require.Zero(t, b2.getSize())
	require.True(t, b2.tryInsert("1", []byte{1}))
	require.Zero(t, b2.getSize())
	require.True(t, b2.tryInsert("2", []byte{2}))
	require.Equal(t, uint32(1), b2.size)
}
