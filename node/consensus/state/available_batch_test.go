/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"math"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"

	"github.com/stretchr/testify/require"
)

func TestAvailableBatchAccessors(t *testing.T) {
	digest := make([]byte, availableBatchDigestSize)
	digest[0] = 0xAB
	ab := NewAvailableBatch(42, 666, 100, digest)

	require.Equal(t, types.PartyID(42), ab.Primary())
	require.Equal(t, types.ShardID(666), ab.Shard())
	require.Equal(t, types.BatchSequence(100), ab.Seq())
	require.Equal(t, digest, ab.Digest())
}

func TestAvailableBatchString(t *testing.T) {
	var ab AvailableBatch
	ab.digest = make([]byte, availableBatchDigestSize)
	ab.primary = 42
	ab.shard = 666
	ab.seq = 100
	require.Equal(t,
		"Sh,Pr,Sq,Dg: <666,42,100,0000000000000000000000000000000000000000000000000000000000000000>",
		ab.String())
}

func TestAvailableBatchSerializeRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		primary types.PartyID
		shard   types.ShardID
		seq     types.BatchSequence
		digest  []byte
	}{
		{
			name:    "typical values",
			primary: 42,
			shard:   666,
			seq:     100,
			digest:  make([]byte, availableBatchDigestSize),
		},
		{
			name:    "zero values",
			primary: 0,
			shard:   0,
			seq:     0,
			digest:  make([]byte, availableBatchDigestSize),
		},
		{
			name:    "max values",
			primary: math.MaxUint16,
			shard:   math.MaxUint16,
			seq:     math.MaxUint64,
			digest:  bytesOf(0xFF, availableBatchDigestSize),
		},
		{
			name:    "non-uniform digest",
			primary: 1,
			shard:   2,
			seq:     3,
			digest:  incrementingDigest(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ab := NewAvailableBatch(tt.primary, tt.shard, tt.seq, tt.digest)

			raw := ab.Serialize()
			require.Len(t, raw, availableBatchSerializedSize)

			var got AvailableBatch
			require.NoError(t, got.Deserialize(raw))
			require.Equal(t, *ab, got)
		})
	}
}

func TestAvailableBatchDeserializeInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{name: "nil bytes", input: nil},
		{name: "empty bytes", input: []byte{}},
		{name: "too short", input: make([]byte, availableBatchSerializedSize-1)},
		{name: "too long", input: make([]byte, availableBatchSerializedSize+1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ab AvailableBatch
			require.Error(t, ab.Deserialize(tt.input))
		})
	}
}

// TestAvailableBatchDeserializeDoesNotAliasInput ensures the deserialized digest is an
// independent copy, so mutating the caller's buffer afterwards cannot corrupt the batch.
func TestAvailableBatchDeserializeDoesNotAliasInput(t *testing.T) {
	src := NewAvailableBatch(1, 2, 3, incrementingDigest())
	raw := src.Serialize()

	var ab AvailableBatch
	require.NoError(t, ab.Deserialize(raw))

	digestBefore := append([]byte(nil), ab.Digest()...)

	// Mutate the input buffer after deserialization.
	for i := range raw {
		raw[i] ^= 0xFF
	}

	require.Equal(t, digestBefore, ab.Digest(), "digest must not alias the input buffer")
}

// TestAvailableBatchSerializeWrongDigestSize documents the current (silent) handling of a
// digest whose length differs from availableBatchDigestSize: the output is always fixed size,
// truncating a longer digest and zero-padding a shorter one.
func TestAvailableBatchSerializeWrongDigestSize(t *testing.T) {
	t.Run("short digest is zero-padded", func(t *testing.T) {
		ab := NewAvailableBatch(1, 2, 3, []byte{0xAA, 0xBB})
		raw := ab.Serialize()
		require.Len(t, raw, availableBatchSerializedSize)

		digest := raw[12:]
		require.Equal(t, byte(0xAA), digest[0])
		require.Equal(t, byte(0xBB), digest[1])
		for _, b := range digest[2:] {
			require.Equal(t, byte(0), b)
		}
	})

	t.Run("long digest is truncated", func(t *testing.T) {
		long := bytesOf(0xCD, availableBatchDigestSize+16)
		ab := NewAvailableBatch(1, 2, 3, long)
		raw := ab.Serialize()
		require.Len(t, raw, availableBatchSerializedSize)
		require.Equal(t, long[:availableBatchDigestSize], raw[12:])
	})

	t.Run("nil digest is zero-padded", func(t *testing.T) {
		ab := NewAvailableBatch(1, 2, 3, nil)
		raw := ab.Serialize()
		require.Len(t, raw, availableBatchSerializedSize)
		require.Equal(t, make([]byte, availableBatchDigestSize), raw[12:])
	})
}

func bytesOf(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func incrementingDigest() []byte {
	d := make([]byte, availableBatchDigestSize)
	for i := range d {
		d[i] = byte(i)
	}
	return d
}
