/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchedRequests_Serialize(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var br *BatchedRequests
		assert.Nil(t, br.Serialize())

		br1 := BatchedRequests{}
		err := br.Deserialize(br1.Serialize())
		assert.EqualError(t, err, "acceptor is nil")
	})

	t.Run("empty and append", func(t *testing.T) {
		br1 := BatchedRequests{}
		var br2 BatchedRequests
		err := br2.Deserialize(br1.Serialize())
		assert.EqualError(t, err, "nil bytes")
		require.Equal(t, br1, br2)

		br1 = append(br1, []byte{1})
		br1 = append(br1, []byte{2})
		br1 = append(br1, []byte{3})

		err = br2.Deserialize(br1.Serialize())
		assert.NoError(t, err)
		require.Equal(t, br1, br2)

		err = br1.Deserialize([]byte{123})
		require.EqualError(t, err, "size of req is not encoded correctly")
	})

	t.Run("big random", func(t *testing.T) {
		var br3 BatchedRequests
		for i := 0; i < 10; i++ {
			req := make([]byte, 100)
			_, _ = rand.Read(req)
			br3 = append(br3, req)
		}
		var b2 BatchedRequests
		err := b2.Deserialize(br3.Serialize())
		require.NoError(t, err)
		require.Equal(t, br3, b2)
	})
}

// Test Digest and its equivalence with hash on serialized form.
func TestBatchedRequests_Digest(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var br1 *BatchedRequests
		assert.Len(t, br1.Digest(), 32)
		var br2 BatchedRequests
		assert.Equal(t, br1.Digest(), br2.Digest())
	})

	t.Run("empty", func(t *testing.T) {
		br1 := BatchedRequests(nil)
		assert.Len(t, br1.Digest(), 32)
		var br2 BatchedRequests
		assert.Len(t, br2.Digest(), 32)
		assert.Equal(t, br1.Digest(), br2.Digest())
		assert.Equal(t, br2.Digest(), BatchRequestsDataHashWithSerialize(br2))
	})

	t.Run("empty TXs", func(t *testing.T) {
		br1 := BatchedRequests(nil)
		br1 = append(br1, []byte{1})
		br1 = append(br1, []byte{})
		br1 = append(br1, []byte{2})
		assert.Len(t, br1.Digest(), 32)
		assert.Equal(t, br1.Digest(), BatchRequestsDataHashWithSerialize(br1))

		var br2 BatchedRequests
		br1 = append(br1, []byte{1})
		br1 = append(br1, []byte{2})
		br1 = append(br1, []byte{})
		assert.Len(t, br2.Digest(), 32)
		assert.Equal(t, br2.Digest(), BatchRequestsDataHashWithSerialize(br2))

		assert.NotEqual(t, br1.Digest(), br2.Digest())
	})

	t.Run("big random", func(t *testing.T) {
		reqs := [][]byte{}
		for i := 0; i < 1000; i++ {
			rq := make([]byte, 512)
			_, _ = rand.Read(rq)
			reqs = append(reqs, rq)
		}

		d1 := BatchRequestsDataHashWithSerialize(reqs)
		assert.NotEmpty(t, d1)

		br := BatchedRequests(reqs)
		d2 := br.Digest()
		assert.NotEmpty(t, d2)
		assert.Equal(t, d1, d2)
	})
}

func Benchmark_Digest(b *testing.B) {
	b.Log("Compare performance of digest computation with serialize vs. cumulative")
	reqs := [][]byte{}
	for i := 0; i < 1000; i++ {
		rq := make([]byte, 512)
		_, _ = rand.Read(rq)
		reqs = append(reqs, rq)
	}

	b.Run("Digest with Serialize", func(b *testing.B) {
		b.ReportAllocs()
		var d1 []byte
		for i := 0; i < b.N; i++ {
			d1 = BatchRequestsDataHashWithSerialize(reqs)
			if d1 == nil {
				b.Fail()
			}
		}
		if d1 == nil {
			b.Fail()
		}
	})

	b.Run("Cumulative Digest", func(b *testing.B) {
		b.ReportAllocs()
		var d1 []byte
		for i := 0; i < b.N; i++ {
			br := BatchedRequests(reqs)
			d1 = br.Digest()
			if d1 == nil {
				b.Fail()
			}
		}
		if d1 == nil {
			b.Fail()
		}
	})
}

func TestBatchedRequests_SizeBytes(t *testing.T) {
	var brp *BatchedRequests
	assert.Equal(t, int64(0), brp.SizeBytes())

	var br BatchedRequests
	assert.Equal(t, int64(0), br.SizeBytes())

	br1 := BatchedRequests([][]byte{{}, {}})
	assert.Equal(t, int64(0), br1.SizeBytes())

	br2 := BatchedRequests([][]byte{{1, 2}, {4, 5, 6}})
	assert.Equal(t, int64(5), br2.SizeBytes())

	br3 := BatchedRequests([][]byte{{1, 2}, {4, 5, 6}, {7}, {}})
	assert.Equal(t, int64(6), br3.SizeBytes())
}
