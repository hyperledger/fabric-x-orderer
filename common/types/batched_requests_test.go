package types

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchedRequests(t *testing.T) {
	br := BatchedRequests{}
	var br2 BatchedRequests
	br2.Deserialize(br.Serialize())
	require.Equal(t, br, br2)

	br = append(br, []byte{1})
	br = append(br, []byte{2})
	br = append(br, []byte{3})

	br2.Deserialize(br.Serialize())
	require.Equal(t, br, br2)

	require.Error(t, br.Deserialize([]byte{123}))

	var b BatchedRequests
	for i := 0; i < 10; i++ {
		req := make([]byte, 100)
		rand.Read(req)
		b = append(b, req)
	}

	var b2 BatchedRequests
	b2.Deserialize(b.Serialize())
	require.Equal(t, b, b2)
}
