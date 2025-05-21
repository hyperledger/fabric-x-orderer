/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
)

func TestNewSimpleBatch(t *testing.T) {
	b1 := types.NewSimpleBatch(1, 2, 3, nil)
	assert.NotNil(t, b1)
	assert.Equal(t, types.BatchSequence(1), b1.Seq())
	assert.Equal(t, types.ShardID(2), b1.Shard())
	assert.Equal(t, types.PartyID(3), b1.Primary())
	assert.Nil(t, b1.Requests())
	assert.Len(t, b1.Digest(), 32)

	b2 := types.NewSimpleBatch(1, 2, 3, [][]byte{{1, 2}, {3, 4}})
	assert.NotNil(t, b2)
	assert.Equal(t, types.BatchSequence(1), b2.Seq())
	assert.Equal(t, types.ShardID(2), b2.Shard())
	assert.Equal(t, types.PartyID(3), b2.Primary())
	br := types.BatchedRequests([][]byte{{1, 2}, {3, 4}})
	assert.Equal(t, br, b2.Requests())
	assert.Equal(t, b2.Digest(), br.Digest())
}
