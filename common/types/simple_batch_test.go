/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/assert"
)

func TestNewSimpleBatch(t *testing.T) {
	b1 := types.NewSimpleBatch(1, 2, 3, nil)
	assert.NotNil(t, b1)
	assert.Equal(t, types.BatchSequence(1), b1.Seq())
	assert.Equal(t, types.ShardID(2), b1.Shard())
	assert.Equal(t, types.PartyID(3), b1.Primary())
	assert.Nil(t, b1.Requests())
	assert.Len(t, b1.Digest(), 32)
	assert.Equal(t, "Sh,Pr,Sq,Dg: <2,3,1,e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855>", types.BatchIDToString(b1))

	b2 := types.NewSimpleBatch(1, 2, 3, [][]byte{{1, 2}, {3, 4}})
	assert.NotNil(t, b2)
	assert.Equal(t, types.BatchSequence(1), b2.Seq())
	assert.Equal(t, types.ShardID(2), b2.Shard())
	assert.Equal(t, types.PartyID(3), b2.Primary())
	br := types.BatchedRequests([][]byte{{1, 2}, {3, 4}})
	assert.Equal(t, br, b2.Requests())
	assert.Equal(t, b2.Digest(), br.Digest())
	assert.Equal(t, "Sh,Pr,Sq,Dg: <2,3,1,93596cbcbd47e65c74edd35e35cbf23cd81d520f8bd04987ae9449c22a4632ba>", types.BatchIDToString(b2))

	var b3 *types.SimpleBatch
	assert.Equal(t, "<nil>", types.BatchIDToString(b3))

	var id types.BatchID
	assert.Equal(t, "<nil>", types.BatchIDToString(id))
}
