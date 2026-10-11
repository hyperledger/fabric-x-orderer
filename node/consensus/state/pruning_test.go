/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"math"
	"testing"

	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssemblerHeightProto(t *testing.T) {
	h := AssemblerHeight{Party: 3, DecisionNum: 42}

	var got AssemblerHeight
	require.NoError(t, got.fromProto(h.toProto()))
	assert.Equal(t, h, got)

	err := got.fromProto(&stateprotos.AssemblerHeight{Party: math.MaxUint16 + 1})
	require.ErrorContains(t, err, "Party value 65536 exceeds uint16 maximum")
}

func TestPrunePointProto(t *testing.T) {
	p := PrunePoint{Shard: 2, Primary: 4, Seq: 1000}

	var got PrunePoint
	require.NoError(t, got.fromProto(p.toProto()))
	assert.Equal(t, p, got)

	err := got.fromProto(&stateprotos.PrunePoint{Shard: math.MaxUint16 + 1, Primary: 1})
	require.ErrorContains(t, err, "Shard value 65536 exceeds uint16 maximum")

	err = got.fromProto(&stateprotos.PrunePoint{Shard: 1, Primary: math.MaxUint16 + 1})
	require.ErrorContains(t, err, "Primary value 65536 exceeds uint16 maximum")
}
