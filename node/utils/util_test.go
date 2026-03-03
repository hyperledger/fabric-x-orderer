/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/stretchr/testify/require"
)

func TestTLSCAcertsFromShards(t *testing.T) {
	// 4 parties, 2 shards - each party has its own CA, expect for 4 CA's
	numParties := 4
	shardIDs := []types.ShardID{types.ShardID(1), types.ShardID(2)}
	shards, cleanup := stub.NewStubBatchersAndInfos(t, numParties, shardIDs)
	require.NotNil(t, shards)
	defer cleanup()
	tlsCAsFromShards := utils.TLSCAcertsFromShards(shards)
	require.Equal(t, len(tlsCAsFromShards), 4)

	// 4 parties, 1 shards - each party has its own CA, expect for 4 CA's
	shardIDs = []types.ShardID{types.ShardID(1)}
	shards, cleanup = stub.NewStubBatchersAndInfos(t, numParties, shardIDs)
	require.NotNil(t, shards)
	defer cleanup()
	tlsCAsFromShards = utils.TLSCAcertsFromShards(shards)
	require.Equal(t, len(tlsCAsFromShards), 4)

	// 3 parties, 1 shards - each party has its own CA, expect for 3 CA's
	numParties = 3
	shards, cleanup = stub.NewStubBatchersAndInfos(t, numParties, shardIDs)
	require.NotNil(t, shards)
	defer cleanup()
	tlsCAsFromShards = utils.TLSCAcertsFromShards(shards)
	require.Equal(t, len(tlsCAsFromShards), 3)
}

func TestTLSCAcertsFromConsenters(t *testing.T) {
	// 3 parties - each party has its own CA, expect for 3 CA's
	numParties := 3

	consentersInfo, cleanup := stub.NewStubConsentersAndInfo(t, numParties)
	require.NotNil(t, consentersInfo)
	defer cleanup()

	tlsCAsFromShards := utils.TLSCAcertsFromConsenters(consentersInfo)
	require.Equal(t, len(tlsCAsFromShards), 3)
}
