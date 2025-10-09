/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/stretchr/testify/require"
)

// This test involves 2 shards and 4 parties.
// It initializes 4 batchers per shard and a cluster of 4 consensus nodes.
// It verifies that the batcher correctly handles a primary failure, elects a new primary,
// continues processing requests, and properly updates when the old primary recovers.
func TestBatcherFailuresAndRecoveryWithTwoShards(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	numParties := 4

	batcherNodesShard0, batchersInfoShard0 := createBatcherNodesAndInfo(t, ca, numParties)
	batcherNodesShard1, batchersInfoShard1 := createBatcherNodesAndInfo(t, ca, numParties)
	consenterNodes, consentersInfo := createConsenterNodesAndInfo(t, ca, numParties)

	shards := []config.ShardInfo{{ShardId: 0, Batchers: batchersInfoShard0}, {ShardId: 1, Batchers: batchersInfoShard1}}

	genesisBlock := utils.EmptyGenesisBlock("arma")

	_, clean := createConsenters(t, numParties, consenterNodes, consentersInfo, shards, genesisBlock)
	defer clean()

	batchers0, configs, loggers, clean := createBatchersForShard(t, numParties, batcherNodesShard0, shards, consentersInfo, shards[0].ShardId)
	defer clean()

	batchers1, _, _, clean := createBatchersForShard(t, numParties, batcherNodesShard1, shards, consentersInfo, shards[1].ShardId)
	defer clean()

	for i := 0; i < 4; i++ {
		require.Equal(t, types.PartyID(1), batchers0[i].GetPrimaryID())
		require.Equal(t, types.PartyID(2), batchers1[i].GetPrimaryID())
	}

	// Submit request to primary in shard 0
	batchers0[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))

	// Verify the batchers created a batch in shard 0
	require.Eventually(t, func() bool {
		return batchers0[0].Ledger.Height(1) == 1 && batchers0[1].Ledger.Height(1) == 1
	}, 30*time.Second, 100*time.Millisecond)

	// Submit a request to primary of shard 1
	batchers1[1].Submit(context.Background(), tx.CreateStructuredRequest([]byte{11}))

	// Verify the batchers created a batch in shard 1
	require.Eventually(t, func() bool {
		return batchers1[3].Ledger.Height(2) == 1 && batchers1[2].Ledger.Height(2) == 1
	}, 120*time.Second, 100*time.Millisecond)

	for i := 0; i < 4; i++ {
		require.Equal(t, types.PartyID(1), batchers0[i].GetPrimaryID())
		require.Equal(t, types.PartyID(2), batchers1[i].GetPrimaryID())
	}

	// Stop primary batcher in shard 0
	batchers0[0].Stop()

	// Submit request to other batchers in shard 0
	batchers0[1].Submit(context.Background(), tx.CreateStructuredRequest([]byte{22}))
	batchers0[2].Submit(context.Background(), tx.CreateStructuredRequest([]byte{22}))

	// Validate a term change occurred in shard 0
	require.Eventually(t, func() bool {
		return batchers0[1].GetPrimaryID() == types.PartyID(2) && batchers0[2].GetPrimaryID() == types.PartyID(2)
	}, 100*time.Second, 1000*time.Millisecond)

	// Verify the new primary created a batch in shard 0
	require.Eventually(t, func() bool {
		return batchers0[1].Ledger.Height(2) == uint64(1) && batchers0[2].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 100*time.Millisecond)

	// Submit a request to primary of shard 1
	batchers1[1].Submit(context.Background(), tx.CreateStructuredRequest([]byte{3}))

	// Verify the batchers created a batch in shard 1
	require.Eventually(t, func() bool {
		return batchers1[0].Ledger.Height(2) == uint64(2) && batchers1[1].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 100*time.Millisecond)

	// Recover old primary in shard 0
	batchers0[0] = recoverBatcher(t, ca, loggers[0], configs[0], batcherNodesShard0[0])

	require.Eventually(t, func() bool {
		return batchers0[0].Ledger.Height(2) == 1
	}, 30*time.Second, 100*time.Millisecond)

	for i := 0; i < 4; i++ {
		require.Equal(t, types.PartyID(2), batchers0[i].GetPrimaryID())
		require.Equal(t, types.PartyID(2), batchers1[i].GetPrimaryID())
	}

	// Submit another request only to a secondary in shard 0 and shard 1
	batchers0[2].Submit(context.Background(), tx.CreateStructuredRequest([]byte{9}))

	batchers1[2].Submit(context.Background(), tx.CreateStructuredRequest([]byte{8}))

	// Verify the batchers created batches in shard 0 and shard 1
	require.Eventually(t, func() bool {
		return batchers0[0].Ledger.Height(2) == 2 && batchers0[1].Ledger.Height(2) == 2
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return batchers1[0].Ledger.Height(2) == 3 && batchers1[1].Ledger.Height(2) == 3
	}, 30*time.Second, 100*time.Millisecond)

	// Still same primary
	for i := 0; i < 4; i++ {
		require.Equal(t, types.PartyID(2), batchers0[i].GetPrimaryID())
		require.Equal(t, types.PartyID(2), batchers1[i].GetPrimaryID())
	}
}
