package test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"

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

	_, clean := createConsenters(t, numParties, consenterNodes, consentersInfo, shards)
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
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(0))
	batchers0[0].Submit(context.Background(), &protos.Request{Payload: req})

	// Verify the batchers created a batch in shard 0
	require.Eventually(t, func() bool {
		return batchers0[0].Ledger.Height(1) == 1 && batchers0[1].Ledger.Height(1) == 1
	}, 30*time.Second, 100*time.Millisecond)

	// Submit a request to primary of shard 1
	req = make([]byte, 8)
	binary.BigEndian.PutUint32(req, uint32(4))
	batchers1[1].Submit(context.Background(), &protos.Request{Payload: req})

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
	// TODO: req should have been batched after the new primary was elected, but it disappears
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(3))
	batchers0[1].Submit(context.Background(), &protos.Request{Payload: req})
	batchers0[2].Submit(context.Background(), &protos.Request{Payload: req})

	// Validate a term change occurred in shard 0
	require.Eventually(t, func() bool {
		return batchers0[1].GetPrimaryID() == types.PartyID(2) && batchers0[2].GetPrimaryID() == types.PartyID(2)
	}, 100*time.Second, 1000*time.Millisecond)

	// Submit a request to primary of shard 1
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(7))
	batchers1[1].Submit(context.Background(), &protos.Request{Payload: req})

	// Verify the batchers created a batch in shard 1
	require.Eventually(t, func() bool {
		return batchers1[0].Ledger.Height(2) == uint64(2) && batchers1[1].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 100*time.Millisecond)

	// Submit and verify request with new primary in shard 0
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(5))
	batchers0[1].Submit(context.Background(), &protos.Request{Payload: req})

	require.Eventually(t, func() bool {
		return batchers0[1].Ledger.Height(2) == uint64(1) && batchers0[2].Ledger.Height(2) == uint64(1)
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
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(8))
	batchers0[2].Submit(context.Background(), &protos.Request{Payload: req})

	req = make([]byte, 9)
	binary.BigEndian.PutUint64(req, uint64(10))
	batchers1[2].Submit(context.Background(), &protos.Request{Payload: req})

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
