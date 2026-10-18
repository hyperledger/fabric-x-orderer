/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configack

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

// TestAddAck_Success verifies that a valid sequential ack is accepted.
func TestAddAck_Success(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)
}

// TestAddAck_DuplicateAccepted verifies that re-sending the same sequence number is accepted, so a
// party member that retries its ack (e.g., after a dropped connection) waits for the release again.
func TestAddAck_DuplicateAccepted(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	err = r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)
}

// TestAddAck_StaleRejected verifies that an ack on a sequence older than the last acknowledged one is rejected
func TestAddAck_StaleRejected(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 2,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	err = r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "config ack has been received from router on sequence 1 but the last acknowledged sequence is 2")
}

// TestAddAck_IllegalValues verifies that ack from illegal shard or unknown node is rejected
func TestAddAck_IllegalValues(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType(3),
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "unknown node type")

	err = r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_BATCHER,
		Shard:     3,
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "config ack received from unknown client: batcher, shard=3")
}

// TestAddAck_Concurrent verifies that concurrent AddAck calls from different
// clients do not cause races.
func TestAddAck_Concurrent(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	var wg sync.WaitGroup

	sendAck := func(nodeType protos.NodeType, shard uint32) {
		defer wg.Done()
		_ = r.AddAck(&protos.ConfigAck{
			ConfigSeq: 1,
			NodeType:  nodeType,
			Shard:     shard,
		})
	}

	wg.Add(4)
	go sendAck(protos.NodeType_ROUTER, 0)
	go sendAck(protos.NodeType_ASSEMBLER, 0)
	go sendAck(protos.NodeType_BATCHER, 1)
	go sendAck(protos.NodeType_BATCHER, 2)
	wg.Wait()
}

// TestExpectAck_AllAcknowledged verifies that ExpectAck returns true when all
// clients have already acknowledged the requested sequence before the call.
func TestExpectAck_AllAcknowledged(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	// send ack from all clients (Router, Assembler, Batchers) at seq=1.
	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	err = r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_BATCHER,
		Shard:     1,
	})
	require.NoError(t, err)

	err = r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_BATCHER,
		Shard:     2,
	})
	require.NoError(t, err)

	err = r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ASSEMBLER,
	})
	require.NoError(t, err)

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	res := r.WaitForAllAcks(timeoutCtx, 1)
	require.True(t, res)
}

// TestExpectAck_StopReturnsFalse verifies that calling Stop() while ExpectAck
// is blocked causes it to return false.
func TestExpectAck_StopReturnsFalse(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	result := make(chan bool, 1)
	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	go func() { result <- r.WaitForAllAcks(timeoutCtx, 1) }()

	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	r.Stop()

	select {
	case res := <-result:
		require.False(t, res)
	case <-time.After(60 * time.Second):
		t.Fatal("ExpectAck did not return after Stop")
	}
}

// TestWaitForRelease_ReleasedWhenAllAcksReceived verifies that a party member waiting for the release of a
// config sequence stays blocked while some acks are missing, and is released once the last ack arrives.
func TestWaitForRelease_ReleasedWhenAllAcksReceived(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	allAcks := make(chan bool, 1)
	go func() { allAcks <- r.WaitForAllAcks(timeoutCtx, 1) }()

	released := make(chan error, 1)
	go func() { released <- r.WaitForRelease(context.Background(), 1) }()

	require.NoError(t, r.AddAck(&protos.ConfigAck{ConfigSeq: 1, NodeType: protos.NodeType_ROUTER}))
	require.NoError(t, r.AddAck(&protos.ConfigAck{ConfigSeq: 1, NodeType: protos.NodeType_BATCHER, Shard: 1}))
	require.NoError(t, r.AddAck(&protos.ConfigAck{ConfigSeq: 1, NodeType: protos.NodeType_BATCHER, Shard: 2}))

	select {
	case err := <-released:
		t.Fatalf("WaitForRelease returned (%v) before the assembler acked", err)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, r.AddAck(&protos.ConfigAck{ConfigSeq: 1, NodeType: protos.NodeType_ASSEMBLER}))

	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForRelease did not return after all acks were received")
	}
	require.True(t, <-allAcks)
}

// TestWaitForRelease_ReleasedOnAckTimeout verifies that when the consenter's wait for acks times out,
// the party members that did ack are released anyway.
func TestWaitForRelease_ReleasedOnAckTimeout(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)
	defer r.Stop()

	released := make(chan error, 1)
	go func() { released <- r.WaitForRelease(context.Background(), 1) }()

	// only the router acks; the batchers and the assembler never do.
	require.NoError(t, r.AddAck(&protos.ConfigAck{ConfigSeq: 1, NodeType: protos.NodeType_ROUTER}))

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelFunc()
	require.False(t, r.WaitForAllAcks(timeoutCtx, 1))

	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForRelease did not return after the ack timeout expired")
	}
}

// TestWaitForRelease_CurrentConfigSeqIsReleased verifies that the receiver treats the consenter's current
// config sequence, and every older one, as released: the consenter is not waiting for acks on them, so a
// party member that acks them (e.g., a retry after the consenter restarted on that config) is answered at once.
func TestWaitForRelease_CurrentConfigSeqIsReleased(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 2)
	defer r.Stop()

	require.NoError(t, r.WaitForRelease(context.Background(), 2))
	require.NoError(t, r.WaitForRelease(context.Background(), 1))

	// the next sequence is not released until the consenter waits for its acks.
	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelFunc()
	require.ErrorIs(t, r.WaitForRelease(timeoutCtx, 3), context.DeadlineExceeded)
}

// TestWaitForRelease_StopReturnsError verifies that stopping the receiver while a party member waits
// returns an error rather than releasing it: the consenter is shutting down, not done waiting for acks.
func TestWaitForRelease_StopReturnsError(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards, 0)

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	allAcks := make(chan bool, 1)
	go func() { allAcks <- r.WaitForAllAcks(timeoutCtx, 1) }()

	released := make(chan error, 1)
	go func() { released <- r.WaitForRelease(context.Background(), 1) }()

	require.NoError(t, r.AddAck(&protos.ConfigAck{ConfigSeq: 1, NodeType: protos.NodeType_ROUTER}))
	r.Stop()

	select {
	case err := <-released:
		require.ErrorContains(t, err, "config ack receiver is stopped")
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForRelease did not return after Stop")
	}

	// the stopped wait for acks did not release the sequence either.
	require.False(t, <-allAcks)
	require.ErrorContains(t, r.WaitForRelease(context.Background(), 1), "config ack receiver is stopped")
}
