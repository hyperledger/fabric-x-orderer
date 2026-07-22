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
	r := NewReceiver(testutil.CreateLogger(t, 0), shards)
	defer r.Stop()

	err := r.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)
}

// TestAddAck_DuplicateRejected verifies that re-sending the same sequence number is rejected
func TestAddAck_DuplicateRejected(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards)
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
	require.Error(t, err)
	require.ErrorContains(t, err, "config ack has been received from router on sequence 1 but the last acknowledged sequence is 1")
}

// TestAddAck_IllegalValues verifies that ack from illegal shard or unknown node is rejected
func TestAddAck_IllegalValues(t *testing.T) {
	shards := []types.ShardID{1, 2}
	r := NewReceiver(testutil.CreateLogger(t, 0), shards)
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
	r := NewReceiver(testutil.CreateLogger(t, 0), shards)
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
	r := NewReceiver(testutil.CreateLogger(t, 0), shards)
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
	r := NewReceiver(testutil.CreateLogger(t, 0), shards)
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
