/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configack

import (
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
	h := NewConfigAckHandler(testutil.CreateLogger(t, 0), shards)
	defer h.Stop()

	err := h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)
}

// TestAddAck_DuplicateRejected verifies that re-sending the same sequence number is rejected
func TestAddAck_DuplicateRejected(t *testing.T) {
	shards := []types.ShardID{1, 2}
	h := NewConfigAckHandler(testutil.CreateLogger(t, 0), shards)
	defer h.Stop()

	err := h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	err = h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "config ack has been received on sequence 1 but the last acknowledged sequence is 1")
}

// TestAddAck_DuplicateRejected verifies that ack with out-of-sequence number is rejected
func TestAddAck_OutOfOrderRejected(t *testing.T) {
	shards := []types.ShardID{1, 2}
	h := NewConfigAckHandler(testutil.CreateLogger(t, 0), shards)
	defer h.Stop()

	err := h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	err = h.AddAck(&protos.ConfigAck{
		ConfigSeq: 3,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "config ack has been received on sequence 3 but the last acknowledged sequence is 1")
}

// TestAddAck_Concurrent verifies that concurrent AddAck calls from different
// clients do not cause races.
func TestAddAck_Concurrent(t *testing.T) {
	shards := []types.ShardID{1, 2}
	h := NewConfigAckHandler(testutil.CreateLogger(t, 0), shards)
	defer h.Stop()

	var wg sync.WaitGroup

	sendAck := func(nodeType protos.NodeType, shard uint32) {
		defer wg.Done()
		_ = h.AddAck(&protos.ConfigAck{
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
// clients have already acked the requested sequence before the call.
func TestExpectAck_AllAcknowledged(t *testing.T) {
	shards := []types.ShardID{1, 2}
	h := NewConfigAckHandler(testutil.CreateLogger(t, 0), shards)
	defer h.Stop()

	// send ack from all clients (Router, Assembler, Batchers) at seq=1.
	err := h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	err = h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_BATCHER,
		Shard:     1,
	})
	require.NoError(t, err)

	err = h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_BATCHER,
		Shard:     2,
	})
	require.NoError(t, err)

	err = h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ASSEMBLER,
	})
	require.NoError(t, err)

	res := h.ExpectAck(1)
	require.True(t, res)
}

// TestExpectAck_StopReturnsFalse verifies that calling Stop() while ExpectAck
// is blocked causes it to return false.
func TestExpectAck_StopReturnsFalse(t *testing.T) {
	shards := []types.ShardID{1, 2}
	h := NewConfigAckHandler(testutil.CreateLogger(t, 0), shards)
	defer h.Stop()

	result := make(chan bool, 1)
	go func() { result <- h.ExpectAck(1) }()

	err := h.AddAck(&protos.ConfigAck{
		ConfigSeq: 1,
		NodeType:  protos.NodeType_ROUTER,
	})
	require.NoError(t, err)

	h.Stop()

	select {
	case res := <-result:
		require.False(t, res)
	case <-time.After(60 * time.Second):
		t.Fatal("ExpectAck did not return after Stop")
	}
}
