/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
)

func init() {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})
}

var (
	digest    = make([]byte, 32-3)
	digest123 = append([]byte{1, 2, 3}, digest...)
	digest124 = append([]byte{1, 2, 4}, digest...)
	digest125 = append([]byte{1, 2, 5}, digest...)
)

func TestCreateConsensusNodePanicsWithNilGenesisBlock(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	require.PanicsWithValue(t, "Error creating Consensus1, last config block is nil", func() {
		setupConsensusTest(t, ca, 1, nil)
	})
}

func TestCreateOneConsensusNode(t *testing.T) {
	t.Parallel()

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, 1, genesisBlock)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)

	setup.consensusNodes[0].Stop()

	err = recoverNode(t, setup, 0, ca, genesisBlock)
	require.NoError(t, err)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)

	setup.consensusNodes[0].Stop()
}

// This test starts a cluster of 4 consensus nodes.
// Sending a couple of requests and waiting for 2 blocks to be written to the ledger.
// Then taking down the leader node (ID=1).
// The rest of the nodes get a new request and we make sure another block is committed (a view change occurs).
// Then we restart the stopped node, send one more request, and we make sure the nodes commit another block.
// Finally, we make sure the restarted node is synced with all blocks.
func TestCreateMultipleConsensusNodes(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	t.Log(">>> Network started")
	time.Sleep(30 * time.Second)

	var err error
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		b := <-setup.listeners[0].c
		b1 := <-setup.listeners[1].c
		b2 := <-setup.listeners[2].c
		return b.Header.Number == uint64(1) && b1.Header.Number == uint64(1) && b2.Header.Number == uint64(1)
	}, 2*time.Minute, 1*time.Second)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		b := <-setup.listeners[0].c
		b1 := <-setup.listeners[1].c
		b2 := <-setup.listeners[2].c
		return b.Header.Number == uint64(2) && b1.Header.Number == uint64(2) && b2.Header.Number == uint64(2)
	}, 2*time.Minute, 1*time.Second)

	t.Log(">>> Stopping node PartyID=1")
	setup.consensusNodes[0].Stop()

	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		b1 := <-setup.listeners[1].c
		b2 := <-setup.listeners[2].c
		return b1.Header.Number == uint64(3) && b2.Header.Number == uint64(3)
	}, 2*time.Minute, 1*time.Second)

	require.Eventually(t, func() bool {
		b31 := <-setup.listeners[3].c
		b32 := <-setup.listeners[3].c
		b33 := <-setup.listeners[3].c
		return b31.Header.Number == uint64(1) && b32.Header.Number == uint64(2) && b33.Header.Number == uint64(3)
	}, 2*time.Minute, 1*time.Second)

	// Recovery of the stopped node (ID=1)
	t.Log(">>> Recovering node PartyID=1")
	recoverNodeReal(t, dir, 0, genesisBlock, setup)

	time.Sleep(time.Minute)

	require.Eventually(t, func() bool {
		b := <-setup.listeners[0].c
		return b.Header.Number == uint64(3)
	}, 2*time.Minute, 1*time.Second)

	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		b1 := <-setup.listeners[1].c
		b2 := <-setup.listeners[2].c
		b3 := <-setup.listeners[3].c
		return b1.Header.Number == uint64(4) && b2.Header.Number == uint64(4) && b3.Header.Number == uint64(4)
	}, 2*time.Minute, 1*time.Second)

	require.Eventually(t, func() bool {
		b := <-setup.listeners[0].c
		return b.Header.Number == uint64(4)
	}, 2*time.Minute, 1*time.Second)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 4 consensus nodes to handle leader failure and recovery.
// The leader (ID=1) fails before the first request is sent, recovers, and writes it to the ledger.
// The leader fails again, recovers, and writes the second request to the ledger.
// The leader is never replaced, and no view change occurs.
func TestLeaderFailureAndRecovery(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	time.Sleep(30 * time.Second)

	// Leader fails and recovers before the first request
	setup.consensusNodes[0].Stop()

	//=====
	// Recovery of the stopped node (ID=1)
	recoverNodeReal(t, dir, 0, genesisBlock, setup)

	var err error
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b1 := <-setup.listeners[1].c
	require.Equal(t, uint64(1), b1.Header.Number)
	b2 := <-setup.listeners[2].c
	require.Equal(t, uint64(1), b2.Header.Number)

	// Leader fails and recovers between blocks
	setup.consensusNodes[0].Stop()

	//=====
	// Recovery of the stopped node (ID=1)
	recoverNodeReal(t, dir, 0, genesisBlock, setup)

	//=====
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 4 consensus nodes and check three scenarios where a non-leader node fails and recovers.
//
// In the first scenario, node (ID=2) fails and recovers before the first request is sent.
// One request is sent, and other 3 nodes are expected to get it in block number 1,
// Second request is sent, and other 3 nodes are expected to get it in block number 2,
// The recovering node is expected to get just block number 1 as it may be one block behind.
//
// In the second scenario, node (ID=2) fails after the first two block are committed, and recovers immediately.
// The third request is sent and committed by the remaining nodes (in block number 3) after node (ID=2) has recovered.
// The recovering node is then expected to get just block number 2 as it may be one block behind.
//
// In the third scenario, node (ID=2) fails and recovers between blocks.
// One request is sent while it is down. The second after its recovery, creating blocks 4 and 5.
// The recovering node is then expected to get just block number 4 as it may be one block behind.
func TestNonLeaderNodeFailureRecovery(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	time.Sleep(30 * time.Second)

	//========
	// First scenario:
	// Node fails and recovers before the first request
	setup.consensusNodes[1].Stop()

	// Recovery of the stopped node (ID=2)
	recoverNodeReal(t, dir, 1, genesisBlock, setup)

	err := createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b2 := <-setup.listeners[2].c
	require.Equal(t, uint64(1), b2.Header.Number)
	b3 := <-setup.listeners[3].c
	require.Equal(t, uint64(1), b3.Header.Number)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)
	b3 = <-setup.listeners[3].c
	require.Equal(t, uint64(2), b3.Header.Number)

	// Ensure node recovers correctly
	// Node ID=2 may be one decision behind
	require.Eventually(t, func() bool {
		return setup.consensusNodes[1].Storage.Height() >= 2
	}, 30*time.Second, 100*time.Millisecond)

	//========
	// Second scenario:
	// Node (ID=2) is stopped
	setup.consensusNodes[1].Stop()

	// Recovery of the stopped node (ID=2)
	recoverNodeReal(t, dir, 1, genesisBlock, setup)
	time.Sleep(10 * time.Second)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(3), b2.Header.Number)

	// Ensure node recovers correctly
	// Node ID=2 may be one decision behind
	require.Eventually(t, func() bool {
		return setup.consensusNodes[1].Storage.Height() >= 3
	}, 30*time.Second, 100*time.Millisecond)

	//========
	// Third scenario:
	// Node fails and recovers between blocks
	setup.consensusNodes[1].Stop()

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(4), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(4), b2.Header.Number)

	// Recovery of the stopped node (ID=2)
	recoverNodeReal(t, dir, 1, genesisBlock, setup)
	time.Sleep(10 * time.Second)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 5)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 5)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(5), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(5), b2.Header.Number)

	// Ensure node recovers correctly
	// Node ID=2 may be one decision behind
	require.Eventually(t, func() bool {
		return setup.consensusNodes[1].Storage.Height() >= 5
	}, 30*time.Second, 100*time.Millisecond)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 4 consensus nodes and simulates failure and recovery of two non-leader nodes (IDs=2,3).
// Nodes (IDs=2,3) fail after the first block is committed.
// Node (ID=2) recovers, and additional requests are sent and committed while node 3 remains down.
func TestTwoNodeFailureRecovery(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	time.Sleep(30 * time.Second)

	var err error
	// Submit the first request and verify the first block is committed
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b = <-setup.listeners[1].c
	require.Equal(t, uint64(1), b.Header.Number)
	b = <-setup.listeners[2].c
	require.Equal(t, uint64(1), b.Header.Number)

	// Nodes (IDs=2,3) fail
	setup.consensusNodes[1].Stop()
	setup.consensusNodes[2].Stop()

	//====
	// Node ID=2 recovers
	recoverNodeReal(t, dir, 1, genesisBlock, setup)

	// Submit and verify requests while node 3 remains down
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[3], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b.Header.Number)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[3], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 3)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b = <-setup.listeners[1].c
	require.Equal(t, uint64(3), b.Header.Number)

	b = <-setup.listeners[3].c
	require.Equal(t, uint64(1), b.Header.Number)
	b = <-setup.listeners[3].c
	require.Equal(t, uint64(2), b.Header.Number)
	b = <-setup.listeners[3].c
	require.Equal(t, uint64(3), b.Header.Number)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 7 consensus nodes and checks scenarios where non-leader nodes (IDs=2,3) fail and recover:
//  1. Nodes (IDs=2,3) fail and recover before the first request is sent.
//  2. Nodes (IDs=2,3) fail, a request is committed by the remaining nodes, and the failed nodes recover and synchronize.
func TestMultipleNodesFailureRecovery(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4, 5, 6, 7}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	time.Sleep(30 * time.Second)

	// Nodes fail and recover before the first request
	setup.consensusNodes[1].Stop()
	setup.consensusNodes[2].Stop()
	recoverNodeReal(t, dir, 1, genesisBlock, setup)
	recoverNodeReal(t, dir, 2, genesisBlock, setup)
	var err error
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b5 := <-setup.listeners[5].c
	require.Equal(t, uint64(1), b5.Header.Number)
	b6 := <-setup.listeners[6].c
	require.Equal(t, uint64(1), b6.Header.Number)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(2), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(2), b6.Header.Number)

	// Ensure nodes recover correctly
	require.Eventually(t, func() bool {
		return setup.consensusNodes[1].Storage.Height() >= 2
	}, 30*time.Second, 1*time.Second)
	require.Eventually(t, func() bool {
		return setup.consensusNodes[2].Storage.Height() >= 2
	}, 30*time.Second, 1*time.Second)

	// Nodes fail
	setup.consensusNodes[1].Stop()
	setup.consensusNodes[2].Stop()

	// Commit a second request with remaining nodes
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 3)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(3), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(3), b6.Header.Number)

	recoverNodeReal(t, dir, 1, genesisBlock, setup)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(4), b.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(4), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(4), b6.Header.Number)

	// Ensure nodes recover correctly
	require.Eventually(t, func() bool {
		return setup.consensusNodes[1].Storage.Height() >= 5
	}, 30*time.Second, 1*time.Second)

	recoverNodeReal(t, dir, 2, genesisBlock, setup)

	// Ensure nodes recover correctly
	require.Eventually(t, func() bool {
		return setup.consensusNodes[2].Storage.Height() >= 5
	}, 30*time.Second, 1*time.Second)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 7 consensus nodes.
// The leader (ID=1) and a follower (ID=3) fail after the first request is committed.
// New requests are committed by the remaining nodes during the failure (a view change occurs).
// After restarting the failed nodes, two more requests are sent, and both blocks are committed to the ledger.
func TestMultipleLeaderNodeFailureRecovery(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4, 5, 6, 7}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	time.Sleep(30 * time.Second)

	var err error
	// Commit the first request
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b1 := <-setup.listeners[1].c
	require.Equal(t, uint64(1), b1.Header.Number)
	b2 := <-setup.listeners[2].c
	require.Equal(t, uint64(1), b2.Header.Number)
	b5 := <-setup.listeners[5].c
	require.Equal(t, uint64(1), b5.Header.Number)
	b6 := <-setup.listeners[6].c
	require.Equal(t, uint64(1), b6.Header.Number)

	// Stop the leader and a follower
	setup.consensusNodes[0].Stop()
	setup.consensusNodes[2].Stop()

	// Commit a second request with remaining nodes
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[3], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(2), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(2), b6.Header.Number)

	// Restart one stopped node
	recoverNodeReal(t, dir, 2, genesisBlock, setup)

	// Commit more requests
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(3), b1.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(3), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(3), b6.Header.Number)

	// Ensure node recovers correctly
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)

	// Restart the other node
	recoverNodeReal(t, dir, 0, genesisBlock, setup)

	// Commit another request
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 4)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 4)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 4)
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(4), b1.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(4), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(4), b6.Header.Number)

	// Ensure nodes recover correctly, they may miss the last decision
	require.Eventually(t, func() bool {
		return setup.consensusNodes[2].Storage.Height() >= 4
	}, 30*time.Second, 1*time.Second)

	require.Eventually(t, func() bool {
		return setup.consensusNodes[0].Storage.Height() >= 4
	}, 30*time.Second, 1*time.Second)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 4 consensus nodes and simulates failure and recovery of a non-leader node (ID=3) while all other nodes are in SoftStop mode.
// Node (ID=3) fails after the first block is committed, and a second request is sent and committed by the remaining nodes while node (ID=3) is down.
// All the rest of the nodes are then put in SoftStop mode, and node (ID=3) recovers.
// After restarting the failed node, we check it synchronizes with the other nodes and commits the second block.
func TestSyncFromSoftStoppedNodes(t *testing.T) {
	t.Parallel()

	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1
	dir := t.TempDir()
	genesisBlock, setup := createTestSetupReal(t, dir, parties, numOfShards)
	time.Sleep(30 * time.Second)

	var err error
	// Commit the first request
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b1 := <-setup.listeners[1].c
	require.Equal(t, uint64(1), b1.Header.Number)
	b2 := <-setup.listeners[2].c
	require.Equal(t, uint64(1), b2.Header.Number)

	// Node 3 fails and submit the second request
	setup.consensusNodes[2].Stop()

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)

	b3 := <-setup.listeners[3].c
	require.Equal(t, uint64(1), b3.Header.Number)
	b3 = <-setup.listeners[3].c
	require.Equal(t, uint64(2), b3.Header.Number)

	// SoftStop all nodes except node 3
	for i, c := range setup.consensusNodes {
		if i != 2 {
			c.SoftStop()
		}
	}

	// Recover node 3
	recoverNodeReal(t, dir, 2, genesisBlock, setup)

	// Verify node 3 synced correctly from other nodes during SoftStop
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}
