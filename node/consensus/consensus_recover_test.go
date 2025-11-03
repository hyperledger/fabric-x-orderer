/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
)

var (
	digest    = make([]byte, 32-3)
	digest123 = append([]byte{1, 2, 3}, digest...)
	digest124 = append([]byte{1, 2, 4}, digest...)
	digest125 = append([]byte{1, 2, 5}, digest...)
)

func TestCreateConsensusNodePanicsWithNilGenesisBlock(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	require.PanicsWithValue(t, "Error creating Consensus1, last config block is nil", func() {
		setupConsensusTest(t, ca, 1, nil)
	})
}

func TestCreateOneConsensusNode(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

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
	parties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

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

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)

	setup.consensusNodes[0].Stop()

	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(3), b1.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(3), b2.Header.Number)

	b3 := <-setup.listeners[3].c
	require.Equal(t, uint64(1), b3.Header.Number)
	b3 = <-setup.listeners[3].c
	require.Equal(t, uint64(2), b3.Header.Number)
	b3 = <-setup.listeners[3].c
	require.Equal(t, uint64(3), b3.Header.Number)

	err = recoverNode(t, setup, 0, ca, genesisBlock)
	require.NoError(t, err)

	time.Sleep(time.Minute)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)

	err = createAndSubmitRequest(setup.consensusNodes[1], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(4), b1.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(4), b2.Header.Number)
	b3 = <-setup.listeners[3].c
	require.Equal(t, uint64(4), b3.Header.Number)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(4), b.Header.Number)

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
	parties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

	// Leader fails and recovers before the first request
	setup.consensusNodes[0].Stop()
	err = recoverNode(t, setup, 0, ca, genesisBlock)
	require.NoError(t, err)

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
	err = recoverNode(t, setup, 0, ca, genesisBlock)
	require.NoError(t, err)

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
// In the first scenario, node (ID=2) fails and recovers before the first request is sent.
// In the second scenario, node (ID=2) fails after the first block is committed.
// The second request is sent and committed by the remaining nodes while node (ID=2) is down.
// After it recovers, another request is sent, and we check the restarted node is synced with all blocks.
// In the third scenario, node (ID=2) fails and recovers between blocks.
// No requests are sent while it is down.
// After recovery, two more requests are sent, and both blocks are committed to the ledger.
func TestNonLeaderNodeFailureRecovery(t *testing.T) {
	t.Parallel()
	parties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

	// Node fails and recovers before the first request
	setup.consensusNodes[1].Stop()
	err = recoverNode(t, setup, 1, ca, genesisBlock)
	require.NoError(t, err)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b2 := <-setup.listeners[2].c
	require.Equal(t, uint64(1), b2.Header.Number)

	// Ensure node recovers correctly
	b1 := <-setup.listeners[1].c
	require.Equal(t, uint64(1), b1.Header.Number)

	// Node fails and submit the second request
	setup.consensusNodes[1].Stop()

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)

	err = recoverNode(t, setup, 1, ca, genesisBlock)
	require.NoError(t, err)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(3), b2.Header.Number)

	// Ensure node recovers correctly
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(3), b1.Header.Number)

	// Node fails and recovers between blocks
	setup.consensusNodes[1].Stop()
	err = recoverNode(t, setup, 1, ca, genesisBlock)
	require.NoError(t, err)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 4)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(4), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(4), b2.Header.Number)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 5)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[2], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 5)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(5), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(5), b2.Header.Number)

	// Ensure node recovers correctly
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(4), b1.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(5), b1.Header.Number)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

// This test initializes a cluster of 4 consensus nodes and simulates failure and recovery of two non-leader nodes (IDs=2,3).
// Nodes (IDs=2,3) fail after the first block is committed.
// Node (ID=2) recovers, and additional requests are sent and committed while node 3 remains down.
func TestTwoNodeFailureRecovery(t *testing.T) {
	t.Parallel()
	parties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

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

	// Node 2 recovers
	err = recoverNode(t, setup, 1, ca, genesisBlock)
	require.NoError(t, err)

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
	parties := 7
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

	// Nodes fail and recover before the first request
	setup.consensusNodes[1].Stop()
	setup.consensusNodes[2].Stop()
	err = recoverNode(t, setup, 1, ca, genesisBlock)
	require.NoError(t, err)
	err = recoverNode(t, setup, 2, ca, genesisBlock)
	require.NoError(t, err)

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

	// Ensure nodes recover correctly
	b1 := <-setup.listeners[1].c
	require.Equal(t, uint64(1), b1.Header.Number)
	b2 := <-setup.listeners[2].c
	require.Equal(t, uint64(1), b2.Header.Number)

	// Nodes fail
	setup.consensusNodes[1].Stop()
	setup.consensusNodes[2].Stop()

	// Commit a second request with remaining nodes
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(2), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(2), b6.Header.Number)

	err = recoverNode(t, setup, 1, ca, genesisBlock)
	require.NoError(t, err)

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[5], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)
	err = createAndSubmitRequest(setup.consensusNodes[6], setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b5 = <-setup.listeners[5].c
	require.Equal(t, uint64(3), b5.Header.Number)
	b6 = <-setup.listeners[6].c
	require.Equal(t, uint64(3), b6.Header.Number)

	// Ensure nodes recover correctly
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(3), b1.Header.Number)

	err = recoverNode(t, setup, 2, ca, genesisBlock)
	require.NoError(t, err)

	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(2), b2.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(3), b2.Header.Number)

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
	parties := 7
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

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
	err = recoverNode(t, setup, 2, ca, genesisBlock)
	require.NoError(t, err)

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
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(3), b2.Header.Number)

	// Restart the other node
	err = recoverNode(t, setup, 0, ca, genesisBlock)
	require.NoError(t, err)

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

	// Ensure nodes recover correctly
	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b = <-setup.listeners[0].c
	require.Equal(t, uint64(4), b.Header.Number)
	b2 = <-setup.listeners[2].c
	require.Equal(t, uint64(4), b2.Header.Number)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}
