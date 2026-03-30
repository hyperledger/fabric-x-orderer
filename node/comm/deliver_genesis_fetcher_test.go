/*
Copyright IBM Corp. 2026 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
)

func TestBlockPullerGenesisByEndpointsHappyPath(t *testing.T) {
	// Scenario: Three ordering nodes, all return genesis block successfully
	osn1 := newClusterNode(t)
	defer osn1.stop()

	osn2 := newClusterNode(t)
	defer osn2.stop()

	osn3 := newClusterNode(t)
	defer osn3.stop()

	dialer := newCountingDialer()
	bp := newBlockPuller(dialer, osn1.srv.Address(), osn2.srv.Address(), osn3.srv.Address())

	// Each node expects a seek request for block 0
	osn1.addExpectPullAssert(0)
	osn2.addExpectPullAssert(0)
	osn3.addExpectPullAssert(0)

	// All nodes return genesis block
	osn1.enqueueResponse(0)
	osn2.enqueueResponse(0)
	osn3.enqueueResponse(0)

	genesisBlocks, err := bp.GenesisByEndpoints()
	require.NoError(t, err)
	require.Len(t, genesisBlocks, 3)

	// Verify all blocks are genesis blocks (block 0)
	for endpoint, block := range genesisBlocks {
		require.NotNil(t, block, "block from %s should not be nil", endpoint)
		require.Equal(t, uint64(0), block.GetHeader().GetNumber(), "block from %s should be genesis", endpoint)
	}

	bp.Close()
	dialer.assertAllConnectionsClosed(t)
}

func TestBlockPullerGenesisByEndpointsPartialFailure(t *testing.T) {
	// Scenario: Three ordering nodes, one offline, one returns error, one succeeds
	osn1 := newClusterNode(t)
	// osn1 will be stopped immediately (offline)

	osn2 := newClusterNode(t)
	defer osn2.stop()

	osn3 := newClusterNode(t)
	defer osn3.stop()

	dialer := newCountingDialer()
	bp := newBlockPuller(dialer, osn1.srv.Address(), osn2.srv.Address(), osn3.srv.Address())

	// First node is offline
	osn1.stop()

	// Second node expects seek but returns forbidden
	osn2.addExpectPullAssert(0)
	osn2.blockResponses <- &orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Status{Status: common.Status_FORBIDDEN},
	}

	// Third node succeeds
	osn3.addExpectPullAssert(0)
	osn3.enqueueResponse(0)

	genesisBlocks, err := bp.GenesisByEndpoints()
	require.NoError(t, err)
	require.Len(t, genesisBlocks, 1)

	// Only osn3 should be in the result
	block, exists := genesisBlocks[osn3.srv.Address()]
	require.True(t, exists)
	require.NotNil(t, block)
	require.Equal(t, uint64(0), block.GetHeader().GetNumber())

	bp.Close()
	dialer.assertAllConnectionsClosed(t)
}

func TestBlockPullerGenesisByEndpointsAllFail(t *testing.T) {
	// Scenario: All ordering nodes fail to return genesis block
	osn1 := newClusterNode(t)
	defer osn1.stop()

	osn2 := newClusterNode(t)
	defer osn2.stop()

	dialer := newCountingDialer()
	bp := newBlockPuller(dialer, osn1.srv.Address(), osn2.srv.Address())

	// Both nodes return errors
	osn1.addExpectPullAssert(0)
	osn1.blockResponses <- &orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Status{Status: common.Status_FORBIDDEN},
	}

	osn2.addExpectPullAssert(0)
	osn2.blockResponses <- &orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Status{Status: common.Status_SERVICE_UNAVAILABLE},
	}

	genesisBlocks, err := bp.GenesisByEndpoints()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to fetch genesis block from any endpoint")
	require.Nil(t, genesisBlocks)

	bp.Close()
	dialer.assertAllConnectionsClosed(t)
}

func TestBlockPullerGenesisByEndpointsWrongBlockNumber(t *testing.T) {
	// Scenario: Node returns wrong block number (not genesis)
	osn1 := newClusterNode(t)
	defer osn1.stop()

	osn2 := newClusterNode(t)
	defer osn2.stop()

	dialer := newCountingDialer()
	bp := newBlockPuller(dialer, osn1.srv.Address(), osn2.srv.Address())

	// First node returns block 5 instead of block 0
	osn1.addExpectPullAssert(0)
	osn1.enqueueResponse(5)

	// Second node returns correct genesis block
	osn2.addExpectPullAssert(0)
	osn2.enqueueResponse(0)

	genesisBlocks, err := bp.GenesisByEndpoints()
	require.NoError(t, err)
	require.Len(t, genesisBlocks, 1)

	// Only osn2 should be in the result
	block, exists := genesisBlocks[osn2.srv.Address()]
	require.True(t, exists)
	require.NotNil(t, block)
	require.Equal(t, uint64(0), block.GetHeader().GetNumber())

	bp.Close()
	dialer.assertAllConnectionsClosed(t)
}

func TestBlockPullerGenesisByEndpointsDuplicateEndpoints(t *testing.T) {
	// Scenario: Same endpoint appears twice in the list
	osn := newClusterNode(t)
	defer osn.stop()

	dialer := newCountingDialer()
	// Add the same endpoint twice
	bp := newBlockPuller(dialer, osn.srv.Address(), osn.srv.Address())

	// Expect two seek requests (one per duplicate)
	osn.addExpectPullAssert(0)
	osn.addExpectPullAssert(0)

	// Return genesis block twice
	osn.enqueueResponse(0)
	osn.enqueueResponse(0)

	genesisBlocks, err := bp.GenesisByEndpoints()
	require.NoError(t, err)
	// Should only have one entry (duplicates handled)
	require.Len(t, genesisBlocks, 1)

	block, exists := genesisBlocks[osn.srv.Address()]
	require.True(t, exists)
	require.NotNil(t, block)
	require.Equal(t, uint64(0), block.GetHeader().GetNumber())

	bp.Close()
	dialer.assertAllConnectionsClosed(t)
}
