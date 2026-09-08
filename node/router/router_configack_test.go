/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/configack"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"

	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"

	"github.com/stretchr/testify/require"
)

// configAckRecorder captures the ConfigAck requests the router sends to the
// stub consenter. AckConfig is served on the consenter's gRPC goroutine, so all
// access is guarded by a mutex.
type configAckRecorder struct {
	mu   sync.Mutex
	acks []*protos.ConfigAck
}

func (r *configAckRecorder) record(req *protos.ConfigAck) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks = append(r.acks, req)
}

func (r *configAckRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.acks)
}

func (r *configAckRecorder) all() []*protos.ConfigAck {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*protos.ConfigAck(nil), r.acks...)
}

// waitForRouterServing blocks until the router's gRPC server is actually accepting
// connections. StartRouterService launches srv.Start() (grpc Serve) in a goroutine and
// returns immediately after setting the status to Running, so "Running" does not imply the
// router can accept connection.
func waitForRouterServing(t *testing.T, s *reconfigTestSetup) {
	require.Eventually(t, func() bool {
		conn, err := s.CreateRouterClient()
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond)
}

// autoRemoveTimeoutConfigUpdate builds a config update that changes a batching timeout. It
// does not require an admin restart in the router, so the router applies it dynamically and
// advances to config sequence 1.
func autoRemoveTimeoutConfigUpdate(t *testing.T, dir string) []byte {
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.AutoRemoveTimeout, "15ms"))
	require.NotNil(t, configUpdatePbData)
	return configUpdatePbData
}

// Scenario:
//  1. Start a router, stub batcher, and stub consenter with the initial config.
//  2. Record the ConfigAck the router sends by defining an AckConfigHandler on the stub consenter.
//  3. Deliver a config update (batching timeout change) through the consenter path.
//  4. Verify the router advances to config sequence 1 and that it sent exactly one
//     ConfigAck identifying itself as the router (NodeType=ROUTER, Shard=0) on sequence 1.
func TestRouterSendsConfigAck(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)

	recorder := &configAckRecorder{}
	testSetup.stubConsenter.AckConfigHandler = func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		recorder.record(req)
		return &protos.ConfigAckResponse{}, nil
	}

	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running with the initial config sequence.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
	waitForRouterServing(t, testSetup)

	testSetup.SendConfigUpdate(t, parties, autoRemoveTimeoutConfigUpdate(t, dir), dir, 1)

	// the router applies the new config and advances to config sequence 1.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)

	// exactly one ConfigAck is sent for the new config block.
	require.Eventually(t, func() bool {
		return recorder.count() == 1
	}, 20*time.Second, 100*time.Millisecond)

	acks := recorder.all()
	require.Len(t, acks, 1)
	require.Equal(t, protos.NodeType_ROUTER, acks[0].NodeType)
	require.EqualValues(t, 0, acks[0].Shard)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Start a router, stub batcher, and stub consenter with the initial config.
//  2. Define an AckConfigHandler that rejects the first few attempts, then accepts.
//  3. Deliver a config update through the consenter path.
//  4. Verify the router's sender retried (more than one attempt), the consenter
//     eventually recorded an ack on sequence 1, and the router still advanced to
//     config sequence 1.
func TestRouterConfigAck_RetriesUntilConsenterAccepts(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)

	const failuresBeforeSuccess = 3
	var callCount int32
	recorder := &configAckRecorder{}
	testSetup.stubConsenter.AckConfigHandler = func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		if atomic.AddInt32(&callCount, 1) <= failuresBeforeSuccess {
			return nil, fmt.Errorf("temporary failure")
		}
		recorder.record(req)
		return &protos.ConfigAckResponse{}, nil
	}

	testSetup.Start()
	defer testSetup.Stop()

	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
	waitForRouterServing(t, testSetup)

	testSetup.SendConfigUpdate(t, parties, autoRemoveTimeoutConfigUpdate(t, dir), dir, 1)

	// despite the initial failures, the router eventually applies the new config.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)

	// the accepted ack is for sequence 1, and the sender made more than one attempt.
	require.Eventually(t, func() bool {
		return recorder.count() == 1
	}, 20*time.Second, 100*time.Millisecond)

	require.Greater(t, atomic.LoadInt32(&callCount), int32(1))
	acks := recorder.all()
	require.Equal(t, protos.NodeType_ROUTER, acks[0].NodeType)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Start a router, stub batcher, and stub consenter with the initial config.
//  2. Record the ConfigAck the router sends.
//  3. Deliver a config update that removes (evicts) the router's own party.
//  4. Verify the router still sends a ConfigAck on sequence 1 before transitioning
//     to pending admin. This documents that SubmitConfigAck runs before the eviction
//     check in ApplyConfig, so the consenter learns the evicted node saw the config.
func TestRouterSendsConfigAckEvenWhenEvicted(t *testing.T) {
	partyId := types.PartyID(1)
	partyToRemove := partyId
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)

	recorder := &configAckRecorder{}
	testSetup.stubConsenter.AckConfigHandler = func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		recorder.record(req)
		return &protos.ConfigAckResponse{}, nil
	}

	testSetup.Start()
	defer testSetup.Stop()

	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
	waitForRouterServing(t, testSetup)

	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)
	require.NotNil(t, configUpdatePbData)
	testSetup.SendConfigUpdate(t, parties, configUpdatePbData, dir, 1)

	// the router detects its own eviction and transitions to pending admin at sequence 0.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// even though the router is evicted, it acknowledged the new config on sequence 1.
	require.Eventually(t, func() bool {
		return recorder.count() == 1
	}, 20*time.Second, 100*time.Millisecond)

	acks := recorder.all()
	require.Equal(t, protos.NodeType_ROUTER, acks[0].NodeType)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Shorten the config-ack submission timeout so the failing-ack path is fast.
//  2. Start a router, stub batcher, and stub consenter with the initial config.
//  3. Install an AckConfigHandler that always fails, so the ack never succeeds and
//     SubmitConfigAck exhausts its retries and times out.
//  4. Deliver a config update through the consenter path.
//  5. Verify that a failed ack is non-fatal to the router: it still advances to
//     config sequence 1 after the submission times out.
func TestRouterConfigAck_FailureIsNonFatal(t *testing.T) {
	origTimeout := configack.SubmitConfigAckTimeout
	configack.SubmitConfigAckTimeout = 1 * time.Second
	defer func() { configack.SubmitConfigAckTimeout = origTimeout }()

	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)

	var callCount int32
	testSetup.stubConsenter.AckConfigHandler = func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		atomic.AddInt32(&callCount, 1)
		return nil, fmt.Errorf("permanent failure")
	}

	testSetup.Start()
	defer testSetup.Stop()

	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
	waitForRouterServing(t, testSetup)

	testSetup.SendConfigUpdate(t, parties, autoRemoveTimeoutConfigUpdate(t, dir), dir, 1)

	// the ack never succeeds, but the router still applies the new config and
	// advances to config sequence 1 once the submission times out.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)

	// the router did attempt to send the ack.
	require.Greater(t, atomic.LoadInt32(&callCount), int32(0))
}
