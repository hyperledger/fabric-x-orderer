/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"

	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Start an assembler, stub batcher, and stub consenter with the initial config.
//  2. Record the ConfigAck the assembler sends by defining an AckConfigHandler on the stub consenter.
//  3. Deliver a config update (batching timeout change) through the consenter path.
//  4. Verify the assembler advances to config sequence 1 and that it sent exactly one
//     ConfigAck identifying itself as the assembler (NodeType=ASSEMBLER, Shard=0) on sequence 1.
func TestAssemblerSendsConfigAck(t *testing.T) {
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

	// wait for the assembler to be running with the initial config sequence.
	require.Eventually(t, func() bool {
		status := testSetup.assemblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	testSetup.SendConfigUpdate(t, autoRemoveTimeoutConfigUpdate(t, dir), parties, dir, 1)

	// the assembler applies the new config and advances to config sequence 1.
	require.Eventually(t, func() bool {
		status := testSetup.assemblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)
	requireAssemblerServing(t, testSetup)

	acks := recorder.all()
	require.Len(t, acks, 1)
	require.Equal(t, protos.NodeType_ASSEMBLER, acks[0].NodeType)
	require.EqualValues(t, 0, acks[0].Shard)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Start an assembler, stub batcher, and stub consenter with the initial config.
//  2. Define an AckConfigHandler that rejects the first few attempts, then accepts.
//  3. Deliver a config update through the consenter path.
//  4. Verify the assembler's sender retried (more than one attempt), the consenter
//     eventually recorded an ack on sequence 1, and the assembler still advanced to
//     config sequence 1.
func TestAssemblerConfigAckRetriesUntilConsenterAccepts(t *testing.T) {
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
		status := testSetup.assemblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	testSetup.SendConfigUpdate(t, autoRemoveTimeoutConfigUpdate(t, dir), parties, dir, 1)

	// despite the initial failures, the assembler eventually applies the new config.
	require.Eventually(t, func() bool {
		status := testSetup.assemblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)
	requireAssemblerServing(t, testSetup)

	require.Greater(t, atomic.LoadInt32(&callCount), int32(1))
	acks := recorder.all()
	require.Len(t, acks, 1)
	require.Equal(t, protos.NodeType_ASSEMBLER, acks[0].NodeType)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Start an assembler, stub batcher, and stub consenter with the initial config.
//  2. Record the ConfigAck the assembler sends.
//  3. Deliver a config update that removes (evicts) the assembler's own party.
//  4. Verify the assembler still sends a ConfigAck on sequence 1 before transitioning
//     to pending admin. This documents that SubmitConfigAck runs before the eviction
//     check in ProcessNewConfigBlock, so the consenter learns the evicted node saw the config.
func TestAssemblerSendsConfigAckEvenWhenEvicted(t *testing.T) {
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
		status := testSetup.assemblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)
	require.NotNil(t, configUpdatePbData)
	testSetup.SendConfigUpdate(t, configUpdatePbData, parties, dir, 1)

	// the assembler detects its own eviction and transitions to pending admin at sequence 0.
	require.Eventually(t, func() bool {
		status := testSetup.assemblerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	acks := recorder.all()
	require.Len(t, acks, 1)
	require.Equal(t, protos.NodeType_ASSEMBLER, acks[0].NodeType)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// configAckRecorder captures the ConfigAck requests the assembler sends to the
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

func (r *configAckRecorder) all() []*protos.ConfigAck {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*protos.ConfigAck(nil), r.acks...)
}

// requireAssemblerServing pulls a block from the assembler, to be sure the assembler's network
// service is fully up after the dynamic restart (and thus stops cleanly). StartAssemblerService
// launches srv.Start() in a goroutine and sets the status to Running immediately, so "Running"
// does not imply the assembler can accept connections.
func requireAssemblerServing(t *testing.T, s *reconfigTestSetup) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	err = createDeliveryClientAndPull(t, s.assemblerNode.Address(), nil, ca, 1, "none", s.signer)
	require.NoError(t, err)
}
