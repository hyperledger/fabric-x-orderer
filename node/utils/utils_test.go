/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils_test

import (
	"encoding/json"
	"syscall"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/stretchr/testify/require"
)

func TestTLSCAcertsFromShards(t *testing.T) {
	// 4 parties, 2 shards - each party has its own CA, expect for 4 CA's
	numParties := 4
	shardIDs := []types.ShardID{types.ShardID(1), types.ShardID(2)}
	shards, cleanup := stub.NewStubBatchersAndInfos(t, numParties, shardIDs)
	require.NotNil(t, shards)
	defer cleanup()
	tlsCAsFromShards := utils.TLSCAcertsFromShards(shards)
	require.Equal(t, len(tlsCAsFromShards), 4)

	// 4 parties, 1 shards - each party has its own CA, expect for 4 CA's
	shardIDs = []types.ShardID{types.ShardID(1)}
	shards, cleanup = stub.NewStubBatchersAndInfos(t, numParties, shardIDs)
	require.NotNil(t, shards)
	defer cleanup()
	tlsCAsFromShards = utils.TLSCAcertsFromShards(shards)
	require.Equal(t, len(tlsCAsFromShards), 4)

	// 3 parties, 1 shards - each party has its own CA, expect for 3 CA's
	numParties = 3
	shards, cleanup = stub.NewStubBatchersAndInfos(t, numParties, shardIDs)
	require.NotNil(t, shards)
	defer cleanup()
	tlsCAsFromShards = utils.TLSCAcertsFromShards(shards)
	require.Equal(t, len(tlsCAsFromShards), 3)
}

func TestTLSCAcertsFromConsenters(t *testing.T) {
	// 3 parties - each party has its own CA, expect for 3 CA's
	numParties := 3

	consentersInfo, cleanup := stub.NewStubConsentersAndInfo(t, numParties)
	require.NotNil(t, consentersInfo)
	defer cleanup()

	tlsCAsFromShards := utils.TLSCAcertsFromConsenters(consentersInfo)
	require.Equal(t, len(tlsCAsFromShards), 3)
}

func TestNodeStatusSettersGettersAndString(t *testing.T) {
	var s utils.NodeStatus

	s.SetState(utils.StateRunning)
	require.Equal(t, utils.StateRunning, s.GetState())

	s.SetConfigSequenceNumber(42)
	st, seq := s.Get()
	require.Equal(t, utils.StateRunning, st)
	require.Equal(t, uint64(42), seq)

	s.Set(utils.StateStopped, 7)
	st2, seq2 := s.Get()
	require.Equal(t, utils.StateStopped, st2)
	require.Equal(t, uint64(7), seq2)

	want := "State: " + utils.StateStopped.String() + ", ConfigSequenceNumber: 7"
	require.Equal(t, want, s.String())
}

func TestNodeStateStringAndMarshal(t *testing.T) {
	tests := []struct {
		state utils.NodeState
		want  string
	}{
		{utils.StateInitializing, "initializing"},
		{utils.StateRunning, "running"},
		{utils.StateStopped, "stopped"},
		{utils.StateSoftStopped, "soft-stopped"},
		{utils.StatePendingAdmin, "pending-admin"},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, tt.state.String())

		b, err := json.Marshal(tt.state)
		require.NoError(t, err)
		var s string
		require.NoError(t, json.Unmarshal(b, &s))
		require.Equal(t, tt.want, s)
	}

	// unknown state
	badState := utils.NodeState(999)
	require.Equal(t, "unknown", badState.String())
	b, err := json.Marshal(badState)
	require.NoError(t, err)
	var s string
	require.NoError(t, json.Unmarshal(b, &s))
	require.Equal(t, "unknown", s)

	// NodeStatus JSON marshal
	sStatus := utils.NodeStatus{
		State:                utils.StateRunning,
		ConfigSequenceNumber: 42,
	}
	b, err = json.Marshal(sStatus)
	require.NoError(t, err)
	require.JSONEq(t, `{"state":"running","config_sequence_number":42}`, string(b))
}

// mockStopper records invocations of Stop by sending on a channel.
type mockStopper struct {
	stopped chan struct{}
}

func newMockStopper() *mockStopper {
	return &mockStopper{stopped: make(chan struct{}, 1)}
}

func (m *mockStopper) Stop() {
	m.stopped <- struct{}{}
}

func TestStopSignalListen(t *testing.T) {
	stopper := newMockStopper()
	logger := flogging.MustGetLogger("test.signal_listener")

	utils.StopSignalListen(stopper, logger, "127.0.0.1:0")

	// wait for a short time to ensure the signal listener is set up before sending the signal
	time.Sleep(200 * time.Millisecond)

	// Send SIGTERM to the current process; the listener should invoke Stop.
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))

	select {
	case <-stopper.stopped:
		// Stop was called as expected.
	case <-time.After(10 * time.Second):
		t.Fatal("expected Stop to be called after SIGTERM, but it was not")
	}
}
