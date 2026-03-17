/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils_test

import (
	"encoding/json"
	"testing"

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
