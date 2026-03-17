/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"encoding/json"
	"fmt"
)

type NodeState int

const (
	StateInitializing NodeState = iota
	StateRunning
	StateStopped
	StateSoftStopped
	StatePendingAdmin
)

var nodeStateName = map[NodeState]string{
	StateInitializing: "initializing",
	StateRunning:      "running",
	StateStopped:      "stopped",
	StateSoftStopped:  "soft-stopped",
	StatePendingAdmin: "pending-admin",
}

func (s NodeState) String() string {
	if str, ok := nodeStateName[s]; ok {
		return str
	}
	return "unknown"
}

func (s NodeState) MarshalJSON() ([]byte, error) {
	if str, ok := nodeStateName[s]; ok {
		return json.Marshal(str)
	}
	return json.Marshal("unknown")
}

// TODO: add UnmarshalJSON for NodeState if needed

type NodeStatus struct {
	State                NodeState `json:"state"`
	ConfigSequenceNumber uint64    `json:"config_sequence_number"`
}

func (s *NodeStatus) SetState(state NodeState) {
	s.State = state
}

func (s *NodeStatus) SetConfigSequenceNumber(seq uint64) {
	s.ConfigSequenceNumber = seq
}

func (s *NodeStatus) Set(state NodeState, seq uint64) {
	s.State = state
	s.ConfigSequenceNumber = seq
}

func (s NodeStatus) Get() (NodeState, uint64) {
	return s.State, s.ConfigSequenceNumber
}

func (s NodeStatus) GetState() NodeState {
	return s.State
}

func (s NodeStatus) String() string {
	return "State: " + s.State.String() + ", ConfigSequenceNumber: " + fmt.Sprintf("%d", s.ConfigSequenceNumber)
}
