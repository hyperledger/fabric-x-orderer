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
	StateStopping
	StateStopped
	StateSoftStopped
	StatePendingAdmin
)

type NodeStatus struct {
	state                NodeState
	configSequenceNumber uint64
}

func (s *NodeStatus) SetState(state NodeState) {
	s.state = state
}

func (s *NodeStatus) SetConfigSequenceNumber(seq uint64) {
	s.configSequenceNumber = seq
}

func (s *NodeStatus) Set(state NodeState, seq uint64) {
	s.state = state
	s.configSequenceNumber = seq
}

func (s *NodeStatus) Get() (NodeState, uint64) {
	return s.state, s.configSequenceNumber
}

func (s *NodeStatus) GetState() NodeState {
	return s.state
}

func (s *NodeStatus) CompareAndSwapState(to NodeState, from ...NodeState) bool {
	for _, f := range from {
		if s.state == f {
			s.state = to
			return true
		}
	}
	return false
}

func (s NodeState) String() string {
	switch s {
	case StateInitializing:
		return "initializing"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	case StateSoftStopped:
		return "soft-stopped"
	case StatePendingAdmin:
		return "pending-admin"
	default:
		return "unknown"
	}
}

func (s NodeStatus) String() string {
	return "State: " + s.state.String() + ", ConfigSequenceNumber: " + fmt.Sprintf("%d", s.configSequenceNumber)
}

type nodeStatusJSON struct {
	State                string `json:"state"`
	ConfigSequenceNumber uint64 `json:"config_sequence_number"`
}

func (s NodeStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(nodeStatusJSON{
		State:                s.state.String(),
		ConfigSequenceNumber: s.configSequenceNumber,
	})
}
