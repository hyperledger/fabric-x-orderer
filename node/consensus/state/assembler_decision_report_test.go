/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestAssemblerDecisionReportSerialization(t *testing.T) {
	t.Run("round-trips with a signature", func(t *testing.T) {
		report := consensus_state.AssemblerDecisionReport{
			Party:       types.PartyID(2),
			DecisionNum: types.DecisionNum(100),
			Signature:   []byte{1, 2, 3},
		}

		var report2 consensus_state.AssemblerDecisionReport
		err := report2.FromBytes(report.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, report, report2)
	})

	t.Run("round-trips without a signature", func(t *testing.T) {
		report := consensus_state.AssemblerDecisionReport{
			Party:       types.PartyID(2),
			DecisionNum: types.DecisionNum(100),
		}

		var report2 consensus_state.AssemblerDecisionReport
		err := report2.FromBytes(report.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, report, report2)
	})

	t.Run("round-trips a signature larger than 64 KiB", func(t *testing.T) {
		// The signature length field is a uint32; a uint16 field would wrap at 65536 and truncate
		// the round trip. The configured request limit can be as large as 1 MiB.
		sig := bytes.Repeat([]byte{0xAB}, 70000)
		report := consensus_state.AssemblerDecisionReport{
			Party:       types.PartyID(2),
			DecisionNum: types.DecisionNum(100),
			Signature:   sig,
		}

		var report2 consensus_state.AssemblerDecisionReport
		err := report2.FromBytes(report.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, report, report2)
	})
}

// TestAssemblerDecisionReportRejectsPartyZero verifies that a report decoded from protobuf with a
// zero party is rejected: PartyID must be greater than zero (common/types/types.go), otherwise an
// unsigned-check-passing report with party 0 would be accepted downstream.
func TestAssemblerDecisionReportRejectsPartyZero(t *testing.T) {
	protoCE := &stateprotos.ControlEvent{
		Event: &stateprotos.ControlEvent_AssemblerDecisionReport{
			AssemblerDecisionReport: &stateprotos.AssemblerDecisionReport{
				Party:       0,
				DecisionNum: 100,
				Signature:   []byte{1, 2, 3},
			},
		},
	}
	raw, err := proto.Marshal(protoCE)
	require.NoError(t, err)

	var ce consensus_state.ControlEvent
	err = ce.FromBytes(raw)
	require.Error(t, err)
	require.Contains(t, err.Error(), "greater than zero")
}
