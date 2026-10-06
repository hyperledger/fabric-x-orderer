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
			ConfigSeq:   types.ConfigSequence(3),
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
			ConfigSeq:   types.ConfigSequence(3),
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
			ConfigSeq:   types.ConfigSequence(3),
			Signature:   sig,
		}

		var report2 consensus_state.AssemblerDecisionReport
		err := report2.FromBytes(report.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, report, report2)
	})
}

// TestAssemblerDecisionReportToBeSigned verifies that ToBeSigned is signature-independent (so a
// signature verifies regardless of the Signature field's current value) and is bound to the
// assembler-report signature domain (so it can never be replayed as a signature over another
// message family).
func TestAssemblerDecisionReportToBeSigned(t *testing.T) {
	report := consensus_state.AssemblerDecisionReport{
		Party:       types.PartyID(2),
		DecisionNum: types.DecisionNum(100),
		ConfigSeq:   types.ConfigSequence(3),
		Signature:   []byte{1, 2, 3},
	}

	reportNoSig := consensus_state.AssemblerDecisionReport{
		Party:       types.PartyID(2),
		DecisionNum: types.DecisionNum(100),
		ConfigSeq:   types.ConfigSequence(3),
	}

	t.Run("is independent of the Signature field", func(t *testing.T) {
		assert.Equal(t, reportNoSig.ToBeSigned(), report.ToBeSigned())
	})

	t.Run("is bound to the assembler-report domain", func(t *testing.T) {
		expected := types.PrefixWithDomain(types.DomainAssemblerDecisionReport, reportNoSig.Bytes())
		assert.Equal(t, expected, report.ToBeSigned())
	})

	t.Run("is not in the complaint or BAF domain", func(t *testing.T) {
		assert.True(t, bytes.HasPrefix(report.ToBeSigned(), types.PrefixWithDomain(types.DomainAssemblerDecisionReport, nil)))
		assert.False(t, bytes.HasPrefix(report.ToBeSigned(), types.PrefixWithDomain(types.DomainComplaint, nil)))
		assert.False(t, bytes.HasPrefix(report.ToBeSigned(), types.PrefixWithDomain(types.DomainBAF, nil)))
	})

	t.Run("changes when a signed field changes", func(t *testing.T) {
		otherParty := reportNoSig
		otherParty.Party = types.PartyID(3)
		assert.NotEqual(t, reportNoSig.ToBeSigned(), otherParty.ToBeSigned())

		otherDecision := reportNoSig
		otherDecision.DecisionNum = types.DecisionNum(101)
		assert.NotEqual(t, reportNoSig.ToBeSigned(), otherDecision.ToBeSigned())

		otherConfigSeq := reportNoSig
		otherConfigSeq.ConfigSeq = types.ConfigSequence(4)
		assert.NotEqual(t, reportNoSig.ToBeSigned(), otherConfigSeq.ToBeSigned())
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
