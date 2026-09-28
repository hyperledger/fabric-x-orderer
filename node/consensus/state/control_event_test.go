/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/stretchr/testify/assert"
)

func TestControlEventSerialization(t *testing.T) {
	// Serialization and deserialization of ControlEvent with Complaint
	ce := consensus_state.ControlEvent{Complaint: &complaint}

	var ce2 consensus_state.ControlEvent

	err := ce2.FromBytes(ce.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with BAF
	baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{3}, types.PartyID(2), 0, 0, nil)
	baf.SetSignature([]byte{4})
	ce = consensus_state.ControlEvent{BAF: baf}

	ce2.Complaint = nil
	err = ce2.FromBytes(ce.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, ce, ce2)

	// Serialization and deserialization of ControlEvent with ConfigRequest
	cr := &consensus_state.ConfigRequest{
		Envelope: &common.Envelope{
			Payload:   []byte("config-payload"),
			Signature: []byte("config-signature"),
		},
	}
	ce = consensus_state.ControlEvent{ConfigRequest: cr}

	var ce3 consensus_state.ControlEvent
	err = ce3.FromBytes(ce.Bytes())
	assert.NoError(t, err)
	assert.NotNil(t, ce3.ConfigRequest)
	assert.Equal(t, cr.Envelope.Payload, ce3.ConfigRequest.Envelope.Payload)
	assert.Equal(t, cr.Envelope.Signature, ce3.ConfigRequest.Envelope.Signature)
}

// bafCE builds a BAF control event with the given identity fields. txCount and primarySignature
// are deliberately parameterized so tests can assert they are excluded from ID().
func bafCE(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest []byte, signer types.PartyID, configSeq types.ConfigSequence, txCount uint64, primarySig []byte) consensus_state.ControlEvent {
	baf := types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, signer, configSeq, txCount, primarySig)
	baf.SetSignature([]byte{9, 9, 9})
	return consensus_state.ControlEvent{BAF: baf}
}

// TestControlEventID pins the identity contract that SmartBFT's request-pool dedup relies on
// (consensus.go feeds ControlEvent.ID() as the RequestInfo ID). Two properties matter:
//   - stability: the same logical event always hashes to the same ID;
//   - the equivocation invariant: two BAFs for the same <seq, configSeq, signer, primary, shard>
//     but DIFFERENT digests must NOT collide — otherwise the second (conflicting) BAF would be
//     dropped as a duplicate and equivocation would go undetected.
func TestControlEventID(t *testing.T) {
	t.Run("empty control event", func(t *testing.T) {
		ce := consensus_state.ControlEvent{}
		assert.Empty(t, ce.ID())
	})

	t.Run("BAF ID is stable", func(t *testing.T) {
		a := bafCE(1, 2, 3, []byte{1, 2, 3}, 4, 5, 0, nil)
		b := bafCE(1, 2, 3, []byte{1, 2, 3}, 4, 5, 0, nil)
		assert.NotEmpty(t, a.ID())
		assert.Equal(t, a.ID(), b.ID())
	})

	t.Run("BAF ID ignores fields outside the identity tuple", func(t *testing.T) {
		// txCount, primary signature, and the fragment signature are not part of the hashed tuple.
		base := bafCE(1, 2, 3, []byte{1, 2, 3}, 4, 5, 0, nil)
		diffTxCount := bafCE(1, 2, 3, []byte{1, 2, 3}, 4, 5, 99, []byte{7})
		assert.Equal(t, base.ID(), diffTxCount.ID())
	})

	t.Run("BAF ID distinguishes every identity field", func(t *testing.T) {
		base := bafCE(1, 2, 3, []byte{1, 2, 3}, 4, 5, 0, nil)
		cases := map[string]consensus_state.ControlEvent{
			"digest":    bafCE(1, 2, 3, []byte{9, 9, 9}, 4, 5, 0, nil),
			"shard":     bafCE(2, 2, 3, []byte{1, 2, 3}, 4, 5, 0, nil),
			"primary":   bafCE(1, 3, 3, []byte{1, 2, 3}, 4, 5, 0, nil),
			"seq":       bafCE(1, 2, 4, []byte{1, 2, 3}, 4, 5, 0, nil),
			"signer":    bafCE(1, 2, 3, []byte{1, 2, 3}, 5, 5, 0, nil),
			"configSeq": bafCE(1, 2, 3, []byte{1, 2, 3}, 4, 6, 0, nil),
		}
		for field, ce := range cases {
			assert.NotEqual(t, base.ID(), ce.ID(), "changing %s must change the ID", field)
		}
	})

	t.Run("complaint ID excludes the signature", func(t *testing.T) {
		// ID() hashes <ShardTerm, Signer, Reason, ConfigSeq> only; two complaints that differ
		// solely in their signature are the same logical event and must share an ID.
		base := consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3, Signature: []byte{1}, Reason: "r", ConfigSeq: 10}
		diffSig := base
		diffSig.Signature = []byte{2, 3, 4}
		ceA := consensus_state.ControlEvent{Complaint: &base}
		ceB := consensus_state.ControlEvent{Complaint: &diffSig}
		assert.NotEmpty(t, ceA.ID())
		assert.Equal(t, ceA.ID(), ceB.ID())

		// A different signer is a distinct complaint.
		diffSigner := base
		diffSigner.Signer = 4
		ceC := consensus_state.ControlEvent{Complaint: &diffSigner}
		assert.NotEqual(t, ceA.ID(), ceC.ID())
	})
}

// TestControlEventSignerID pins the ClientID that SmartBFT uses for per-signer request accounting.
func TestControlEventSignerID(t *testing.T) {
	baf := bafCE(1, 2, 3, []byte{1, 2, 3}, 7, 5, 0, nil)
	assert.Equal(t, "7", baf.SignerID())

	c := consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3, ConfigSeq: 10}
	ceComplaint := consensus_state.ControlEvent{Complaint: &c}
	assert.Equal(t, "3", ceComplaint.SignerID())

	// ConfigRequest has no signer wired up yet, and an empty event has none either.
	ceConfig := consensus_state.ControlEvent{ConfigRequest: &consensus_state.ConfigRequest{}}
	assert.Empty(t, ceConfig.SignerID())
	empty := consensus_state.ControlEvent{}
	assert.Empty(t, empty.SignerID())
}

// TestControlEventString covers the payload-selection branches, including the empty sentinel.
func TestControlEventString(t *testing.T) {
	empty := consensus_state.ControlEvent{}
	assert.Equal(t, "empty control event", empty.String())

	c := consensus_state.Complaint{ShardTerm: consensus_state.ShardTerm{Shard: 1, Term: 1}, Signer: 3, ConfigSeq: 10}
	ceComplaint := consensus_state.ControlEvent{Complaint: &c}
	assert.Equal(t, c.String(), ceComplaint.String())

	baf := bafCE(1, 2, 3, []byte{1, 2, 3}, 7, 5, 0, nil)
	assert.Equal(t, baf.BAF.String(), baf.String())
}
