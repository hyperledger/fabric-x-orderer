/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"fmt"
	"math"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
)

// AssemblerHeight is the highest committed decision number an assembler party reported.
type AssemblerHeight struct {
	Party       types.PartyID
	DecisionNum types.DecisionNum
}

func (h *AssemblerHeight) toProto() *stateprotos.AssemblerHeight {
	return &stateprotos.AssemblerHeight{
		Party:       uint32(h.Party),
		DecisionNum: uint64(h.DecisionNum),
	}
}

func (h *AssemblerHeight) fromProto(ph *stateprotos.AssemblerHeight) error {
	if ph.GetParty() > math.MaxUint16 {
		return fmt.Errorf("the AssemblerHeight Party value %d exceeds uint16 maximum %d", ph.Party, math.MaxUint16)
	}
	h.Party = types.PartyID(ph.GetParty())
	h.DecisionNum = types.DecisionNum(ph.GetDecisionNum())
	return nil
}

// PrunePoint is the agreed prune point of the batch ledger of <Shard, Primary>: batchers may drop
// every batch of that ledger whose sequence is below Seq.
type PrunePoint struct {
	Shard   types.ShardID
	Primary types.PartyID
	Seq     types.BatchSequence
}

func (p *PrunePoint) toProto() *stateprotos.PrunePoint {
	return &stateprotos.PrunePoint{
		Shard:   uint32(p.Shard),
		Primary: uint32(p.Primary),
		Seq:     uint64(p.Seq),
	}
}

func (p *PrunePoint) fromProto(pp *stateprotos.PrunePoint) error {
	if pp.GetShard() > math.MaxUint16 {
		return fmt.Errorf("the PrunePoint Shard value %d exceeds uint16 maximum %d", pp.Shard, math.MaxUint16)
	}
	if pp.GetPrimary() > math.MaxUint16 {
		return fmt.Errorf("the PrunePoint Primary value %d exceeds uint16 maximum %d", pp.Primary, math.MaxUint16)
	}
	p.Shard = types.ShardID(pp.GetShard())
	p.Primary = types.PartyID(pp.GetPrimary())
	p.Seq = types.BatchSequence(pp.GetSeq())
	return nil
}
