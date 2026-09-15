/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
)

// AssemblerDecisionReport is an assembler's report of the highest global decision number it has
// committed. Consensus aggregates these across parties to agree on a durable pruning watermark.
type AssemblerDecisionReport struct {
	Party       types.PartyID
	DecisionNum types.DecisionNum
	Signature   []byte
}

// reportHeaderLen is the fixed prefix: Party (uint16) + DecisionNum (uint64) + len(Signature) (uint32).
const reportHeaderLen = 2 + 8 + 4

// Bytes serializes the report as <Party, DecisionNum, len(Signature), Signature>. The signature is
// appended (not hashed away) so Bytes/FromBytes round-trips the full value; ID() hashes a
// signature-less encoding separately. The length is a uint32 so the full value survives even for
// signatures larger than 64 KiB.
func (r *AssemblerDecisionReport) Bytes() []byte {
	buff := make([]byte, reportHeaderLen+len(r.Signature))
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], uint16(r.Party))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], uint64(r.DecisionNum))
	pos += 8
	binary.BigEndian.PutUint32(buff[pos:], uint32(len(r.Signature)))
	pos += 4
	copy(buff[pos:pos+len(r.Signature)], r.Signature)
	return buff
}

func (r *AssemblerDecisionReport) FromBytes(bytes []byte) error {
	if len(bytes) < reportHeaderLen {
		return fmt.Errorf("input too small (%d < %d)", len(bytes), reportHeaderLen)
	}
	r.Party = types.PartyID(binary.BigEndian.Uint16(bytes[0:2]))
	r.DecisionNum = types.DecisionNum(binary.BigEndian.Uint64(bytes[2:10]))
	sigSize := int(binary.BigEndian.Uint32(bytes[10:14]))
	sigEnd := reportHeaderLen + sigSize
	if sigEnd > len(bytes) {
		return fmt.Errorf("input too small for signature (%d < %d)", len(bytes), sigEnd)
	}
	if sigSize == 0 {
		r.Signature = nil
	} else {
		r.Signature = append([]byte(nil), bytes[reportHeaderLen:sigEnd]...)
	}
	return nil
}

func (r *AssemblerDecisionReport) toProto() *stateprotos.AssemblerDecisionReport {
	return &stateprotos.AssemblerDecisionReport{
		Party:       uint32(r.Party),
		DecisionNum: uint64(r.DecisionNum),
		Signature:   r.Signature,
	}
}

func (r *AssemblerDecisionReport) fromProto(pr *stateprotos.AssemblerDecisionReport) error {
	if pr.GetParty() == 0 {
		return fmt.Errorf("the AssemblerDecisionReport Party value must be greater than zero")
	}
	if pr.GetParty() > math.MaxUint16 {
		return fmt.Errorf("the AssemblerDecisionReport Party value %d exceeds uint16 maximum %d", pr.Party, math.MaxUint16)
	}
	r.Party = types.PartyID(pr.GetParty())
	r.DecisionNum = types.DecisionNum(pr.GetDecisionNum())
	r.Signature = pr.GetSignature()
	return nil
}

func (r *AssemblerDecisionReport) String() string {
	return fmt.Sprintf("AssemblerDecisionReport: Party: %d; DecisionNum: %d", r.Party, r.DecisionNum)
}
