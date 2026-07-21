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

type Complaint struct {
	ShardTerm
	Signer    types.PartyID
	Signature []byte
	Reason    string
	ConfigSeq types.ConfigSequence
}

func (c *Complaint) Bytes() []byte {
	reasonLen := len([]byte(c.Reason))
	if reasonLen > math.MaxUint16 {
		reasonLen = math.MaxUint16
	}
	buff := make([]byte, 24+len(c.Signature)+reasonLen)
	var pos int
	binary.BigEndian.PutUint64(buff, uint64(c.ConfigSeq))
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(c.Shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], c.Term)
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(c.Signer))
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(len(c.Signature)))
	pos += 2
	copy(buff[pos:pos+len(c.Signature)], c.Signature)
	pos += len(c.Signature)
	binary.BigEndian.PutUint16(buff[pos:], uint16(reasonLen))
	pos += 2
	copy(buff[pos:pos+reasonLen], []byte(c.Reason))
	return buff
}

func (c *Complaint) FromBytes(bytes []byte) error {
	if len(bytes) <= 24 {
		return fmt.Errorf("input too small (%d <= 24)", len(bytes))
	}
	c.ConfigSeq = types.ConfigSequence(binary.BigEndian.Uint64(bytes))
	c.Shard = types.ShardID(binary.BigEndian.Uint16(bytes[8:10]))
	c.Term = binary.BigEndian.Uint64(bytes[10:18])
	c.Signer = types.PartyID(binary.BigEndian.Uint16(bytes[18:20]))
	sigSize := binary.BigEndian.Uint16(bytes[20:22])
	c.Signature = bytes[22 : 22+sigSize]
	rSize := binary.BigEndian.Uint16(bytes[22+sigSize : 22+sigSize+2])
	c.Reason = string(bytes[22+int(sigSize)+2 : 22+int(sigSize)+2+int(rSize)])
	return nil
}

func (c *Complaint) toProto() *stateprotos.Complaint {
	return &stateprotos.Complaint{
		ConfigSeq: uint64(c.ConfigSeq),
		Shard:     uint32(c.Shard),
		Term:      c.Term,
		Signer:    uint32(c.Signer),
		Signature: c.Signature,
		Reason:    c.Reason,
	}
}

func (c *Complaint) fromProto(pc *stateprotos.Complaint) error {
	if pc.GetShard() > math.MaxUint16 {
		return fmt.Errorf("the Complaint Shard value %d at exceeds uint16 maximum %d", pc.Shard, math.MaxUint16)
	}
	if pc.GetSigner() > math.MaxUint16 {
		return fmt.Errorf("the Complaint Signer value %d at exceeds uint16 maximum %d", pc.Signer, math.MaxUint16)
	}
	c.ConfigSeq = types.ConfigSequence(pc.GetConfigSeq())
	c.Shard = types.ShardID(pc.GetShard())
	c.Term = pc.GetTerm()
	c.Signer = types.PartyID(pc.GetSigner())
	c.Signature = pc.GetSignature()
	c.Reason = pc.GetReason()
	return nil
}

func (c *Complaint) ToBeSigned() []byte {
	toBeSignedComplaint := Complaint{
		ShardTerm: c.ShardTerm,
		Signer:    c.Signer,
		Signature: nil,
		Reason:    c.Reason,
		ConfigSeq: c.ConfigSeq,
	}
	return toBeSignedComplaint.Bytes()
}

func (c *Complaint) String() string {
	return fmt.Sprintf("Complaint: Signer: %d; Shard: %d; Term %d; Config Seq: %d; Reason: %s", c.Signer, c.Shard, c.Term, c.ConfigSeq, c.Reason)
}
