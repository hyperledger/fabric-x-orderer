/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"google.golang.org/protobuf/proto"
)

// ControlEvent is a single item consensus orders: exactly one of a batch attestation fragment
// (BAF), a Complaint, or a ConfigRequest.
type ControlEvent struct {
	BAF           types.BatchAttestationFragment
	Complaint     *Complaint
	ConfigRequest *ConfigRequest
}

// String returns a human-readable description of the control event's payload.
func (ce *ControlEvent) String() string {
	if ce.Complaint != nil {
		return ce.Complaint.String()
	} else if ce.BAF != nil {
		return ce.BAF.String()
	} else if ce.ConfigRequest != nil {
		return ce.ConfigRequest.String()
	}
	return "empty control event"
}

// ID returns a string representing the specific control event
func (ce *ControlEvent) ID() string {
	var payloadToHash []byte
	switch {
	case ce.BAF != nil:
		payloadToHash = make([]byte, 22+32) // seq and config sequence are uint64, signer, primary and shard are uint16, and digest is 32 bytes
		binary.BigEndian.PutUint64(payloadToHash, uint64(ce.BAF.Seq()))
		binary.BigEndian.PutUint64(payloadToHash[8:], uint64(ce.BAF.ConfigSequence()))
		binary.BigEndian.PutUint16(payloadToHash[16:], uint16(ce.BAF.Signer()))
		binary.BigEndian.PutUint16(payloadToHash[18:], uint16(ce.BAF.Primary()))
		binary.BigEndian.PutUint16(payloadToHash[20:], uint16(ce.BAF.Shard()))
		copy(payloadToHash[22:], ce.BAF.Digest())
	case ce.Complaint != nil:
		complaintWithNoSig := &Complaint{
			ShardTerm: ce.Complaint.ShardTerm,
			Signer:    ce.Complaint.Signer,
			Reason:    ce.Complaint.Reason,
			ConfigSeq: ce.Complaint.ConfigSeq,
		}
		payloadToHash = complaintWithNoSig.Bytes()
	case ce.ConfigRequest != nil:
		// TODO: maybe use a different ID for ConfigRequest
		payloadToHash = ce.ConfigRequest.Bytes()
	default:
		return ""
	}
	dig := sha256.Sum256(payloadToHash)
	return hex.EncodeToString(dig[:])
}

// SignerID returns a string representing the signer of the specific control event
func (ce *ControlEvent) SignerID() string {
	switch {
	case ce.BAF != nil:
		return fmt.Sprintf("%d", ce.BAF.Signer())
	case ce.Complaint != nil:
		return fmt.Sprintf("%d", ce.Complaint.Signer)
	case ce.ConfigRequest != nil:
		// TODO: add ConfigRequest SignerID
		return ""
	default:
		return ""
	}
}

// toProto converts the control event to its protobuf representation. It panics on an empty event.
func (ce *ControlEvent) toProto() *stateprotos.ControlEvent {
	protoEvent := &stateprotos.ControlEvent{}

	switch {
	case ce.BAF != nil:
		bafProto, ok := ce.BAF.(*types.SimpleBatchAttestationFragment)
		if !ok {
			panic("unexpected type for BAF")
		}
		protoEvent.Event = &stateprotos.ControlEvent_Baf{
			Baf: bafProto.ToProto(),
		}
	case ce.Complaint != nil:
		protoEvent.Event = &stateprotos.ControlEvent_Complaint{
			Complaint: ce.Complaint.toProto(),
		}
	case ce.ConfigRequest != nil:
		protoEvent.Event = &stateprotos.ControlEvent_ConfigRequest{
			ConfigRequest: ce.ConfigRequest.toProto(),
		}
	default:
		panic("empty control event")
	}

	return protoEvent
}

// fromProto populates the control event from its protobuf representation, clearing any existing payload.
func (ce *ControlEvent) fromProto(pe *stateprotos.ControlEvent) error {
	ce.BAF = nil
	ce.Complaint = nil
	ce.ConfigRequest = nil

	switch event := pe.Event.(type) {
	case *stateprotos.ControlEvent_Baf:
		if event.Baf == nil {
			return fmt.Errorf("BAF event payload is nil")
		}
		baf := &types.SimpleBatchAttestationFragment{}
		if err := baf.FromProto(event.Baf); err != nil {
			return err
		}
		ce.BAF = baf
	case *stateprotos.ControlEvent_Complaint:
		if event.Complaint == nil {
			return fmt.Errorf("Complaint event payload is nil")
		}
		ce.Complaint = &Complaint{}
		if err := ce.Complaint.fromProto(event.Complaint); err != nil {
			return err
		}
	case *stateprotos.ControlEvent_ConfigRequest:
		if event.ConfigRequest == nil {
			return fmt.Errorf("ConfigRequest event payload is nil")
		}
		ce.ConfigRequest = &ConfigRequest{}
		if err := ce.ConfigRequest.fromProto(event.ConfigRequest); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown control event type")
	}

	return nil
}

// Bytes returns the serialized protobuf encoding of the control event. It panics on marshal failure.
func (ce *ControlEvent) Bytes() []byte {
	protoEvent := ce.toProto()
	bytes, err := proto.Marshal(protoEvent)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal control event: %v", err))
	}
	return bytes
}

// FromBytes populates the control event by deserializing its protobuf encoding.
func (ce *ControlEvent) FromBytes(bytes []byte) error {
	protoEvent := &stateprotos.ControlEvent{}
	if err := proto.Unmarshal(bytes, protoEvent); err != nil {
		return fmt.Errorf("failed to unmarshal control event: %v", err)
	}
	return ce.fromProto(protoEvent)
}
