/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type ConfigRequest struct {
	Envelope *common.Envelope
}

func (c *ConfigRequest) ConfigSequence() (types.ConfigSequence, error) {
	payload, err := protoutil.UnmarshalPayload(c.Envelope.Payload)
	if err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal payload")
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal config envelope")
	}

	if configEnvelope.Config == nil {
		return 0, errors.New("config envelope has nil config")
	}

	return types.ConfigSequence(configEnvelope.Config.Sequence), nil
}

func (c *ConfigRequest) toProto() *stateprotos.ConfigRequest {
	envelopeBytes, err := protoutil.Marshal(c.Envelope)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal envelope: %v", err))
	}
	return &stateprotos.ConfigRequest{
		Envelope: envelopeBytes,
	}
}

func (c *ConfigRequest) fromProto(pc *stateprotos.ConfigRequest) error {
	envelope, err := protoutil.UnmarshalEnvelope(pc.Envelope)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal envelope")
	}
	c.Envelope = envelope
	return nil
}

func (c *ConfigRequest) Bytes() []byte {
	protoConfigRequest := c.toProto()
	bytes, err := proto.Marshal(protoConfigRequest)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal config request: %v", err))
	}
	return bytes
}

func (c *ConfigRequest) FromBytes(bytes []byte) error {
	protoConfigRequest := &stateprotos.ConfigRequest{}
	if err := proto.Unmarshal(bytes, protoConfigRequest); err != nil {
		return errors.Wrap(err, "failed to unmarshal config request")
	}
	return c.fromProto(protoConfigRequest)
}

func (c *ConfigRequest) String() string {
	configSeq, err := c.ConfigSequence()
	if err != nil {
		return fmt.Sprintf("Config Request with error: %v", err)
	}
	return fmt.Sprintf("Config Request with config sequence %d", configSeq)
}
