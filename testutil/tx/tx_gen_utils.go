/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

func createStructuredPayload(data []byte) *common.Payload {
	payloadChannelHeader := protoutil.MakeChannelHeader(common.HeaderType_MESSAGE, 0, "channelID", 0)
	payloadSignatureHeader := &common.SignatureHeader{
		Creator: []byte("creator"),
		Nonce:   []byte("nonce"),
	}
	return &common.Payload{
		Header: protoutil.MakePayloadHeader(payloadChannelHeader, payloadSignatureHeader),
		Data:   data,
	}
}

func CreateStructuredEnvelope(data []byte) *common.Envelope {
	payload := createStructuredPayload(data)
	payloadBytes := protoutil.MarshalOrPanic(payload)
	return &common.Envelope{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
}

func CreateStructuredRequest(data []byte) *protos.Request {
	payload := createStructuredPayload(data)
	payloadBytes := protoutil.MarshalOrPanic(payload)
	return &protos.Request{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
}

func GetDataFromEnvelope(env *common.Envelope) ([]byte, error) {
	if env == nil {
		return nil, fmt.Errorf("bad envelope")
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}
	return payload.Data, nil
}
