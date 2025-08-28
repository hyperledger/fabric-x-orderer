/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

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

// PrepareTxWithTimestamp is used only in performance testing and its content consists of the tx number, the time stamp (creation time) and the session number the tx is sent through
func PrepareTxWithTimestamp(txNumber int, txSize int, sessionNumber []byte) []byte {
	// create timestamp (8 bytes)
	timeStamp := uint64(time.Now().UnixNano())

	// prepare the payload data
	buffer := make([]byte, txSize)
	buff := bytes.NewBuffer(buffer[:0])
	binary.Write(buff, binary.BigEndian, uint64(txNumber))
	binary.Write(buff, binary.BigEndian, timeStamp)
	buff.Write(sessionNumber)
	result := buff.Bytes()
	if len(buff.Bytes()) < txSize {
		padding := make([]byte, txSize-len(result))
		result = append(result, padding...)
	}
	return result
}

// ExtractTimestampFromTx extracts the time stamp from its content (payload.data)
// This function assumes that the tx structure is: txNumber (8 bytes), timeStamp (8 bytes) and session number (16 bytes).
func ExtractTimestampFromTx(data []byte) time.Time {
	readPayload := bytes.NewBuffer(data)
	startPosition := 8
	readPayload.Next(startPosition)
	var extractedSendTime uint64
	binary.Read(readPayload, binary.BigEndian, &extractedSendTime)
	sendTime := time.Unix(0, int64(extractedSendTime))
	return sendTime
}
