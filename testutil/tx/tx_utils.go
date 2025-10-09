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
	"google.golang.org/protobuf/proto"
)

func createChannelHeader(headerType common.HeaderType, version int32, chainID string, epoch uint64) *common.ChannelHeader {
	return &common.ChannelHeader{
		Type:      int32(headerType),
		Version:   version,
		ChannelId: chainID,
		Epoch:     epoch,
	}
}

func createPayloadHeader(ch *common.ChannelHeader, sh *common.SignatureHeader) *common.Header {
	opts := proto.MarshalOptions{Deterministic: true}
	channelHeader, err := opts.Marshal(ch)
	if err != nil {
		panic(err)
	}
	signatureHeader, err := opts.Marshal(sh)
	if err != nil {
		panic(err)
	}

	return &common.Header{
		ChannelHeader:   channelHeader,
		SignatureHeader: signatureHeader,
	}
}

func createStructuredPayload(data []byte) *common.Payload {
	payloadChannelHeader := createChannelHeader(common.HeaderType_MESSAGE, 0, "channelID", 0)
	payloadSignatureHeader := &common.SignatureHeader{
		Creator: []byte("creator"),
		Nonce:   []byte("nonce"),
	}
	return &common.Payload{
		Header: createPayloadHeader(payloadChannelHeader, payloadSignatureHeader),
		Data:   data,
	}
}

func deterministicPayloadMarshall(payload *common.Payload) []byte {
	opts := proto.MarshalOptions{Deterministic: true}
	bytes, err := opts.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return bytes
}

func CreateStructuredEnvelope(data []byte) *common.Envelope {
	payload := createStructuredPayload(data)
	payloadBytes := deterministicPayloadMarshall(payload)
	return &common.Envelope{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
}

func CreateStructuredRequest(data []byte) *protos.Request {
	payload := createStructuredPayload(data)
	payloadBytes := deterministicPayloadMarshall(payload)
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
func PrepareTxWithTimestamp(txNumber int, requiredDataSize int, sessionNumber []byte) []byte {
	// create timestamp (8 bytes)
	timeStamp := uint64(time.Now().UnixNano())

	// prepare the payload data
	dataInfoSize := 8 + 8 + len(sessionNumber) // size of info fields stored in the data
	buffer := make([]byte, max(requiredDataSize, dataInfoSize))
	buff := bytes.NewBuffer(buffer[:0])
	binary.Write(buff, binary.BigEndian, uint64(txNumber))
	binary.Write(buff, binary.BigEndian, timeStamp)
	buff.Write(sessionNumber)
	result := buff.Bytes()
	if len(buff.Bytes()) < requiredDataSize {
		padding := make([]byte, requiredDataSize-len(result))
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

// currently, for large enough data, the overhead remains constant at 58 bytes.
var headersOverheadSize = func() int {
	size := 300
	env := CreateStructuredEnvelope(make([]byte, size))
	envBytes, _ := proto.Marshal(env)
	return len(envBytes) - size
}()

// PrepareEnvWithTimestamp prepares an envelope of size envSize, accounting for the overhead introduced by additional headers in the transaction.
func PrepareEnvWithTimestamp(txNumber int, envSize int, sessionNumber []byte) *common.Envelope {
	var dataSize int

	overheadSize := headersOverheadSize
	if envSize < 186 { // 128+58
		overheadSize -= 2
	}

	if envSize < overheadSize {
		dataSize = envSize
	} else {
		dataSize = envSize - overheadSize
	}
	data := PrepareTxWithTimestamp(txNumber, dataSize, sessionNumber)
	return CreateStructuredEnvelope(data)
}
