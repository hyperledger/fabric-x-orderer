/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-common/utils/certificate"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func createChannelHeader(headerType common.HeaderType, version int32, channelId string, epoch uint64) *common.ChannelHeader {
	return &common.ChannelHeader{
		Type:      int32(headerType),
		Version:   version,
		ChannelId: channelId,
		Epoch:     epoch,
	}
}

func createPayloadHeader(ch *common.ChannelHeader, sh *common.SignatureHeader) *common.Header {
	return &common.Header{
		ChannelHeader:   deterministicMarshall(ch),
		SignatureHeader: deterministicMarshall(sh),
	}
}

// fixedNonce is the nonce of the structured test envelopes, which are deterministic and so all share the same TxID.
var fixedNonce = []byte("nonce")

func createStructuredPayload(data []byte, requestType common.HeaderType, nonce []byte) *common.Payload {
	payloadChannelHeader := createChannelHeader(requestType, 0, "channelID", 0)
	id, err := msp.NewSerializedIdentity("org1", []byte("cert"))
	if err != nil {
		panic(err)
	}
	payloadSignatureHeader := &common.SignatureHeader{
		Creator: id,
		Nonce:   nonce,
	}
	protoutil.SetTxID(payloadChannelHeader, payloadSignatureHeader)
	return &common.Payload{
		Header: createPayloadHeader(payloadChannelHeader, payloadSignatureHeader),
		Data:   data,
	}
}

func deterministicMarshall(msg proto.Message) []byte {
	opts := proto.MarshalOptions{Deterministic: true}
	bytes, err := opts.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return bytes
}

func CreateStructuredEnvelope(data []byte) *common.Envelope {
	payload := createStructuredPayload(data, common.HeaderType_MESSAGE, fixedNonce)
	payloadBytes := deterministicMarshall(payload)
	return &common.Envelope{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
}

func CreateECDSAPrivateKey(privateKeyBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyBytes)
	if block == nil || block.Bytes == nil {
		return nil, fmt.Errorf("failed decoding private key PEM")
	}

	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("unexpected pem type, got a %s", strings.ToLower(block.Type))
	}

	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed parsing private key DER: %v", err)
	}

	return priv.(*ecdsa.PrivateKey), nil
}

func CreateStructuredConfigUpdateEnvelope(data []byte) *common.Envelope {
	payload := createStructuredPayload(data, common.HeaderType_CONFIG_UPDATE, fixedNonce)
	payloadBytes := deterministicMarshall(payload)
	return &common.Envelope{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
}

func CreateStructuredConfigEnvelope(data []byte) *common.Envelope {
	payload := createStructuredPayload(data, common.HeaderType_CONFIG, fixedNonce)
	payloadBytes := deterministicMarshall(payload)
	return &common.Envelope{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
}

func CreateConfigBlock(number uint64, data []byte) *common.Block {
	block := protoutil.NewBlock(number, nil)
	payload := createStructuredPayload(data, common.HeaderType_CONFIG, fixedNonce)
	payloadBytes := deterministicMarshall(payload)
	env := &common.Envelope{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
	}
	envBytes := deterministicMarshall(env)
	block.Data = &common.BlockData{Data: [][]byte{envBytes}}
	md, err := ledger.AssemblerBlockMetadataToBytes(&types.SimpleBatch{}, &state.OrderingInformation{DecisionNum: types.DecisionNum(number)}, 0)
	if err != nil {
		panic("could not create block metadata")
	}
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = md
	return block
}

func CreateStructuredConfigUpdateRequest(data []byte) *protos.Request {
	payload := createStructuredPayload(data, common.HeaderType_CONFIG_UPDATE, fixedNonce)
	payloadBytes := deterministicMarshall(payload)
	return &protos.Request{
		Payload:   payloadBytes,
		Signature: []byte("signature"),
		ConfigSeq: 1,
	}
}

func CreateStructuredRequest(data []byte) *protos.Request {
	payload := createStructuredPayload(data, common.HeaderType_MESSAGE, fixedNonce)
	payloadBytes := deterministicMarshall(payload)
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

// PrepareUnsignedEnvelope prepares an envelope of size envSize, accounting for the overhead introduced by additional headers in the transaction.
// When no data size yields exactly envSize (a varint length grows by a byte), the envelope is one byte smaller.
// The data is at least 8+8+len(sessionNumber) bytes (see PrepareTxWithTimestamp), so the envelope is larger than
// requested when envSize is below that plus the headers.
// Each envelope gets a random nonce, and so a unique TxID.
// this method will be used for creating envelope for unsigned signing mode.
func PrepareUnsignedEnvelope(txNumber int, envSize int, sessionNumber []byte) *common.Envelope {
	payload := createStructuredPayload(nil, common.HeaderType_MESSAGE, protoutil.CreateNonceOrPanic())
	signature := []byte("signature")
	dataSize := fitDataSize(envSize, proto.Size(payload), signatureFieldSize(len(signature)))
	payload.Data = PrepareTxWithTimestamp(txNumber, dataSize, sessionNumber)
	return &common.Envelope{
		Payload:   deterministicMarshall(payload),
		Signature: signature,
	}
}

// PrepareSignedEnvelopeWithCertificate prepares an fully signed envelope of size envSize, or at most a few bytes
// smaller, as the size of an ECDSA signature varies. As with PrepareUnsignedEnvelope, the data has a minimum size.
// this method will be used for creating signed envelope for full signing mode.
func PrepareSignedEnvelopeWithCertificate(txNumber int, envSize int, sessionNumber []byte, signer *crypto.ECDSASigner, certBytes []byte, org string) *common.Envelope {
	payload := createSignedStructuredPayload(nil, certBytes, org)
	return prepareSignedEnvelope(payload, txNumber, envSize, sessionNumber, signer)
}

// PrepareSignedEnvelopeWithCertificateID is used for creating signed envelopes for the "short" (known-identity) signing mode.
// The envelope's size is as with PrepareSignedEnvelopeWithCertificate.
func PrepareSignedEnvelopeWithCertificateID(txNumber int, envSize int, sessionNumber []byte, signer *crypto.ECDSASigner, certBytes []byte, org string) *common.Envelope {
	payload := createSignedStructuredPayloadWithCertID(nil, certBytes, org)
	return prepareSignedEnvelope(payload, txNumber, envSize, sessionNumber, signer)
}

// prepareSignedEnvelope fills the payload with data, so that once signed the envelope is at most envSize bytes,
// assuming a signature of the largest size the signer can make.
func prepareSignedEnvelope(payload *common.Payload, txNumber int, envSize int, sessionNumber []byte, signer *crypto.ECDSASigner) *common.Envelope {
	dataSize := fitDataSize(envSize, proto.Size(payload), signatureFieldSize(maxECDSASignatureSize(signer)))
	payload.Data = PrepareTxWithTimestamp(txNumber, dataSize, sessionNumber)
	return signPayload(payload, signer)
}

// fitDataSize returns the largest data size for which envelopeSize is at most envSize, or 0 if there is none.
func fitDataSize(envSize int, emptyPayloadSize int, sigFieldSize int) int {
	// The overhead, envelopeSize(d)-d, never decreases as d grows, and the fitting data size is at most envSize.
	// So starting at d0 = envSize-overhead(envSize) never overshoots, as envelopeSize(d0) = d0+overhead(d0) <=
	// d0+overhead(envSize) = envSize, and the loop only steps up from there, a few bytes at most.
	overhead := envelopeSize(emptyPayloadSize, sigFieldSize, envSize) - envSize
	dataSize := max(0, envSize-overhead)
	for envelopeSize(emptyPayloadSize, sigFieldSize, dataSize+1) <= envSize {
		dataSize++
	}
	return dataSize
}

// envelopeSize returns the marshaled size of an envelope whose payload is emptyPayloadSize bytes without data, plus
// dataSize bytes of data, and whose signature field is sigFieldSize bytes.
func envelopeSize(emptyPayloadSize int, sigFieldSize int, dataSize int) int {
	payloadSize := emptyPayloadSize
	if dataSize > 0 {
		payloadSize += protowire.SizeTag(2) + protowire.SizeBytes(dataSize)
	}
	return protowire.SizeTag(1) + protowire.SizeBytes(payloadSize) + sigFieldSize
}

// signatureFieldSize returns the marshaled size of an envelope's signature field, for a signature of sigSize bytes.
func signatureFieldSize(sigSize int) int {
	return protowire.SizeTag(2) + protowire.SizeBytes(sigSize)
}

// maxECDSASignatureSize returns the largest DER encoding of a signature by the signer: a sequence of two integers,
// each at most one byte longer than the curve's order.
func maxECDSASignatureSize(signer *crypto.ECDSASigner) int {
	intSize := 2 + (signer.Curve.Params().BitSize+7)/8 + 1
	contentSize := 2 * intSize
	if contentSize > 127 {
		return 3 + contentSize
	}
	return 2 + contentSize
}

// CreateSignedStructuredEnvelopeWithCertID creates an envelope whose SignatureHeader.Creator carries a
// certificate-ID (hex SHA256 of the DER-encoded cert) rather than the full PEM certificate.
// The verifying MSP must have the corresponding certificate registered in its known-identities list.
func CreateSignedStructuredEnvelopeWithCertID(data []byte, signer *crypto.ECDSASigner, certBytes []byte, org string) *common.Envelope {
	return signPayload(createSignedStructuredPayloadWithCertID(data, certBytes, org), signer)
}

func computeCertID(certBytes []byte) (string, error) {
	digest, err := certificate.DigestPemContent(certBytes, bccsp.SHA256)
	if err != nil {
		return "", fmt.Errorf("failed computing cert ID: %w", err)
	}
	return hex.EncodeToString(digest), nil
}

// createSignedStructuredPayloadWithCertID builds a Payload whose creator identity uses a certificate-ID
// (hash) instead of the full certificate PEM.
func createSignedStructuredPayloadWithCertID(data []byte, certBytes []byte, org string) *common.Payload {
	payloadChannelHeader := createChannelHeader(common.HeaderType_MESSAGE, 0, "channelID", 0)

	certID, err := computeCertID(certBytes)
	if err != nil {
		panic(err)
	}

	sId := msppb.NewIdentityWithIDOfCert(org, certID)

	payloadSignatureHeader := &common.SignatureHeader{
		Creator: deterministicMarshall(sId),
		Nonce:   protoutil.CreateNonceOrPanic(),
	}
	protoutil.SetTxID(payloadChannelHeader, payloadSignatureHeader)
	return &common.Payload{
		Header: createPayloadHeader(payloadChannelHeader, payloadSignatureHeader),
		Data:   data,
	}
}

func CreateSignedStructuredEnvelope(data []byte, signer *crypto.ECDSASigner, certBytes []byte, org string) *common.Envelope {
	return signPayload(createSignedStructuredPayload(data, certBytes, org), signer)
}

// signPayload marshals the payload and signs it, returning nil if signing fails.
func signPayload(payload *common.Payload, signer *crypto.ECDSASigner) *common.Envelope {
	payloadBytes := deterministicMarshall(payload)

	// Sign the payload
	signature, err := signer.Sign(payloadBytes)
	if err != nil {
		return nil
	}
	return &common.Envelope{
		Payload:   payloadBytes,
		Signature: signature,
	}
}

// TODO: Revise it when cert registry is integrated into orderer
func createSignedStructuredPayload(data []byte, certBytes []byte, org string) *common.Payload {
	payloadChannelHeader := createChannelHeader(common.HeaderType_MESSAGE, 0, "channelID", 0)

	sId := msppb.NewIdentity(org, certBytes)

	payloadSignatureHeader := &common.SignatureHeader{
		Creator: deterministicMarshall(sId),
		Nonce:   protoutil.CreateNonceOrPanic(),
	}
	protoutil.SetTxID(payloadChannelHeader, payloadSignatureHeader)
	return &common.Payload{
		Header: createPayloadHeader(payloadChannelHeader, payloadSignatureHeader),
		Data:   data,
	}
}

func CreatePayloadWithConfigUpdate(data []byte, certBytes []byte, org string) *common.Payload {
	payloadChannelHeader := createChannelHeader(common.HeaderType_CONFIG_UPDATE, 0, "arma", 0)

	sId := msppb.NewIdentity(org, certBytes)

	payloadSignatureHeader := &common.SignatureHeader{
		Creator: deterministicMarshall(sId),
		Nonce:   protoutil.CreateNonceOrPanic(),
	}

	protoutil.SetTxID(payloadChannelHeader, payloadSignatureHeader)

	return &common.Payload{
		Header: createPayloadHeader(payloadChannelHeader, payloadSignatureHeader),
		Data:   data,
	}
}

func CreateSignedEnvelope(payload *common.Payload, signer identity.SignerSerializer) (*common.Envelope, error) {
	payloadBytes := deterministicMarshall(payload)
	sig, err := signer.Sign(payloadBytes)
	if err != nil {
		return nil, err
	}

	env := &common.Envelope{
		Payload:   payloadBytes,
		Signature: sig,
	}

	return env, nil
}
