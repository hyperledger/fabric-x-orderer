/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"crypto/rand"
	"fmt"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hyperledger/fabric-x-common/internaltools/pkg/identity"
)

// MarshalOrPanic serializes a protobuf message and panics if this
// operation fails
func MarshalOrPanic(pb proto.Message) []byte {
	if !pb.ProtoReflect().IsValid() {
		panic(errors.New("proto: Marshal called with nil"))
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		panic(err)
	}
	return data
}

// Marshal serializes a protobuf message.
func Marshal(pb proto.Message) ([]byte, error) {
	if !pb.ProtoReflect().IsValid() {
		return nil, errors.New("proto: Marshal called with nil")
	}
	return proto.Marshal(pb)
}

// CreateNonceOrPanic generates a nonce using the common/crypto package
// and panics if this operation fails.
func CreateNonceOrPanic() []byte {
	nonce, err := CreateNonce()
	if err != nil {
		panic(err)
	}
	return nonce
}

// CreateNonce generates a nonce using the common/crypto package.
func CreateNonce() ([]byte, error) {
	nonce, err := getRandomNonce()
	return nonce, errors.WithMessage(err, "error generating random nonce")
}

// UnmarshalEnvelopeOfType unmarshals an envelope of the specified type,
// including unmarshalling the payload data
func UnmarshalEnvelopeOfType(envelope *cb.Envelope, headerType cb.HeaderType, message proto.Message) (*cb.ChannelHeader, error) {
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, err
	}

	if payload.Header == nil {
		return nil, errors.New("envelope must have a Header")
	}

	chdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	if chdr.Type != int32(headerType) {
		return nil, errors.Errorf("invalid type %s, expected %s", cb.HeaderType(chdr.Type), headerType)
	}

	err = proto.Unmarshal(payload.Data, message)
	err = errors.Wrapf(err, "error unmarshalling message for type %s", headerType)
	return chdr, err
}

// ExtractEnvelopeOrPanic retrieves the requested envelope from a given block
// and unmarshals it -- it panics if either of these operations fail
func ExtractEnvelopeOrPanic(block *cb.Block, index int) *cb.Envelope {
	envelope, err := ExtractEnvelope(block, index)
	if err != nil {
		panic(err)
	}
	return envelope
}

// ExtractEnvelope retrieves the requested envelope from a given block and
// unmarshals it
func ExtractEnvelope(block *cb.Block, index int) (*cb.Envelope, error) {
	if block.Data == nil {
		return nil, errors.New("block data is nil")
	}

	envelopeCount := len(block.Data.Data)
	if index < 0 || index >= envelopeCount {
		return nil, errors.New("envelope index out of bounds")
	}
	marshaledEnvelope := block.Data.Data[index]
	envelope, err := GetEnvelopeFromBlock(marshaledEnvelope)
	err = errors.WithMessagef(err, "block data does not carry an envelope at index %d", index)
	return envelope, err
}

// MakeChannelHeader creates a ChannelHeader.
func MakeChannelHeader(headerType cb.HeaderType, version int32, chainID string, epoch uint64) *cb.ChannelHeader {
	tm := timestamppb.Now()
	tm.Nanos = 0
	return &cb.ChannelHeader{
		Type:      int32(headerType),
		Version:   version,
		Timestamp: tm,
		ChannelId: chainID,
		Epoch:     epoch,
	}
}

// MakeSignatureHeader creates a SignatureHeader.
func MakeSignatureHeader(serializedCreatorCertChain []byte, nonce []byte) *cb.SignatureHeader {
	return &cb.SignatureHeader{
		Creator: serializedCreatorCertChain,
		Nonce:   nonce,
	}
}

// SetTxID generates a transaction id based on the provided signature header
// and sets the TxId field in the channel header
func SetTxID(channelHeader *cb.ChannelHeader, signatureHeader *cb.SignatureHeader) {
	channelHeader.TxId = ComputeTxID(
		signatureHeader.Nonce,
		signatureHeader.Creator,
	)
}

// MakePayloadHeader creates a Payload Header.
func MakePayloadHeader(ch *cb.ChannelHeader, sh *cb.SignatureHeader) *cb.Header {
	return &cb.Header{
		ChannelHeader:   MarshalOrPanic(ch),
		SignatureHeader: MarshalOrPanic(sh),
	}
}

// NewSignatureHeader returns a SignatureHeader with a valid nonce.
func NewSignatureHeader(id identity.Serializer) (*cb.SignatureHeader, error) {
	creator, err := id.Serialize()
	if err != nil {
		return nil, err
	}
	nonce, err := CreateNonce()
	if err != nil {
		return nil, err
	}

	return &cb.SignatureHeader{
		Creator: creator,
		Nonce:   nonce,
	}, nil
}

// NewSignatureHeaderOrPanic returns a signature header and panics on error.
func NewSignatureHeaderOrPanic(id identity.Serializer) *cb.SignatureHeader {
	if id == nil {
		panic(errors.New("invalid signer. cannot be nil"))
	}

	signatureHeader, err := NewSignatureHeader(id)
	if err != nil {
		panic(fmt.Errorf("failed generating a new SignatureHeader: %s", err))
	}

	return signatureHeader
}

// SignOrPanic signs a message and panics on error.
func SignOrPanic(signer identity.Signer, msg []byte) []byte {
	if signer == nil {
		panic(errors.New("invalid signer. cannot be nil"))
	}

	sigma, err := signer.Sign(msg)
	if err != nil {
		panic(fmt.Errorf("failed generating signature: %s", err))
	}
	return sigma
}

// IsConfigBlock validates whenever given block contains configuration
// update transaction
func IsConfigBlock(block *cb.Block) bool {
	if block.Data == nil {
		return false
	}

	return HasConfigTx(block.Data)
}

func HasConfigTx(blockdata *cb.BlockData) bool {
	if blockdata.Data == nil {
		return false
	}

	if len(blockdata.Data) != 1 {
		return false
	}

	marshaledEnvelope := blockdata.Data[0]
	envelope, err := GetEnvelopeFromBlock(marshaledEnvelope)
	if err != nil {
		return false
	}

	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return false
	}

	if payload.Header == nil {
		return false
	}

	hdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return false
	}

	return cb.HeaderType(hdr.Type) == cb.HeaderType_CONFIG
}

// ChannelHeader returns the *cb.ChannelHeader for a given *cb.Envelope.
func ChannelHeader(env *cb.Envelope) (*cb.ChannelHeader, error) {
	if env == nil {
		return nil, errors.New("Invalid envelope payload. can't be nil")
	}

	envPayload, err := UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	if envPayload.Header == nil {
		return nil, errors.New("header not set")
	}

	if envPayload.Header.ChannelHeader == nil {
		return nil, errors.New("channel header not set")
	}

	chdr, err := UnmarshalChannelHeader(envPayload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling channel header")
	}

	return chdr, nil
}

// ChannelID returns the Channel ID for a given *cb.Envelope.
func ChannelID(env *cb.Envelope) (string, error) {
	chdr, err := ChannelHeader(env)
	if err != nil {
		return "", errors.WithMessage(err, "error retrieving channel header")
	}

	return chdr.ChannelId, nil
}

// EnvelopeToConfigUpdate is used to extract a ConfigUpdateEnvelope from an envelope of
// type CONFIG_UPDATE
func EnvelopeToConfigUpdate(configtx *cb.Envelope) (*cb.ConfigUpdateEnvelope, error) {
	configUpdateEnv := &cb.ConfigUpdateEnvelope{}
	_, err := UnmarshalEnvelopeOfType(configtx, cb.HeaderType_CONFIG_UPDATE, configUpdateEnv)
	if err != nil {
		return nil, err
	}
	return configUpdateEnv, nil
}

func getRandomNonce() ([]byte, error) {
	key := make([]byte, 24)

	_, err := rand.Read(key)
	if err != nil {
		return nil, errors.Wrap(err, "error getting random bytes")
	}
	return key, nil
}

func IsConfigTransaction(envelope *cb.Envelope) bool {
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return false
	}

	if payload.Header == nil {
		return false
	}

	hdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return false
	}

	return cb.HeaderType(hdr.Type) == cb.HeaderType_CONFIG || cb.HeaderType(hdr.Type) == cb.HeaderType_ORDERER_TRANSACTION
}
