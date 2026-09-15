/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil/tlsgen"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// signingIdentity returns a signer and the PEM certificate it corresponds to.
func signingIdentity(t *testing.T) (*crypto.ECDSASigner, []byte) {
	t.Helper()

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	pair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	block, _ := pem.Decode(pair.Key)
	require.NotNil(t, block)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: block.Bytes})
	privateKey, err := CreateECDSAPrivateKey(keyPEM)
	require.NoError(t, err)

	return (*crypto.ECDSASigner)(privateKey), pair.Cert
}

// publicKeyOf returns the public key the given PEM certificate carries.
func publicKeyOf(t *testing.T, certPEM []byte) *ecdsa.PublicKey {
	t.Helper()

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	publicKey, isECDSA := cert.PublicKey.(*ecdsa.PublicKey)
	require.True(t, isECDSA)

	return publicKey
}

// headersOf returns the two marshalled headers of an envelope's payload.
func headersOf(t *testing.T, envelope *common.Envelope) (channelHeader []byte, signatureHeader []byte) {
	t.Helper()

	payload := &common.Payload{}
	require.NoError(t, proto.Unmarshal(envelope.Payload, payload))
	require.NotNil(t, payload.Header)

	return payload.Header.ChannelHeader, payload.Header.SignatureHeader
}

func TestShortModeEnvelopeBuilderMatchesPerTransactionPath(t *testing.T) {
	// Scenario:
	// 1. Create a signing identity.
	// 2. Build an envelope for one transaction with a builder.
	// 3. Build an envelope for the same transaction with the per-transaction function.
	// 4. Compare the two marshalled headers of both envelopes.
	// 5. Compare the size of the transaction both envelopes carry.
	signer, certPEM := signingIdentity(t)
	sessionNumber := []byte("0123456789abcdef")

	builder, err := NewShortModeEnvelopeBuilder(signer, certPEM, "org1")
	require.NoError(t, err)

	built, err := builder.Envelope(7, 125, sessionNumber)
	require.NoError(t, err)

	perTransaction := PrepareSignedEnvelopeWithCertificateID(7, 125, sessionNumber, signer, certPEM, "org1")
	require.NotNil(t, perTransaction)

	builtChannelHeader, builtSignatureHeader := headersOf(t, built)
	expectedChannelHeader, expectedSignatureHeader := headersOf(t, perTransaction)
	require.Equal(t, expectedChannelHeader, builtChannelHeader)
	require.Equal(t, expectedSignatureHeader, builtSignatureHeader)

	builtPayload := &common.Payload{}
	require.NoError(t, proto.Unmarshal(built.Payload, builtPayload))
	expectedPayload := &common.Payload{}
	require.NoError(t, proto.Unmarshal(perTransaction.Payload, expectedPayload))
	require.Len(t, builtPayload.Data, len(expectedPayload.Data))
}

func TestShortModeEnvelopeBuilderNamesTheSignersCertificate(t *testing.T) {
	// Scenario:
	// 1. Create a signing identity.
	// 2. Build an envelope with a builder.
	// 3. Extract the creator identity from the envelope's signature header.
	// 4. Compare the certificate identifier it names against the hash of the certificate.
	// 5. Confirm the identity carries no certificate.
	signer, certPEM := signingIdentity(t)

	builder, err := NewShortModeEnvelopeBuilder(signer, certPEM, "org1")
	require.NoError(t, err)

	envelope, err := builder.Envelope(1, 125, []byte("0123456789abcdef"))
	require.NoError(t, err)

	_, signatureHeaderBytes := headersOf(t, envelope)
	signatureHeader := &common.SignatureHeader{}
	require.NoError(t, proto.Unmarshal(signatureHeaderBytes, signatureHeader))

	identity := &msppb.Identity{}
	require.NoError(t, proto.Unmarshal(signatureHeader.Creator, identity))

	expectedCertID, err := computeCertID(certPEM)
	require.NoError(t, err)
	require.Equal(t, "org1", identity.MspId)
	require.Equal(t, expectedCertID, identity.GetCertificateId())
	require.Empty(t, identity.GetCertificate())
}

func TestShortModeEnvelopeBuilderSignsEveryEnvelope(t *testing.T) {
	// Scenario:
	// 1. Create a signing identity.
	// 2. Build three envelopes for three transactions with one builder.
	// 3. Verify each signature against the public key of the signer's certificate.
	// 4. Confirm all three envelopes share the same two marshalled headers.
	// 5. Confirm the three payloads differ from one another.
	signer, certPEM := signingIdentity(t)
	publicKey := publicKeyOf(t, certPEM)
	sessionNumber := []byte("0123456789abcdef")

	builder, err := NewShortModeEnvelopeBuilder(signer, certPEM, "org1")
	require.NoError(t, err)

	envelopes := make([]*common.Envelope, 3)
	for i := range envelopes {
		envelopes[i], err = builder.Envelope(i, 125, sessionNumber)
		require.NoError(t, err)

		digest := sha256.Sum256(envelopes[i].Payload)
		require.True(t, ecdsa.VerifyASN1(publicKey, digest[:], envelopes[i].Signature))
	}

	firstChannelHeader, firstSignatureHeader := headersOf(t, envelopes[0])
	for _, envelope := range envelopes[1:] {
		channelHeader, signatureHeader := headersOf(t, envelope)
		require.Equal(t, firstChannelHeader, channelHeader)
		require.Equal(t, firstSignatureHeader, signatureHeader)
	}

	require.NotEqual(t, envelopes[0].Payload, envelopes[1].Payload)
	require.NotEqual(t, envelopes[1].Payload, envelopes[2].Payload)
}

func TestShortModeEnvelopeBuilderSizesTheWholeEnvelope(t *testing.T) {
	// Scenario:
	// 1. Create a signing identity and a builder.
	// 2. Build an envelope for each of several requested sizes above the envelope's own overhead.
	// 3. Confirm no envelope exceeds the size requested.
	// 4. Confirm every envelope is within four bytes of the size requested.
	// 5. Compare each size against the envelope the unsigned mode produces for the same request.
	signer, certPEM := signingIdentity(t)
	sessionNumber := []byte("0123456789abcdef")

	builder, err := NewShortModeEnvelopeBuilder(signer, certPEM, "org1")
	require.NoError(t, err)

	// A DER encoded signature loses a byte whenever a leading zero drops out of r or s, so a run of
	// envelopes covers the sizes one identity produces rather than a single one.
	for _, envSize := range []int{300, 500, 1000, 4096} {
		for txNumber := 0; txNumber < 64; txNumber++ {
			envelope, err := builder.Envelope(txNumber, envSize, sessionNumber)
			require.NoError(t, err)

			size := proto.Size(envelope)
			require.LessOrEqual(t, size, envSize)
			require.Greater(t, size, envSize-4)
		}

		unsigned := PrepareUnsignedEnvelope(0, envSize, sessionNumber)
		require.Equal(t, envSize, proto.Size(unsigned))
	}
}

func TestShortModeEnvelopeBuilderSizesTheTransactionWhenTheEnvelopeCannotFit(t *testing.T) {
	// Scenario:
	// 1. Create a signing identity and a builder.
	// 2. Request an envelope smaller than the overhead an envelope carries.
	// 3. Confirm the transaction in it is the size requested instead.
	signer, certPEM := signingIdentity(t)

	builder, err := NewShortModeEnvelopeBuilder(signer, certPEM, "org1")
	require.NoError(t, err)
	require.Greater(t, builder.overheadSize, 0)

	envSize := builder.overheadSize - 1
	envelope, err := builder.Envelope(1, envSize, []byte("0123456789abcdef"))
	require.NoError(t, err)

	payload := &common.Payload{}
	require.NoError(t, proto.Unmarshal(envelope.Payload, payload))
	require.Len(t, payload.Data, envSize)
}

func TestShortModeEnvelopeBuilderRejectsACertificateItCannotParse(t *testing.T) {
	// Scenario:
	// 1. Create a signing identity.
	// 2. Construct a builder with content that is not a PEM certificate.
	// 3. Confirm the construction fails and returns no builder.
	signer, _ := signingIdentity(t)

	builder, err := NewShortModeEnvelopeBuilder(signer, []byte("not a certificate"), "org1")
	require.Error(t, err)
	require.Nil(t, builder)
}
