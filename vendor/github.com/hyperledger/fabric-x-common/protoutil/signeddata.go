/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-x-common/api/applicationpb"
)

// SignedData is used to represent the general triplet required to verify a signature
// This is intended to be generic across crypto schemes, while most crypto schemes will
// include the signing identity and a nonce within the Data, this is left to the crypto
// implementation.
type SignedData struct {
	Data      []byte
	Identity  *applicationpb.Identity
	Signature []byte
}

// ConfigUpdateEnvelopeAsSignedData returns the set of signatures for the
// ConfigUpdateEnvelope as SignedData or an error indicating why this was not
// possible.
func ConfigUpdateEnvelopeAsSignedData(ce *common.ConfigUpdateEnvelope) ([]*SignedData, error) {
	if ce == nil {
		return nil, fmt.Errorf("No signatures for nil SignedConfigItem")
	}

	result := make([]*SignedData, len(ce.Signatures))
	for i, configSig := range ce.Signatures {
		sigHeader := &common.SignatureHeader{}
		err := proto.Unmarshal(configSig.SignatureHeader, sigHeader)
		if err != nil {
			return nil, err
		}

		id, err := UnmarshalIdentity(sigHeader.GetCreator())
		if err != nil {
			return nil, err
		}
		result[i] = &SignedData{
			Data:      bytes.Join([][]byte{configSig.SignatureHeader, ce.ConfigUpdate}, nil),
			Identity:  id,
			Signature: configSig.Signature,
		}

	}

	return result, nil
}

// EnvelopeAsSignedData returns the signatures for the Envelope as SignedData
// slice of length 1 or an error indicating why this was not possible.
func EnvelopeAsSignedData(env *common.Envelope) ([]*SignedData, error) {
	if env == nil {
		return nil, fmt.Errorf("No signatures for nil Envelope")
	}

	payload := &common.Payload{}
	err := proto.Unmarshal(env.Payload, payload)
	if err != nil {
		return nil, err
	}

	if payload.Header == nil /* || payload.Header.SignatureHeader == nil */ {
		return nil, fmt.Errorf("Missing Header")
	}

	shdr := &common.SignatureHeader{}
	err = proto.Unmarshal(payload.Header.SignatureHeader, shdr)
	if err != nil {
		return nil, fmt.Errorf("GetSignatureHeaderFromBytes failed, err %s", err)
	}

	id, err := UnmarshalIdentity(shdr.GetCreator())
	if err != nil {
		return nil, err
	}
	return []*SignedData{{
		Data:      env.Payload,
		Identity:  id,
		Signature: env.Signature,
	}}, nil
}

// LogMessageForIdentity returns a string with serialized identity information,
// or a string indicating why the serialized identity information cannot be returned.
// Any errors are intentionally returned in the return strings so that the function can be used in single-line log messages with minimal clutter.
func LogMessageForIdentity(id *applicationpb.Identity) string {
	switch id.GetCreator().(type) {
	case *applicationpb.Identity_Certificate:
		pemBlock, _ := pem.Decode(id.GetCertificate())
		if pemBlock == nil {
			// not all identities are certificates so simply log the serialized
			// identity bytes
			return fmt.Sprintf("serialized-identity=%x", id)
		}
		cert, err := x509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			return fmt.Sprintf("Could not parse certificate: %s", err)
		}
		return fmt.Sprintf("(mspid=%s subject=%s issuer=%s serialnumber=%d)",
			id.MspId, cert.Subject, cert.Issuer, cert.SerialNumber)
	case *applicationpb.Identity_CertificateId:
		return fmt.Sprintf("(mspid=%s certificateID=%s)", id.MspId, id.GetCertificateId())
	default:
		return "unknown creator type"
	}
}

// LogMessageForIdentities returns a string with identity information.
func LogMessageForIdentities(signedData []*SignedData) (logMsg string) {
	var identityMessages []string
	for _, sd := range signedData {
		identityMessages = append(identityMessages, LogMessageForIdentity(sd.Identity))
	}
	return strings.Join(identityMessages, ", ")
}
