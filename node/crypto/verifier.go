/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

// ECDSAVerifier maps a node's identity to its ECDSA public key. The role is part of the key so that
// nodes which are not part of a shard (routers, consenters, and assemblers) can be told apart even
// though they all carry shard 0.
type ECDSAVerifier map[types.NodeIdentity]ecdsa.PublicKey

func (v ECDSAVerifier) VerifySignature(id types.NodeIdentity, msg, sig []byte) error {
	pk, exists := v[id]
	if !exists {
		return fmt.Errorf("key does not exist: %s", id.String())
	}

	digest := sha256.Sum256(msg)

	if ecdsa.VerifyASN1(&pk, digest[:], sig) {
		return nil
	}

	return fmt.Errorf("signature %s of %s", base64.StdEncoding.EncodeToString(sig), id.String())
}

// AddPublicKeyToVerifier adds a public key to the verifier map after parsing it from PEM format.
func (v ECDSAVerifier) AddPublicKeyToVerifier(publicKeyPEM []byte, id types.NodeIdentity, logger *flogging.FabricLogger) {
	ecdsaPK := ParsePublicKeyFromPEM(publicKeyPEM, id, logger)
	v[id] = *ecdsaPK
}

// ParsePublicKeyFromPEM decodes and parses a PEM-encoded public key into an ECDSA public key.
// It panics with a descriptive error message if the key is invalid.
func ParsePublicKeyFromPEM(publicKeyPEM []byte, id types.NodeIdentity, logger *flogging.FabricLogger) *ecdsa.PublicKey {
	if publicKeyPEM == nil {
		logger.Panicf("Nil public key of %s", id)
	}

	pkDecoded, _ := pem.Decode(publicKeyPEM)
	if pkDecoded == nil || pkDecoded.Bytes == nil {
		logger.Panicf("Failed decoding public key of %s from PEM", id)
	}

	pkParsed, err := x509.ParsePKIXPublicKey(pkDecoded.Bytes)
	if err != nil {
		logger.Panicf("Failed parsing public key of %s: %v", id, err)
	}

	ecdsaPK, ok := pkParsed.(*ecdsa.PublicKey)
	if !ok {
		logger.Panicf("Unsupported public key type %T for %s", pkParsed, id)
	}

	return ecdsaPK
}
