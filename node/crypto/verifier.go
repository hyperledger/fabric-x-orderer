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

type ShardPartyKey struct {
	Shard types.ShardID
	Party types.PartyID
}

func (k *ShardPartyKey) ToString() string {
	return fmt.Sprintf("Shard: %d, Party: %d", k.Shard, k.Party)
}

type ECDSAVerifier map[ShardPartyKey]ecdsa.PublicKey

func (v ECDSAVerifier) VerifySignature(partyID types.PartyID, shardID types.ShardID, msg, sig []byte) error {
	key := ShardPartyKey{Shard: shardID, Party: partyID}
	pk, exists := v[key]
	if !exists {
		return fmt.Errorf("key does not exist: %s", key.ToString())
	}

	digest := sha256.Sum256(msg)

	if ecdsa.VerifyASN1(&pk, digest[:], sig) {
		return nil
	}

	return fmt.Errorf("signature %s of %s", base64.StdEncoding.EncodeToString(sig), key.ToString())
}

// AddPublicKeyToVerifier adds a public key to the verifier map after parsing it from PEM format.
func (v ECDSAVerifier) AddPublicKeyToVerifier(publicKeyPEM []byte, entityType string, shardID types.ShardID, partyID types.PartyID, logger *flogging.FabricLogger) {
	ecdsaPK := ParsePublicKeyFromPEM(publicKeyPEM, entityType, shardID, partyID, logger)
	v[ShardPartyKey{Shard: shardID, Party: partyID}] = *ecdsaPK
}

// ParsePublicKeyFromPEM decodes and parses a PEM-encoded public key into an ECDSA public key.
// It panics with a descriptive error message if the key is invalid.
func ParsePublicKeyFromPEM(publicKeyPEM []byte, entityType string, shardID types.ShardID, partyID types.PartyID, logger *flogging.FabricLogger) *ecdsa.PublicKey {
	if publicKeyPEM == nil {
		logger.Panicf("Nil %s public key (shard %d, party %d)", entityType, shardID, partyID)
	}

	pkDecoded, _ := pem.Decode(publicKeyPEM)
	if pkDecoded == nil || pkDecoded.Bytes == nil {
		logger.Panicf("Failed decoding %s public key of party %d (shard %d) from PEM", entityType, partyID, shardID)
	}

	pkParsed, err := x509.ParsePKIXPublicKey(pkDecoded.Bytes)
	if err != nil {
		logger.Panicf("Failed parsing %s public key (shard %d, party %d): %v", entityType, shardID, partyID, err)
	}

	ecdsaPK, ok := pkParsed.(*ecdsa.PublicKey)
	if !ok {
		logger.Panicf("Unsupported public key type %T for %s (shard %d, party %d)", pkParsed, entityType, shardID, partyID)
	}

	return ecdsaPK
}
