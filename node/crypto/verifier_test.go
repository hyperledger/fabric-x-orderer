/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/stretchr/testify/require"
)

// generateTestECDSAKey generates a test ECDSA key pair and returns the PEM-encoded public key
func generateTestECDSAKey(t *testing.T) ([]byte, *ecdsa.PublicKey) {
	pubKeyPEM, _, pubKey := generateTestECDSAKeyPair(t)
	return pubKeyPEM, pubKey
}

// generateTestECDSAKeyPair generates a test ECDSA key pair and returns the PEM-encoded public
// key along with the private and public keys, so tests can produce signatures to verify.
func generateTestECDSAKeyPair(t *testing.T) ([]byte, *ecdsa.PrivateKey, *ecdsa.PublicKey) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)

	pubKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyBytes,
	})

	return pubKeyPEM, privateKey, &privateKey.PublicKey
}

func TestParsePublicKeyToPEM(t *testing.T) {
	logger := flogging.MustGetLogger("test")
	shardID := types.ShardID(1)
	partyID := types.PartyID(2)

	tests := []struct {
		name        string
		setupKey    func(t *testing.T) []byte
		expectPanic bool
		validateKey func(t *testing.T, key *ecdsa.PublicKey)
	}{
		{
			name: "Success",
			setupKey: func(t *testing.T) []byte {
				pubKeyPEM, _ := generateTestECDSAKey(t)
				return pubKeyPEM
			},
			expectPanic: false,
			validateKey: func(t *testing.T, key *ecdsa.PublicKey) {
				require.NotNil(t, key)
				require.NotNil(t, key.X)
				require.NotNil(t, key.Y)
				require.NotNil(t, key.Curve)
			},
		},
		{
			name: "NilKey",
			setupKey: func(t *testing.T) []byte {
				return nil
			},
			expectPanic: true,
		},
		{
			name: "InvalidPEM",
			setupKey: func(t *testing.T) []byte {
				return []byte("not a valid PEM")
			},
			expectPanic: true,
		},
		{
			name: "EmptyPEMBytes",
			setupKey: func(t *testing.T) []byte {
				return pem.EncodeToMemory(&pem.Block{
					Type:  "PUBLIC KEY",
					Bytes: nil,
				})
			},
			expectPanic: true,
		},
		{
			name: "InvalidPublicKeyFormat",
			setupKey: func(t *testing.T) []byte {
				return pem.EncodeToMemory(&pem.Block{
					Type:  "PUBLIC KEY",
					Bytes: []byte("invalid key bytes"),
				})
			},
			expectPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pubKeyPEM := tt.setupKey(t)

			if tt.expectPanic {
				require.Panics(t, func() {
					crypto.ParsePublicKeyFromPEM(pubKeyPEM, types.RoleBatcher, shardID, partyID, logger)
				}, "Expected panic for test case: %s", tt.name)
			} else {
				parsedKey := crypto.ParsePublicKeyFromPEM(pubKeyPEM, types.RoleBatcher, shardID, partyID, logger)
				if tt.validateKey != nil {
					tt.validateKey(t, parsedKey)
				}
			}
		})
	}
}

func TestAddPublicKeyToVerifier(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	tests := []struct {
		name        string
		setup       func(t *testing.T) (crypto.ECDSAVerifier, []byte, types.ShardID, types.PartyID)
		expectPanic bool
		validate    func(t *testing.T, verifier crypto.ECDSAVerifier, shardID types.ShardID, partyID types.PartyID)
	}{
		{
			name: "Success",
			setup: func(t *testing.T) (crypto.ECDSAVerifier, []byte, types.ShardID, types.PartyID) {
				verifier := make(crypto.ECDSAVerifier)
				pubKeyPEM, _ := generateTestECDSAKey(t)
				return verifier, pubKeyPEM, types.ShardID(1), types.PartyID(2)
			},
			expectPanic: false,
			validate: func(t *testing.T, verifier crypto.ECDSAVerifier, shardID types.ShardID, partyID types.PartyID) {
				key := types.NodeIdentity{Role: types.RoleBatcher, ShardID: shardID, PartyID: partyID}
				_, exists := verifier[key]
				require.True(t, exists, "Key should exist in verifier")
				require.Equal(t, 1, len(verifier))
			},
		},
		{
			name: "MultipleKeys",
			setup: func(t *testing.T) (crypto.ECDSAVerifier, []byte, types.ShardID, types.PartyID) {
				verifier := make(crypto.ECDSAVerifier)
				// Add first two keys
				for i := 0; i < 2; i++ {
					pubKeyPEM, _ := generateTestECDSAKey(t)
					verifier.AddPublicKeyToVerifier(pubKeyPEM, types.RoleBatcher, types.ShardID(i), types.PartyID(i+1), logger)
				}
				// Return third key to add
				pubKeyPEM, _ := generateTestECDSAKey(t)
				return verifier, pubKeyPEM, types.ShardID(2), types.PartyID(3)
			},
			expectPanic: false,
			validate: func(t *testing.T, verifier crypto.ECDSAVerifier, shardID types.ShardID, partyID types.PartyID) {
				require.Equal(t, 3, len(verifier), "Verifier should contain 3 keys")
				for i := 0; i < 3; i++ {
					key := types.NodeIdentity{Role: types.RoleBatcher, ShardID: types.ShardID(i), PartyID: types.PartyID(i + 1)}
					_, exists := verifier[key]
					require.True(t, exists, "Key %d should exist in verifier", i)
				}
			},
		},
		{
			name: "DifferentRoles",
			setup: func(t *testing.T) (crypto.ECDSAVerifier, []byte, types.ShardID, types.PartyID) {
				verifier := make(crypto.ECDSAVerifier)
				roles := []types.NodeRole{types.RoleBatcher, types.RoleConsenter}
				for i, role := range roles {
					pubKeyPEM, _ := generateTestECDSAKey(t)
					verifier.AddPublicKeyToVerifier(pubKeyPEM, role, types.ShardID(i), types.PartyID(i), logger)
				}
				// Return third key with different entity type
				pubKeyPEM, _ := generateTestECDSAKey(t)
				return verifier, pubKeyPEM, types.ShardID(2), types.PartyID(2)
			},
			expectPanic: false,
			validate: func(t *testing.T, verifier crypto.ECDSAVerifier, shardID types.ShardID, partyID types.PartyID) {
				require.Equal(t, 3, len(verifier))
			},
		},
		{
			name: "InvalidKey",
			setup: func(t *testing.T) (crypto.ECDSAVerifier, []byte, types.ShardID, types.PartyID) {
				verifier := make(crypto.ECDSAVerifier)
				invalidPEM := []byte("not a valid PEM")
				return verifier, invalidPEM, types.ShardID(1), types.PartyID(2)
			},
			expectPanic: true,
			validate: func(t *testing.T, verifier crypto.ECDSAVerifier, shardID types.ShardID, partyID types.PartyID) {
				require.Equal(t, 0, len(verifier), "Verifier should be empty after failed add")
			},
		},
		{
			name: "OverwriteExistingKey",
			setup: func(t *testing.T) (crypto.ECDSAVerifier, []byte, types.ShardID, types.PartyID) {
				verifier := make(crypto.ECDSAVerifier)
				shardID := types.ShardID(1)
				partyID := types.PartyID(2)
				// Add first key
				pubKeyPEM1, _ := generateTestECDSAKey(t)
				verifier.AddPublicKeyToVerifier(pubKeyPEM1, types.RoleBatcher, shardID, partyID, logger)
				// Return second key with same shard and party (should overwrite)
				pubKeyPEM2, _ := generateTestECDSAKey(t)
				return verifier, pubKeyPEM2, shardID, partyID
			},
			expectPanic: false,
			validate: func(t *testing.T, verifier crypto.ECDSAVerifier, shardID types.ShardID, partyID types.PartyID) {
				require.Equal(t, 1, len(verifier), "Verifier should still contain only 1 key")
				key := types.NodeIdentity{Role: types.RoleBatcher, ShardID: shardID, PartyID: partyID}
				_, exists := verifier[key]
				require.True(t, exists, "Key should exist in verifier")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifier, pubKeyPEM, shardID, partyID := tt.setup(t)

			if tt.expectPanic {
				require.Panics(t, func() {
					verifier.AddPublicKeyToVerifier(pubKeyPEM, types.RoleBatcher, shardID, partyID, logger)
				}, "Expected panic for test case: %s", tt.name)
			} else {
				verifier.AddPublicKeyToVerifier(pubKeyPEM, types.RoleBatcher, shardID, partyID, logger)
			}

			if tt.validate != nil {
				tt.validate(t, verifier, shardID, partyID)
			}
		})
	}
}

// TestVerifySignatureRole verifies that the node role is part of the verifier key: a key
// registered under one role verifies signatures for that role, and a lookup under a
// different role (same shard and party) does not find it.
func TestVerifySignatureRole(t *testing.T) {
	logger := flogging.MustGetLogger("test")
	partyID := types.PartyID(3)
	// Assemblers are not part of a shard, so like consenters they are keyed under ShardIDConsensus
	// and disambiguated by their role.
	shardID := types.ShardIDConsensus

	pubKeyPEM, privateKey, _ := generateTestECDSAKeyPair(t)

	verifier := make(crypto.ECDSAVerifier)
	verifier.AddPublicKeyToVerifier(pubKeyPEM, types.RoleAssembler, shardID, partyID, logger)

	msg := []byte("a message signed by the assembler")
	signer := crypto.ECDSASigner(*privateKey)
	sig, err := signer.Sign(msg)
	require.NoError(t, err)

	// The assembler key verifies the assembler's signature.
	require.NoError(t, verifier.VerifySignature(types.RoleAssembler, partyID, shardID, msg, sig))

	// A lookup under a different role at the same shard and party does not find the key.
	require.Error(t, verifier.VerifySignature(types.RoleConsenter, partyID, shardID, msg, sig))

	// A zero-valued (unset) role is the empty string, so it fails loudly rather than silently
	// resolving against a real node's key.
	require.Zero(t, types.NodeRole(""), "the zero value of NodeRole must be the empty string")
	require.Error(t, verifier.VerifySignature(types.NodeRole(""), partyID, shardID, msg, sig))

	// A different role's key does not collide with the assembler's key.
	consenterPEM, _, _ := generateTestECDSAKeyPair(t)
	verifier.AddPublicKeyToVerifier(consenterPEM, types.RoleConsenter, shardID, partyID, logger)
	require.Len(t, verifier, 2)
	require.NoError(t, verifier.VerifySignature(types.RoleAssembler, partyID, shardID, msg, sig))
}
