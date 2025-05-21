/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
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
