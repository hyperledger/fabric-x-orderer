package node

import (
	arma "arma/pkg"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

type ECDSAVerifier map[struct {
	party arma.PartyID
	shard arma.ShardID
}]ecdsa.PublicKey

func (v ECDSAVerifier) VerifySignature(id arma.PartyID, shardID arma.ShardID, msg, sig []byte) error {
	pk, exists := v[struct {
		party arma.PartyID
		shard arma.ShardID
	}{
		party: id,
		shard: shardID,
	}]
	if !exists {
		return fmt.Errorf("node %d and shard %d does not exist", id, shardID)
	}

	digest := sha256.Sum256(msg)

	if ecdsa.VerifyASN1(&pk, digest[:], sig) {
		return nil
	}

	return fmt.Errorf("signature %s of node %d", base64.StdEncoding.EncodeToString(sig), id)
}
