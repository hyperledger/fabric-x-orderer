package node

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

type ECDSAVerifier map[uint16]ecdsa.PublicKey

func (v ECDSAVerifier) VerifySignature(id uint16, msg, sig []byte) error {
	pk, exists := v[id]
	if !exists {
		return fmt.Errorf("node %d does not exist", id)
	}

	digest := sha256.Sum256(msg)

	if ecdsa.VerifyASN1(&pk, digest[:], sig) {
		return nil
	}

	return fmt.Errorf("signature %s of node %d", base64.StdEncoding.EncodeToString(sig), id)
}
