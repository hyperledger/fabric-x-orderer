/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
)

type ECDSASigner ecdsa.PrivateKey

func (s ECDSASigner) Sign(message []byte) ([]byte, error) {
	digest := sha256.Sum256(message)
	sk := (ecdsa.PrivateKey)(s)
	return sk.Sign(rand.Reader, digest[:], nil)
}
