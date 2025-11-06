/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto_test

import (
	"crypto/ecdsa"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/utils"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/csp"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/stretchr/testify/require"
)

func TestECDSASigner(t *testing.T) {
	// Generate a private key
	dir := t.TempDir()
	cryptoPrivateKey, err := csp.GeneratePrivateKey(dir, "ecdsa")
	require.NoError(t, err)
	ecdsaPrivateKey := cryptoPrivateKey.(*ecdsa.PrivateKey)
	signer := crypto.ECDSASigner(*ecdsaPrivateKey)

	// Sign a message
	msg := []byte("foo")
	sig, err := signer.Sign(msg)
	require.NoError(t, err)

	// Verify the signature over the massage
	r, s, err := utils.UnmarshalECDSASignature(sig)
	require.NoError(t, err)
	verify := ecdsa.Verify(&((*ecdsa.PrivateKey)(&signer).PublicKey), util.ComputeSHA256(msg), r, s)
	require.True(t, verify)
}
