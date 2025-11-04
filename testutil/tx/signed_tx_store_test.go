/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tx_test

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

func TestSignedTransactionStore(t *testing.T) {
	numOfTxs := 100
	txSize := 300

	dir := createCryptoForArmaNetwork(t, 1, 1)
	require.NotNil(t, dir)

	keyPath := filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "users", "user", "msp", "keystore", "priv_sk")
	keyBytes, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	certPath := filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "users", "user", "msp", "signcerts", "sign-cert.pem")
	certBytes, err := os.ReadFile(certPath)
	require.NoError(t, err)

	txStore, err := tx.NewSignedTransactionStore(numOfTxs, txSize, keyBytes, certBytes)
	require.NoError(t, err)

	// verify all txs
	for i := 0; i < numOfTxs; i++ {
		isValid := verifyTransaction(txStore.Txs[i], txStore.SignerCrypto.Certificate)
		require.True(t, isValid)
	}
}

func createCryptoForArmaNetwork(t *testing.T, parties int, shards int) string {
	dir := t.TempDir()
	require.NotNil(t, dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	testutil.CreateNetwork(t, configPath, parties, shards, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	return dir
}

// VerifyTransaction verifies the signature over message.
// This method is used for testing only.
func verifyTransaction(envelope *common.Envelope, certificate *x509.Certificate) bool {
	// Get the transaction from the list
	digest := sha256.Sum256(envelope.Payload)

	// Extract public key from the cert
	publicKey := certificate.PublicKey.(*ecdsa.PublicKey)

	// Verify the signature over the digest with the public key
	valid := ecdsa.VerifyASN1(publicKey, digest[:], envelope.Signature)
	return valid
}
