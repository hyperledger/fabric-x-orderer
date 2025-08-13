/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"math/big"
	"os"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/ca"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
)

// SignedTransactionService holds signed transactions with the cryptographic keys.
type SignedTransactionService struct {
	txs         []*common.Envelope
	privateKey  *ecdsa.PrivateKey
	certificate *x509.Certificate
}

func NewSignedTransactionService(numOfTxs int, txSize int) (*SignedTransactionService, error) {
	// Create private key and certificate used to sign and verify txs
	pk, cert, err := createSignerPKAndCert()
	if err != nil {
		return nil, err
	}

	// Create signed transactions
	txs, err := createSignedTransactions(numOfTxs, txSize, (*crypto.ECDSASigner)(pk))
	if err != nil {
		return nil, err
	}

	service := &SignedTransactionService{
		txs:         txs,
		privateKey:  pk,
		certificate: cert,
	}

	return service, nil
}

// GetRandomTransactionIndex returns a random number in [0, len(txs)).
func (sts *SignedTransactionService) GetRandomTransactionIndex() (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(sts.txs))))
	return int(n.Int64()), err
}

// VerifyTransaction verifies the transaction[txIndex] in the transactions list.
func (sts *SignedTransactionService) VerifyTransaction(txIndex int) bool {
	// Get the transaction from the list
	envelope := sts.txs[txIndex]
	digest := sha256.Sum256(envelope.Payload)

	// Extract public key from the cert
	publicKey := sts.certificate.PublicKey.(*ecdsa.PublicKey)

	// Verify the signature over the digest with the public key
	valid := ecdsa.VerifyASN1(publicKey, digest[:], envelope.Signature)
	return valid
}

// createSignerPKAndCert create a CA, private key and a certificate.
// NOTE: this function is based on Fabric internal methods.
func createSignerPKAndCert() (*ecdsa.PrivateKey, *x509.Certificate, error) {
	// Create a fake CA
	dir, err := os.MkdirTemp("", "ca")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a temp dir, err: %s", err)
	}
	defer os.RemoveAll(dir)

	signCA, err := ca.NewCA(dir, "signCA", "ca", "US", "California", "San Francisco", "ARMA", "addr", "12345", "ecdsa")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a fake CA, err: %s", err)
	}

	// Create private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a private key, err: %s", err)
	}

	// Issue a certificate with the public key associated to the generated private key (the certificate contains the public key)
	cert, err := signCA.SignCertificate(dir, "signer", nil, nil, getPublicKey(privateKey), x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a certificate, err: %s", err)
	}

	return privateKey, cert, err
}

func createSignedTransactions(numOfTxs int, txSize int, signer *crypto.ECDSASigner) ([]*common.Envelope, error) {
	sessionNumber := make([]byte, 16)
	_, err := rand.Read(sessionNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to create a session number, err: %s", err)
	}

	txs := make([]*common.Envelope, numOfTxs)
	for i := 0; i < numOfTxs; i++ {
		payload := createPayload(i, txSize, sessionNumber)
		signedTx, err := signTransaction(payload, signer)
		if err != nil {
			return nil, fmt.Errorf("failed to sign transaction %d", i)
		}
		txs[i] = signedTx
	}

	return txs, nil
}

func createPayload(txNum int, txSize int, sessionNumber []byte) []byte {
	payload := prepareTx(txNum, txSize, sessionNumber)
	return payload
}

func signTransaction(payload []byte, signer *crypto.ECDSASigner) (*common.Envelope, error) {
	signature, err := signer.Sign(payload)
	if err != nil {
		return nil, err
	}

	envelope := &common.Envelope{
		Payload:   payload,
		Signature: signature,
	}
	return envelope, nil
}
