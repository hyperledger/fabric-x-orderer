/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package tx

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
)

// SignedTransactionStore holds signed transactions with the cryptographic keys.
// The signers can be clients or admins.
// A signed data transaction that is sent to a router should pass all verification checks (signature verification and writers policy verification).
// A signed config transaction that is sent to a router should pass all verification checks (signature verification and writers + admins policy verification).
type SignedTransactionStore struct {
	Txs          []*common.Envelope
	SignerCrypto *SignerCrypto
}

type SignerCrypto struct {
	PrivateKey  *ecdsa.PrivateKey
	Certificate *x509.Certificate
}

func NewSignedTransactionStore(numOfTxs int, txSize int, privateKeyBytes []byte, certificateBytes []byte) (*SignedTransactionStore, error) {
	// Decode private key and certificate
	privateKey, certificate, err := parseCertAndKeyBytes(privateKeyBytes, certificateBytes)
	if err != nil {
		return nil, err
	}

	// Create signed transactions
	txs, err := createSignedTransactions(numOfTxs, txSize, (*crypto.ECDSASigner)(privateKey))
	if err != nil {
		return nil, err
	}

	service := &SignedTransactionStore{
		Txs: txs,
		SignerCrypto: &SignerCrypto{
			PrivateKey:  privateKey,
			Certificate: certificate,
		},
	}

	return service, nil
}

func parseCertAndKeyBytes(privateKeyBytes []byte, certificateBytes []byte) (*ecdsa.PrivateKey, *x509.Certificate, error) {
	// Parse private key bytes
	block, _ := pem.Decode(privateKeyBytes)
	if block == nil || block.Bytes == nil {
		return nil, nil, fmt.Errorf("failed decoding private key PEM")
	}

	if block.Type != "PRIVATE KEY" {
		return nil, nil, fmt.Errorf("unexpected pem type, got a %s", strings.ToLower(block.Type))
	}

	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed parsing private key DER: %v", err)
	}

	// Parse certificates bytes
	pbl, _ := pem.Decode(certificateBytes)
	if pbl == nil || pbl.Bytes == nil {
		return nil, nil, fmt.Errorf("no pem content for cert")
	}

	if pbl.Type != "CERTIFICATE" {
		return nil, nil, fmt.Errorf("unexpected pem type, got a %s", strings.ToLower(pbl.Type))
	}

	certificate, err := x509.ParseCertificate(pbl.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed parsing certificate")
	}

	return priv.(*ecdsa.PrivateKey), certificate, nil
}

func createSignedTransactions(numOfTxs int, txSize int, signer *crypto.ECDSASigner) ([]*common.Envelope, error) {
	txs := make([]*common.Envelope, numOfTxs)
	buff := make([]byte, txSize)

	for i := 0; i < numOfTxs; i++ {
		binary.BigEndian.PutUint32(buff, uint32(i))
		payload := createStructuredPayload(buff, common.HeaderType_MESSAGE)
		payloadBytes := deterministicMarshall(payload)
		signedTx, err := signTransaction(payloadBytes, signer)
		if err != nil {
			return nil, fmt.Errorf("failed to sign transaction %d", i)
		}
		txs[i] = signedTx
	}

	return txs, nil
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
