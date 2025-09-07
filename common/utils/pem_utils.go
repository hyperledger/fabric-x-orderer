/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

func ReadPem(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("failed reading pem file, path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading a pem file from %s, err: %v", path, err)
	}

	return data, nil
}

func blockToPublicKey(block *pem.Block) []byte {
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		panic(fmt.Sprintf("Failed parsing consenter signing certificate: %v", err))
	}

	pubKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		panic(fmt.Sprintf("Failed parsing consenter public key: %v", err))
	}

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		panic(fmt.Sprintf("Failed marshaling consenter public key: %v", err))
	}

	pemPublicKey := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return pemPublicKey
}

func GetPublicKeyFromCertificate(nodeCert []byte) []byte {
	// Fetch public key from signing certificate
	// NOTE: ARMA's new configuration now uses certificates, which inherently contain the public key, instead of a separate public key field.
	// To ensure backward compatibility until the full new config integration, the public key it enabled.
	block, _ := pem.Decode(nodeCert)
	if block == nil || block.Bytes == nil {
		panic("Failed decoding consenter signing certificate")
	}

	var pemPublicKey []byte
	if block.Type == "CERTIFICATE" {
		pemPublicKey = blockToPublicKey(block)
	}

	if block.Type == "PUBLIC KEY" {
		pemPublicKey = nodeCert
	}

	return pemPublicKey
}
