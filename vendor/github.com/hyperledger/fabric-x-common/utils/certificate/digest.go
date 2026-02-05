/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package certificate

import (
	"crypto/sha256"
	"crypto/sha3"
	"crypto/sha512"
	"crypto/x509"
	"encoding/pem"
	"hash"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-lib-go/bccsp"
)

// DigestFromFile creates a hash of the content of the passed file.
func DigestFromFile(pemCertPath, hashFunc string) ([]byte, error) {
	cert, err := GetCert(pemCertPath)
	if err != nil {
		return nil, err
	}

	return DigestCert(cert, hashFunc)
}

// DigestPemContent creates a hash of the content of the PEM.
func DigestPemContent(pemContent []byte, hashFunc string) ([]byte, error) {
	cert, err := GetCertFromBytes(pemContent)
	if err != nil {
		return nil, err
	}
	return DigestCert(cert, hashFunc)
}

// DigestCert creates a hash of certificate.
func DigestCert(cert *x509.Certificate, hashFunc string) ([]byte, error) {
	var hasher hash.Hash
	switch hashFunc {
	case bccsp.SHA256:
		hasher = sha256.New()
	case bccsp.SHA384:
		hasher = sha512.New384()
	case bccsp.SHA3_256:
		hasher = sha3.New256()
	case bccsp.SHA3_384:
		hasher = sha3.New384()
	default:
		return nil, errors.Newf("unsupported hash function: %s", hashFunc)
	}

	if _, err := hasher.Write(cert.Raw); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

// GetCert reads a PEM-encoded X.509 certificate from the specified file path.
// and returns the parsed certificate.
func GetCert(certPath string) (*x509.Certificate, error) {
	pemContent, err := os.ReadFile(filepath.Clean(certPath))
	if err != nil {
		return nil, errors.Wrap(err, "cannot read certificate")
	}
	block, _ := pem.Decode(pemContent)
	if block == nil {
		return nil, errors.Newf("no PEM content in file %s", certPath)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse cert")
	}

	return cert, nil
}

// GetCertFromBytes reads a PEM-encoded X.509 certificate and returns the parsed certificate.
func GetCertFromBytes(pemContent []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemContent)
	if block == nil {
		return nil, errors.New("no PEM content in file")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse cert")
	}
	return cert, nil
}
