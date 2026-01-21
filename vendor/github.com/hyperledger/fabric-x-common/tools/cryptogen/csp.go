/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cryptogen

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"io"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// Common constants.
const (
	ECDSA   = "ecdsa"
	ED25519 = "ed25519"

	CertType       = "CERTIFICATE"
	PrivateKeyType = "PRIVATE KEY"

	PrivateKeySuffix = "_sk"
	PrivateKeyFile   = "priv" + PrivateKeySuffix
	CertFileExt      = ".pem"
	CertSuffix       = "-cert" + CertFileExt
)

// generatePrivateKey creates an ecdsa private key using a P-256 curve or an ed25519 key
// and stores it in keystorePath.
func generatePrivateKey(keystorePath, keyAlg string) (priv crypto.PrivateKey, err error) {
	switch keyAlg {
	case ECDSA:
		priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case ED25519:
		_, priv, err = ed25519.GenerateKey(rand.Reader)
	default:
		err = errors.Newf("unsupported key algorithm: %s", keyAlg)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to generate private key")
	}

	pkcs8Encoded, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal private key")
	}

	keyFile := filepath.Join(keystorePath, PrivateKeyFile)
	return priv, writePEM(keyFile, PrivateKeyType, pkcs8Encoded)
}

// loadPrivateKey loads a private key from a file in keystorePath.  It looks
// for a file ending in "_sk" and expects a PEM-encoded PKCS8 EC private key.
func loadPrivateKey(keystorePath string) (crypto.PrivateKey, error) {
	keyPath, block, err := findAndDecodePem(keystorePath, PrivateKeySuffix, PrivateKeyType)
	if err != nil {
		return nil, err
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.Wrapf(err, "PEM bytes are not PKCS8 encoded [%s]", keyPath)
	}

	_, isEcdsa := key.(*ecdsa.PrivateKey)
	_, isEd25519 := key.(ed25519.PrivateKey)
	if !isEcdsa && !isEd25519 {
		return nil, errors.Errorf("PEM bytes do not contain an ECDSA nor ed25519 private key [%s]", keyPath)
	}
	return key, nil
}

// loadCertificate load an ECDSA cert from a file in cert path.
func loadCertificate(certPath string) (*x509.Certificate, error) {
	var cert *x509.Certificate
	certPath, block, err := findAndDecodePem(certPath, CertFileExt, CertType)
	if err != nil {
		return nil, err
	}
	cert, err = x509.ParseCertificate(block.Bytes)
	return cert, errors.Wrapf(err, "wrong DER encoding [%s]", certPath)
}

func findAndDecodePem(pemDirPath, suffix, blockType string) (
	retPath string, block *pem.Block, err error,
) {
	err = filepath.WalkDir(pemDirPath, func(curPath string, dir os.DirEntry, _ error) error {
		if dir.IsDir() || !strings.HasSuffix(curPath, suffix) {
			return nil
		}
		rawPEM, readErr := os.ReadFile(curPath)
		if readErr != nil {
			return errors.Wrapf(readErr, "failed to read PEM file [%s]", curPath)
		}
		curBlock, _ := pem.Decode(rawPEM)
		if curBlock == nil {
			return errors.Errorf("bytes are not PEM encoded [%s]", curPath)
		}
		if curBlock.Type != blockType {
			return errors.Errorf("wrong PEM encoding [%s]", curPath)
		}
		block = curBlock
		retPath = curPath
		// We can skip the rest of the directory.
		return filepath.SkipAll
	})
	if err == nil && block == nil {
		return "", nil, errors.Errorf(
			"no '%s' PEM blocks found with file suffix '%s' [%s]", blockType, suffix, pemDirPath,
		)
	}
	return retPath, block, err
}

func x509FilePath(name ...string) string {
	return path.Join(name...) + CertSuffix
}

func writeCert(outputPath string, cert *x509.Certificate) error {
	return writePEM(outputPath, CertType, cert.Raw)
}

func writePEM(outputPath, pemType string, bytes []byte) error {
	pemEncoded := pem.EncodeToMemory(&pem.Block{Type: pemType, Bytes: bytes})
	err := os.WriteFile(outputPath, pemEncoded, 0o600)
	return errors.Wrapf(err, "failed to save PEM to file [%s]", outputPath)
}

// ECDSASigner ECDSA signer implements the crypto.Signer interface for ECDSA keys.  The
// Sign method ensures signatures are created with Low S values since Fabric
// normalizes all signatures to Low S.
// See https://github.com/bitcoin/bips/blob/master/bip-0146.mediawiki#low_s
// for more detail.
type ECDSASigner struct {
	PrivateKey *ecdsa.PrivateKey
}

// Public returns the ecdsa.PublicKey associated with PrivateKey.
func (e *ECDSASigner) Public() crypto.PublicKey {
	return &e.PrivateKey.PublicKey
}

// Sign signs the digest and ensures that signatures use the Low S value.
func (e *ECDSASigner) Sign(randReader io.Reader, digest []byte, _ crypto.SignerOpts) ([]byte, error) {
	r, s, err := ecdsa.Sign(randReader, e.PrivateKey, digest)
	if err != nil {
		return nil, err
	}

	// ensure Low S signatures
	sig := toLowS(
		e.PrivateKey.PublicKey,
		ECDSASignature{
			R: r,
			S: s,
		},
	)

	// return marshaled signature
	return asn1.Marshal(sig)
}

// When using ECDSA, both (r,s) and (r, -s mod n) are valid signatures.  In order
// to protect against signature malleability attacks, Fabric normalizes all
// signatures to a canonical form where s is at most half the order of the curve.
// In order to make signatures compliant with what Fabric expects, toLowS creates
// signatures in this canonical form.
func toLowS(key ecdsa.PublicKey, sig ECDSASignature) ECDSASignature {
	// calculate half order of the curve
	halfOrder := new(big.Int).Div(key.Curve.Params().N, big.NewInt(2))
	// check if s is greater than half order of curve
	if sig.S.Cmp(halfOrder) == 1 {
		// Set s to N - s so that s will be less than or equal to half order
		sig.S.Sub(key.Params().N, sig.S)
	}
	return sig
}

// ECDSASignature represents a signature.
type ECDSASignature struct {
	R, S *big.Int
}

// ED25519Signer represents the signer.
type ED25519Signer struct {
	PrivateKey ed25519.PrivateKey
}

// Public returns the ed25519.PublicKey associated with PrivateKey.
func (e *ED25519Signer) Public() crypto.PublicKey {
	return e.PrivateKey.Public()
}

// Sign signs the digest.
func (e *ED25519Signer) Sign(_ io.Reader, msg []byte, _ crypto.SignerOpts) ([]byte, error) {
	sig := ed25519.Sign(e.PrivateKey, msg)
	return sig, nil
}
