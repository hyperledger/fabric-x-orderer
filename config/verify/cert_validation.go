/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"crypto/x509"
	"encoding/pem"
	"time"

	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/pkg/errors"
)

// validatePartyCertificates validates CA certificates and verifies node certificates for the party.
func validatePartyCertificates(party *config_protos.PartyConfig) error {
	tlsPool := x509.NewCertPool()
	for _, raw := range party.TLSCACerts {
		cert, err := validateCACert(raw)
		if err != nil {
			return errors.Wrap(err, "invalid TLS CA certificate")
		}
		tlsPool.AddCert(cert)
	}

	tlsOpts := x509.VerifyOptions{
		Roots:       tlsPool,
		CurrentTime: time.Now(),
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
	}

	if party.RouterConfig != nil {
		if err := verifyCert(party.RouterConfig.TlsCert, tlsOpts); err != nil {
			return errors.Wrap(err, "router TLS validation failed")
		}
	} else {
		return errors.New("router config is nil")
	}

	if party.AssemblerConfig != nil {
		if err := verifyCert(party.AssemblerConfig.TlsCert, tlsOpts); err != nil {
			return errors.Wrap(err, "assembler TLS validation failed")
		}
	} else {
		return errors.New("assembler config is nil")
	}

	if party.ConsenterConfig != nil {
		if err := verifyCert(party.ConsenterConfig.TlsCert, tlsOpts); err != nil {
			return errors.Wrap(err, "consenter TLS validation failed")
		}
	} else {
		return errors.New("consenter config is nil")
	}

	for _, b := range party.BatchersConfig {
		if b == nil {
			return errors.Errorf("batcher config is nil")
		}

		if err := verifyCert(b.TlsCert, tlsOpts); err != nil {
			return errors.Wrap(err, "batcher TLS validation failed")
		}

	}

	// TODO: Add similar validation for CACerts and SignCert.
	// TODO: Validate certificate matches configured host (SAN).
	return nil
}

// validateCACert parses a certificate and ensures it is a CA within its validity period.
func validateCACert(raw []byte) (*x509.Certificate, error) {
	if len(raw) == 0 {
		return nil, errors.New("certificate is empty")
	}
	cert, err := parseCertificateFromBytes(raw)
	if err != nil {
		return nil, err
	}

	if !cert.IsCA {
		return nil, errors.Errorf("certificate %q is not a CA", cert.Subject.String())
	}

	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return nil, errors.Errorf("certificate %q is expired or not yet valid", cert.Subject.String())
	}

	return cert, nil
}

// verifyCert validates certificate chain, expiration, and key usage.
func verifyCert(raw []byte, opts x509.VerifyOptions) error {
	if len(raw) == 0 {
		return errors.New("certificate is empty")
	}
	cert, err := parseCertificateFromBytes(raw)
	if err != nil {
		return err
	}
	_, err = cert.Verify(opts)
	return err
}

func parseCertificateFromBytes(cert []byte) (*x509.Certificate, error) {
	pemBlock, _ := pem.Decode(cert)
	if pemBlock == nil {
		return nil, errors.Errorf("no PEM data found in certificate %x", cert)
	}

	certificate, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse certificate from ASN1 structure")
	}

	return certificate, nil
}
