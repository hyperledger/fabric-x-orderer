/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"crypto/x509"
	"encoding/pem"
	"time"

	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

// validatePartyCertificates validates TLS CA and CA certificates, and verifies TLS and signing certificates for the party.
func validatePartyCertificates(party *ordererpb.PartyConfig, ignoreExpiration bool) error {
	if len(party.TLSCACerts) == 0 {
		return errors.New("empty TLS CA certificates")
	}

	if len(party.CACerts) == 0 {
		return errors.New("empty signing CA certificates")
	}

	tlsPool := x509.NewCertPool()
	for _, raw := range party.TLSCACerts {
		cert, err := validateCACert(raw)
		if err != nil {
			return errors.Wrap(err, "invalid TLS CA certificate")
		}
		tlsPool.AddCert(cert)
	}

	tlsOpts := x509.VerifyOptions{
		Roots:         tlsPool,
		Intermediates: nil, // TODO: add intermediate TLS CA certs when added to the party config
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
	}

	signPool := x509.NewCertPool()
	for _, raw := range party.CACerts {
		cert, err := validateCACert(raw)
		if err != nil {
			return errors.Wrap(err, "invalid CA certificate")
		}
		signPool.AddCert(cert)
	}

	signOpts := x509.VerifyOptions{
		Roots:         signPool,
		Intermediates: nil, // TODO: add intermediate CA certs when added to the party config
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageAny,
		},
	}

	if party.RouterConfig != nil {
		if err := verifyCert(party.RouterConfig.TlsCert, tlsOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "router TLS certificate validation failed")
		}
		if err := verifyCert(party.RouterConfig.SignCert, signOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "router signing certificate validation failed")
		}
	} else {
		return errors.New("router config is nil")
	}

	if party.AssemblerConfig != nil {
		if err := verifyCert(party.AssemblerConfig.TlsCert, tlsOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "assembler TLS certificate validation failed")
		}
		if err := verifyCert(party.AssemblerConfig.SignCert, signOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "assembler signing certificate validation failed")
		}
	} else {
		return errors.New("assembler config is nil")
	}

	if party.ConsenterConfig != nil {
		if err := verifyCert(party.ConsenterConfig.TlsCert, tlsOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "consenter TLS certificate validation failed")
		}
		if err := verifyCert(party.ConsenterConfig.SignCert, signOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "consenter signing certificate validation failed")
		}
	} else {
		return errors.New("consenter config is nil")
	}

	for _, b := range party.BatchersConfig {
		if b == nil {
			return errors.New("batcher config is nil")
		}

		if err := verifyCert(b.TlsCert, tlsOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "batcher TLS certificate validation failed")
		}
		if err := verifyCert(b.SignCert, signOpts, ignoreExpiration); err != nil {
			return errors.Wrap(err, "batcher signing certificate validation failed")
		}

	}

	return nil
}

// validateUniqueSigningCerts verifies that no two nodes share a signing certificate.
func validateUniqueSigningCerts(parties []*ordererpb.PartyConfig) error {
	type nodeSignCert struct {
		node arma_types.NodeIdentity
		cert []byte
	}

	signCertOwners := make(map[string]arma_types.NodeIdentity)
	for _, party := range parties {
		if party == nil {
			return errors.New("party config is nil")
		}

		partyID := arma_types.PartyID(party.PartyID)
		signCerts := []nodeSignCert{
			{node: arma_types.NewRouterIdentity(partyID), cert: party.GetRouterConfig().GetSignCert()},
			{node: arma_types.NewAssemblerIdentity(partyID), cert: party.GetAssemblerConfig().GetSignCert()},
			{node: arma_types.NewConsenterIdentity(partyID), cert: party.GetConsenterConfig().GetSignCert()},
		}
		for _, b := range party.BatchersConfig {
			signCerts = append(signCerts, nodeSignCert{
				node: arma_types.NewBatcherIdentity(partyID, arma_types.ShardID(b.GetShardID())),
				cert: b.GetSignCert(),
			})
		}

		for _, sc := range signCerts {
			cert, err := parseCertificateFromBytes(sc.cert)
			if err != nil {
				return errors.Wrapf(err, "invalid signing certificate of %s", sc.node)
			}

			certKey := string(cert.Raw)
			if owner, exists := signCertOwners[certKey]; exists {
				return errors.Errorf("%s and %s share the same signing certificate", owner, sc.node)
			}
			signCertOwners[certKey] = sc.node
		}
	}

	return nil
}

// validateCACert decodes PEM cert, parses and ensures it is a CA.
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

	return cert, nil
}

// verifyCert decodes PEM cert, parses and verifies the certificate chain, with optional expiration handling.
func verifyCert(raw []byte, opts x509.VerifyOptions, ignoreExpiration bool) error {
	if len(raw) == 0 {
		return errors.New("certificate is empty")
	}

	cert, err := parseCertificateFromBytes(raw)
	if err != nil {
		return err
	}

	if ignoreExpiration {
		opts.CurrentTime = cert.NotBefore
	}

	if _, err := cert.Verify(opts); err != nil {
		return errors.Wrapf(err, "verifying certificate with serial number %d", cert.SerialNumber)
	}

	if ignoreExpiration && time.Now().Before(cert.NotBefore) {
		return errors.Errorf("certificate with serial number %d is not yet valid", cert.SerialNumber)
	}

	return nil
}

func parseCertificateFromBytes(cert []byte) (*x509.Certificate, error) {
	pemBlock, _ := pem.Decode(cert)
	if pemBlock == nil {
		return nil, errors.New("no PEM data found in certificate")
	}

	if pemBlock.Type != "CERTIFICATE" {
		return nil, errors.Errorf("expected PEM block of type CERTIFICATE, but found %s", pemBlock.Type)
	}

	certificate, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse certificate from ASN1 structure")
	}

	return certificate, nil
}
