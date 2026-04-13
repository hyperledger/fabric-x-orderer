/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/stretchr/testify/require"
)

func TestValidatePartyCertificates_TLS(t *testing.T) {
	now := time.Now()
	tlsCACert, tlsCAKey, tlsCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	tlsCertPEM := generateTestTLSCert(t, tlsCACert, tlsCAKey, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

	signCACert, signCAKey, signCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	signCertPEM := generateTestSigningCert(t, signCACert, signCAKey, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

	t.Run("Valid party certificates", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  tlsCertPEM,
				SignCert: signCertPEM,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{
					TlsCert:  tlsCertPEM,
					SignCert: signCertPEM,
				},
			},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.NoError(t, err)
	})

	t.Run("Empty TLS CA certificates", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty TLS CA certificates")
	})

	t.Run("Invalid TLS CA certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{[]byte("invalid")},
			CACerts:    [][]byte{signCACertPEM},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid TLS CA certificate")
	})

	t.Run("Invalid router TLS certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: []byte("invalid"),
			},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "router TLS certificate validation failed")
	})

	t.Run("TLS certificate not signed by provided CA", func(t *testing.T) {
		_, _, differentTLSCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{differentTLSCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "router TLS certificate validation failed")
	})

	t.Run("Expired consenter TLS certificate", func(t *testing.T) {
		// expired consenter cert
		expiredConsenterTLSCert := generateTestTLSCert(t, tlsCACert, tlsCAKey, now.Add(-48*time.Hour), now.Add(-24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  expiredConsenterTLSCert,
				SignCert: signCertPEM,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{
					TlsCert:  tlsCertPEM,
					SignCert: signCertPEM,
				},
			},
		}

		// Should fail when expiration is enforced
		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "consenter TLS certificate validation failed")

		// Should succeed when expiration is ignored
		err = validatePartyCertificates(partyConfig, true)
		require.NoError(t, err)
	})
}

func TestValidatePartyCertificates_Signing(t *testing.T) {
	now := time.Now()
	tlsCACert, tlsCAKey, tlsCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	tlsCertPEM := generateTestTLSCert(t, tlsCACert, tlsCAKey, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

	signCACert, signCAKey, signCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	signCertPEM := generateTestSigningCert(t, signCACert, signCAKey, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

	t.Run("Empty signing CA certificates", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty signing CA certificates")
	})

	t.Run("Invalid signing CA certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{[]byte("invalid")},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid CA certificate")
	})

	t.Run("Invalid consenter signing certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  tlsCertPEM,
				SignCert: []byte("invalid"),
			},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "consenter signing certificate validation failed")
	})

	t.Run("Signing certificate not signed by provided CA", func(t *testing.T) {
		_, _, differentSignCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{differentSignCACertPEM}, // Different CA
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  tlsCertPEM,
				SignCert: signCertPEM, // Signed by different CA
			},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "consenter signing certificate validation failed")
	})

	t.Run("Expired consenter signing certificate", func(t *testing.T) {
		expiredSignCert := generateTestSigningCert(t, signCACert, signCAKey, now.Add(-48*time.Hour), now.Add(-24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  tlsCertPEM,
				SignCert: expiredSignCert,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{
					TlsCert:  tlsCertPEM,
					SignCert: signCertPEM,
				},
			},
		}

		// Should fail when expiration is enforced
		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "consenter signing certificate validation failed")

		// Should succeed when expiration is ignored
		err = validatePartyCertificates(partyConfig, true)
		require.NoError(t, err)
	})
	t.Run("Invalid batcher signing certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  tlsCertPEM,
				SignCert: signCertPEM,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{
					TlsCert:  tlsCertPEM,
					SignCert: []byte("invalid"),
				},
			},
		}

		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batcher signing certificate validation failed")
	})

	t.Run("Expired batcher signing certificate", func(t *testing.T) {
		expiredSignCert := generateTestSigningCert(t, signCACert, signCAKey, now.Add(-48*time.Hour), now.Add(-24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{tlsCACertPEM},
			CACerts:    [][]byte{signCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: tlsCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: tlsCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert:  tlsCertPEM,
				SignCert: signCertPEM,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{
					TlsCert:  tlsCertPEM,
					SignCert: expiredSignCert,
				},
			},
		}

		// Should fail when expiration is enforced
		err := validatePartyCertificates(partyConfig, false)
		require.Error(t, err)

		// Should succeed when expiration is ignored
		err = validatePartyCertificates(partyConfig, true)
		require.NoError(t, err)
	})
}

func generateTestCAWithKey(t *testing.T, notBefore, notAfter time.Time) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return cert, priv, certPEM
}

func generateTestTLSCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, notBefore, notAfter time.Time) []byte {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test TLS"},
			CommonName:   "Test TLS",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return certPEM
}

func generateTestSigningCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, notBefore, notAfter time.Time) []byte {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Signing"},
			CommonName:   "Test Signing",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}
