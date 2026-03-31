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

func TestValidatePartyCertificates(t *testing.T) {
	now := time.Now()
	caCert, caKey, caCertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	nodeCertPEM := generateTestNodeCert(t, caCert, caKey, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

	t.Run("Valid party certificates", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{caCertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: nodeCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: nodeCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert: nodeCertPEM,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{TlsCert: nodeCertPEM},
			},
		}

		err := validatePartyCertificates(partyConfig)
		require.NoError(t, err)
	})

	t.Run("Invalid TLS CA certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{[]byte("invalid")},
		}

		err := validatePartyCertificates(partyConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid TLS CA certificate")
	})

	t.Run("Expired TLS CA certificate", func(t *testing.T) {
		// Create a CA that expired 1 year ago
		_, _, expiredCACertPEM := generateTestCAWithKey(t, now.Add(-2*365*24*time.Hour), now.Add(-365*24*time.Hour))
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{expiredCACertPEM},
		}

		err := validatePartyCertificates(partyConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid TLS CA certificate")
	})

	t.Run("Invalid router TLS certificate", func(t *testing.T) {
		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{caCertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: []byte("invalid"),
			},
		}

		err := validatePartyCertificates(partyConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "router TLS validation failed")
	})

	t.Run("Certificate not signed by provided CA", func(t *testing.T) {
		_, _, differentCACertPEM := generateTestCAWithKey(t, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{differentCACertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: nodeCertPEM,
			},
		}

		err := validatePartyCertificates(partyConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "router TLS validation failed")
	})

	t.Run("Expired consenter TLS certificate", func(t *testing.T) {
		// expired consenter cert
		expiredConsenterCert := generateTestNodeCert(t, caCert, caKey, now.Add(-48*time.Hour), now.Add(-24*time.Hour))

		partyConfig := &config_protos.PartyConfig{
			TLSCACerts: [][]byte{caCertPEM},
			RouterConfig: &config_protos.RouterNodeConfig{
				TlsCert: nodeCertPEM,
			},
			AssemblerConfig: &config_protos.AssemblerNodeConfig{
				TlsCert: nodeCertPEM,
			},
			ConsenterConfig: &config_protos.ConsenterNodeConfig{
				TlsCert: expiredConsenterCert,
			},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{TlsCert: nodeCertPEM},
			},
		}

		err := validatePartyCertificates(partyConfig)
		require.Error(t, err)
		require.Contains(t, err.Error(), "consenter TLS validation failed")
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

func generateTestNodeCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, notBefore, notAfter time.Time) []byte {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Node"},
			CommonName:   "Test Node",
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
