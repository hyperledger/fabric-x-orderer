/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// Mock address implementing net.Addr
type mockAddr struct {
	addr string
}

func (m mockAddr) Network() string { return "tcp" }
func (m mockAddr) String() string  { return m.addr }

func TestExtractClientAddressFromContext(t *testing.T) {
	t.Run("Valid peer with address", func(t *testing.T) {
		expectedAddr := "127.0.0.1:8080"
		mockPeer := &peer.Peer{
			Addr: mockAddr{addr: expectedAddr},
		}
		ctx := peer.NewContext(context.Background(), mockPeer)

		addr, err := ExtractClientAddressFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedAddr, addr)
	})

	t.Run("Missing peer in context", func(t *testing.T) {
		ctx := context.Background()
		_, err := ExtractClientAddressFromContext(ctx)
		require.Error(t, err, "peer address is nil")
	})

	t.Run("Peer with nil address", func(t *testing.T) {
		mockPeer := &peer.Peer{
			Addr: nil,
		}
		ctx := peer.NewContext(context.Background(), mockPeer)

		_, err := ExtractClientAddressFromContext(ctx)
		require.Error(t, err, "peer address is nil")
	})
}

// mockAuth is a fake AuthInfo implementation for testing
type mockAuth struct{}

func (m *mockAuth) AuthType() string {
	return "mock"
}

func TestExtractCertificateFromContext(t *testing.T) {
	t.Run("No peer in context", func(t *testing.T) {
		ctx := context.Background()
		cert := ExtractCertificateFromContext(ctx)
		require.Nil(t, cert)
	})

	t.Run("Peer with nil AuthInfo", func(t *testing.T) {
		p := &peer.Peer{AuthInfo: nil}
		ctx := peer.NewContext(context.Background(), p)
		cert := ExtractCertificateFromContext(ctx)
		require.Nil(t, cert)
	})

	t.Run("Peer with non-TLS AuthInfo", func(t *testing.T) {
		mockAuthInfo := &mockAuth{}
		p := &peer.Peer{AuthInfo: mockAuthInfo}
		ctx := peer.NewContext(context.Background(), p)
		cert := ExtractCertificateFromContext(ctx)
		require.Nil(t, cert)
	})

	t.Run("TLSInfo with empty PeerCertificates", func(t *testing.T) {
		tlsInfo := credentials.TLSInfo{}
		p := &peer.Peer{AuthInfo: tlsInfo}
		ctx := peer.NewContext(context.Background(), p)
		cert := ExtractCertificateFromContext(ctx)
		require.Nil(t, cert)
	})

	t.Run("TLSInfo with nil first certificate", func(t *testing.T) {
		tlsInfo := credentials.TLSInfo{
			State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{nil},
			},
		}
		p := &peer.Peer{AuthInfo: tlsInfo}
		ctx := peer.NewContext(context.Background(), p)
		cert := ExtractCertificateFromContext(ctx)
		require.Nil(t, cert)
	})

	t.Run("Valid certificate in TLSInfo", func(t *testing.T) {
		template := x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				CommonName: "my-test-cert",
			},
			NotBefore: time.Now(),
			NotAfter:  time.Now().Add(24 * time.Hour),

			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
		}

		testCert := generateTestCertificate(t, &template)
		tlsInfo := credentials.TLSInfo{
			State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{testCert},
			},
		}
		p := &peer.Peer{AuthInfo: tlsInfo}
		ctx := peer.NewContext(context.Background(), p)
		cert := ExtractCertificateFromContext(ctx)
		require.NotNil(t, cert)
		require.Equal(t, "my-test-cert", cert.Subject.CommonName)
	})
}

func TestCertificateToString(t *testing.T) {
	SerialNumber := 123456789
	CommonName := "example.com"
	Organization := "Example Org"
	Email := "admin@example.com"
	DNS1 := "example.com"
	DNS2 := "www.example.com"
	IP := "127.0.0.1"

	SerialNumberString := strconv.Itoa(123456789)
	EmailAddresses := []string{Email}
	DNSNames := []string{DNS1, DNS2}
	IPAddresses := []net.IP{net.ParseIP(IP)}

	template := x509.Certificate{
		SerialNumber: big.NewInt(int64(SerialNumber)),
		Subject: pkix.Name{
			CommonName:   CommonName,
			Organization: []string{Organization},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		SignatureAlgorithm:    x509.SHA256WithRSA,
		PublicKeyAlgorithm:    x509.RSA,
		EmailAddresses:        EmailAddresses,
		DNSNames:              DNSNames,
		IPAddresses:           IPAddresses,
		BasicConstraintsValid: true,
	}

	cert := generateTestCertificate(t, &template)

	output := CertificateToString(cert)

	expectedChecks := []string{
		"Certificate:",
		"Version: 3",
		"Serial Number: " + SerialNumberString,
		"Signature Algorithm: SHA256-RSA",
		"Subject: CN=" + CommonName + ",O=" + Organization,
		"Not Before:",
		"Not After :",
		"Public Key Algorithm: RSA",
		"DNS Names: [" + DNS1 + " " + DNS2 + "]",
		"Email Addresses: [" + Email + "]",
		"IP Addresses: [" + IP + "]",
	}

	for _, expected := range expectedChecks {
		require.Contains(t, output, expected)
	}
}

func generateTestCertificate(t *testing.T, template *x509.Certificate) *x509.Certificate {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)

	return cert
}
