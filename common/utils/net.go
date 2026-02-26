/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func ExtractCertificateFromContext(ctx context.Context) *x509.Certificate {
	pr, extracted := peer.FromContext(ctx)
	if !extracted {
		return nil
	}

	authInfo := pr.AuthInfo
	if authInfo == nil {
		return nil
	}

	tlsInfo, isTLSConn := authInfo.(credentials.TLSInfo)
	if !isTLSConn {
		return nil
	}
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return nil
	}

	if certs[0] == nil {
		return nil
	}

	return certs[0]
}

func ExtractClientAddressFromContext(ctx context.Context) (string, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("failed to extract peer from context")
	}
	if peer.Addr == net.Addr(nil) {
		return "", fmt.Errorf("peer address is nil")
	}
	return peer.Addr.String(), nil
}

func CertificateBytesToString(cert []byte) (string, error) {
	x509Cert, err := Parsex509Cert(cert)
	if err != nil {
		return "", err
	}
	return CertificateToString(x509Cert), nil
}

func AreCertificatesEqual(cert1, cert2 []byte) (bool, error) {
	x509Cert1, err := Parsex509Cert(cert1)
	if err != nil {
		return false, err
	}
	x509Cert2, err := Parsex509Cert(cert2)
	if err != nil {
		return false, err
	}

	// Compare RawTBSCertificate fields
	return bytes.Equal(x509Cert1.RawTBSCertificate, x509Cert2.RawTBSCertificate), nil
}

func CertificateToString(cert *x509.Certificate) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Certificate:\n")
	fmt.Fprintf(&sb, "%4sVersion: %d\n", "", cert.Version)
	fmt.Fprintf(&sb, "%4sSerial Number: %s\n", "", cert.SerialNumber.String())
	fmt.Fprintf(&sb, "%4sSignature Algorithm: %s\n", "", cert.SignatureAlgorithm.String())
	fmt.Fprintf(&sb, "%4sIssuer: %s\n", "", cert.Issuer.String())
	fmt.Fprintf(&sb, "%4sValidity:\n", "")
	fmt.Fprintf(&sb, "%8sNot Before: %s\n", "", cert.NotBefore.UTC().String())
	fmt.Fprintf(&sb, "%8sNot After : %s\n", "", cert.NotAfter.UTC().String())
	fmt.Fprintf(&sb, "%4sSubject: %s\n", "", cert.Subject.String())
	fmt.Fprintf(&sb, "%4sPublic Key Algorithm: %s\n", "", cert.PublicKeyAlgorithm.String())

	// Print DNS Names
	if len(cert.DNSNames) > 0 {
		fmt.Fprintf(&sb, "%4sDNS Names: %v\n", "", cert.DNSNames)
	}
	// Print SANs etc.
	if len(cert.EmailAddresses) > 0 {
		fmt.Fprintf(&sb, "%4sEmail Addresses: %v\n", "", cert.EmailAddresses)
	}
	if len(cert.IPAddresses) > 0 {
		fmt.Fprintf(&sb, "%4sIP Addresses: %v\n", "", cert.IPAddresses)
	}
	// print raw PEM
	// pem.Encode(&sb, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})

	return sb.String()
}

func GetPortFromEndpoint(endpoint string) uint32 {
	if strings.Contains(endpoint, ":") {
		_, portS, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		port, err := strconv.ParseUint(portS, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return uint32(port)
	}

	return 0
}

func TrimPortFromEndpoint(endpoint string) string {
	if strings.Contains(endpoint, ":") {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return host
	}

	return endpoint
}
