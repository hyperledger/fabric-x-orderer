/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
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
