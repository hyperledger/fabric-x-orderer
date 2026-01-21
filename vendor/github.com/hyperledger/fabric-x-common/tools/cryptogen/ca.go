/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cryptogen

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/cockroachdb/errors"
)

// caParams describes a CA for crypto generation.
type caParams struct {
	Organization       string
	Name               string
	Country            string
	Province           string
	Locality           string
	OrganizationalUnit string
	StreetAddress      string
	PostalCode         string
	KeyAlgorithm       string

	// These fields are filled by the buildCA() method.
	Signer   crypto.Signer
	SignCert *x509.Certificate
}

// signCertParams describes the parameters for the signCertificate() method.
type signCertParams struct {
	OrgUnits       []string
	AlternateNames []string
	KeyUsage       x509.KeyUsage
	ExtKeyUsage    []x509.ExtKeyUsage
	PublicKey      crypto.PublicKey
}

type certParams struct {
	Template   *x509.Certificate
	Parent     *x509.Certificate
	PublicKey  crypto.PublicKey
	PrivateKey any
}

// caFromSpec creates a CA from a node spec, generates, and saves the signing key pair in baseDir/name.
func caFromSpec(baseDir, orgName, namePrefix string, s *NodeSpec) (*caParams, error) {
	newCA := &caParams{
		Organization:       orgName,
		Name:               namePrefix + s.CommonName,
		Country:            s.Country,
		Province:           s.Province,
		Locality:           s.Locality,
		OrganizationalUnit: s.OrganizationalUnit,
		StreetAddress:      s.StreetAddress,
		PostalCode:         s.PostalCode,
		KeyAlgorithm:       s.PublicKeyAlgorithm,
	}
	err := buildCA(baseDir, newCA)
	return newCA, err
}

// buildCA generates and saves the signing key pair in baseDir/name.
func buildCA(baseDir string, ca *caParams) error {
	err := os.MkdirAll(baseDir, 0o750)
	if err != nil {
		return errors.Wrapf(err, "cannot create directory %s", baseDir)
	}

	priv, err := generatePrivateKey(baseDir, ca.KeyAlgorithm)
	if err != nil {
		return err
	}
	ca.Signer = newSignerFromPrivateKey(priv)

	template := x509Template()
	// this is a CA
	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageDigitalSignature |
		x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign |
		x509.KeyUsageCRLSign
	template.ExtKeyUsage = []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	}

	// set the organization for the subject
	subject := subjectTemplateAdditional(ca)
	subject.Organization = []string{ca.Organization}
	subject.CommonName = ca.Name

	template.Subject = subject
	template.SubjectKeyId, err = computeSKI(priv)
	if err != nil {
		return err
	}

	ca.SignCert, err = genCertificate(baseDir, ca.Name, certParams{
		Template:   &template,
		Parent:     &template,
		PublicKey:  getPublicKey(priv),
		PrivateKey: priv,
	})
	return err
}

func loadCA(caDir string, spec *OrgSpec, name string) (*caParams, error) {
	privateKey, err := loadPrivateKey(caDir)
	if err != nil {
		return nil, err
	}
	cert, err := loadCertificate(caDir)
	if err != nil {
		return nil, err
	}
	return &caParams{
		Name:               name,
		Signer:             newSignerFromPrivateKey(privateKey),
		SignCert:           cert,
		Country:            spec.CA.Country,
		Province:           spec.CA.Province,
		Locality:           spec.CA.Locality,
		OrganizationalUnit: spec.CA.OrganizationalUnit,
		StreetAddress:      spec.CA.StreetAddress,
		PostalCode:         spec.CA.PostalCode,
	}, nil
}

// signCertificate creates a signed certificate based on a built-in template and saves it in baseDir/name.
func (ca *caParams) signCertificate(baseDir, name string, p signCertParams) (*x509.Certificate, error) {
	template := x509Template()
	template.KeyUsage = p.KeyUsage
	template.ExtKeyUsage = p.ExtKeyUsage

	// set the organization for the subject
	subject := subjectTemplateAdditional(ca)
	subject.CommonName = name
	subject.OrganizationalUnit = append(subject.OrganizationalUnit, p.OrgUnits...)

	template.Subject = subject
	for _, san := range p.AlternateNames {
		// try to parse as an IP address first
		ip := net.ParseIP(san)
		if ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, san)
		}
	}

	return genCertificate(baseDir, name, certParams{
		Template:   &template,
		Parent:     ca.SignCert,
		PublicKey:  p.PublicKey,
		PrivateKey: ca.Signer,
	})
}

// computeSKI compute Subject Key Identifier using RFC 7093, Section 2, Method 4.
func computeSKI(privKey crypto.PrivateKey) ([]byte, error) {
	var raw []byte

	// Marshall the public key
	switch kk := privKey.(type) {
	case *ecdsa.PrivateKey:
		//nolint:errcheck,forcetypeassert // implementation always returns this type.
		ecdhKey, err := kk.Public().(*ecdsa.PublicKey).ECDH()
		if err != nil {
			return nil, fmt.Errorf("private key transition failed: %w", err)
		}
		raw = ecdhKey.Bytes()
	case ed25519.PrivateKey:
		//nolint:errcheck,revive,forcetypeassert // implementation always returns this type.
		raw = kk.Public().(ed25519.PublicKey)
	}

	// Hash it
	hash := sha256.Sum256(raw)
	return hash[:], nil
}

// subjectTemplate default template for X509 subject.
func subjectTemplate() pkix.Name {
	return pkix.Name{
		Country:  []string{"US"},
		Locality: []string{"San Francisco"},
		Province: []string{"California"},
	}
}

// subjectTemplateAdditional additional for X509 subject.
func subjectTemplateAdditional(ca *caParams) pkix.Name {
	name := subjectTemplate()
	if len(ca.Country) > 0 {
		name.Country = []string{ca.Country}
	}
	if len(ca.Province) > 0 {
		name.Province = []string{ca.Province}
	}
	if len(ca.Locality) > 0 {
		name.Locality = []string{ca.Locality}
	}
	if len(ca.OrganizationalUnit) > 0 {
		name.OrganizationalUnit = []string{ca.OrganizationalUnit}
	}
	if len(ca.StreetAddress) > 0 {
		name.StreetAddress = []string{ca.StreetAddress}
	}
	if len(ca.PostalCode) > 0 {
		name.PostalCode = []string{ca.PostalCode}
	}
	return name
}

// x509Template default template for X509 certificates.
func x509Template() x509.Certificate {
	// generate a serial number
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)

	// set expiry to around 10 years
	expiry := 3650 * 24 * time.Hour
	// round minute and backdate 5 minutes
	notBefore := time.Now().Round(time.Minute).Add(-5 * time.Minute).UTC()

	// basic template to use
	return x509.Certificate{
		SerialNumber:          serialNumber,
		NotBefore:             notBefore,
		NotAfter:              notBefore.Add(expiry).UTC(),
		BasicConstraintsValid: true,
	}
}

// genCertificate generate a signed X509 certificate using ECDSA.
func genCertificate(baseDir, name string, p certParams) (*x509.Certificate, error) {
	// create the x509 public cert
	certBytes, err := x509.CreateCertificate(rand.Reader, p.Template, p.Parent, p.PublicKey, p.PrivateKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create certificate")
	}

	x509Cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse certificate")
	}

	return x509Cert, writePEM(x509FilePath(baseDir, name), CertType, certBytes)
}

// newSignerFromPrivateKey creates a signer from a private key.
func newSignerFromPrivateKey(priv crypto.PrivateKey) crypto.Signer {
	switch kk := priv.(type) {
	case *ecdsa.PrivateKey:
		return &ECDSASigner{
			PrivateKey: kk,
		}
	case ed25519.PrivateKey:
		return &ED25519Signer{
			PrivateKey: kk,
		}
	default:
		panic("unsupported key algorithm")
	}
}
