/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-common/tools/cryptogen"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/ca"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
)

// GenerateCryptoConfigWithProfile provides all crypto material of Arma network, divided into parties and written to files in a folder structure.
// For each party for all nodes (i.e. Router, Batchers, Consenter and Assembler) it is required to provide TLS certificate and private key for secure communications between nodes using TLS.
// For Batchers and Consenters is it also required to provide a signing certificate with a corresponding private key that are used to sign BAS's by Batchers and blocks by Consenter.
// NOTE: for compatability with Fabric cryptogen tool and for future use, signing certificate and a corresponding private key will be created for all nodes.
//
// Folder structure:
//
// dir
// └── crypto
//
//			└── ordererOrganizations
//			        └── org{partyID}
//			            ├── ca   (sign ca cert + private key)
//			            ├── tlsca  (tls ca cert + private key)
//			            ├── msp
//			                  ├── cacerts
//			                  ├── tlscacerts
//	                          ├── knowncerts
//			                  ├── admincerts
//			            ├── orderers
//			            │    └── party{partyID}
//			            │          ├── router
//			            │          │   ├── tls
//			            │          │   └── msp
//			            │          │       ├── cacerts
//			            │          │       ├── admincerts  (ignored)
//			            │          │       ├── keystore
//			            │          │       ├── signcerts
//			            │          │       ├── tlscacerts
//			            │          ├── batcher1
//			            │          ├── batcher2
//			            │          ├── ...
//			            │          ├── batcher{shards}
//			            │          ├── consenter
//			            │          └── assembler
//			            └── users
//		                  └── user (admin, orderer loadgen)
//		                       ├── tls
//			                   └── msp
//			                        ├── cacerts
//			                        ├── admincerts  (ignored)
//			                        ├── keystore
//			                        ├── signcerts
//			                        ├── tlscacerts
func GenerateCryptoConfigWithProfile(networkConfig *generate.Network, outputDir string) (*configtxgen.Profile, error) {
	orgs := make([]cryptogen.OrganizationParameters, 0, len(networkConfig.Parties)+len(networkConfig.Peers))

	for _, party := range networkConfig.Parties {
		partyName := fmt.Sprintf("party%d", party.ID)
		routerHost := utils.TrimPortFromEndpoint(party.RouterEndpoint)
		routerNodeSANS := []string{routerHost}
		routerTLSHostname := routerHost
		if party.RouterTLSHostname != "" {
			routerTLSHostname = party.RouterTLSHostname
			routerNodeSANS = append(routerNodeSANS, routerTLSHostname)
		}
		assemblerHost := utils.TrimPortFromEndpoint(party.AssemblerEndpoint)
		assemblerNodeSANS := []string{assemblerHost}
		assemblerTLSHostname := assemblerHost
		if party.AssemblerTLSHostname != "" {
			assemblerTLSHostname = party.AssemblerTLSHostname
			assemblerNodeSANS = append(assemblerNodeSANS, assemblerTLSHostname)
		}
		consenterHost := utils.TrimPortFromEndpoint(party.ConsenterEndpoint)
		routerPort := utils.GetPortFromEndpoint(party.RouterEndpoint)
		assemblerPort := utils.GetPortFromEndpoint(party.AssemblerEndpoint)

		ordererNodes := []cryptogen.Node{
			{
				CommonName: "router",
				PartyName:  partyName,
				Hostname:   routerHost,
				SANS:       routerNodeSANS,
			},
			{
				CommonName: "assembler",
				PartyName:  partyName,
				Hostname:   assemblerHost,
				SANS:       assemblerNodeSANS,
			},
		}
		for batcherID, ep := range party.BatchersEndpoints {
			batcherHost := utils.TrimPortFromEndpoint(ep)
			ordererNodes = append(ordererNodes, cryptogen.Node{
				CommonName: fmt.Sprintf("batcher%d", batcherID+1),
				PartyName:  partyName,
				Hostname:   batcherHost,
				SANS:       []string{batcherHost},
			})
		}

		orgName := fmt.Sprintf("org%d", party.ID)
		orgs = append(orgs, cryptogen.OrganizationParameters{
			Name:             orgName,
			Domain:           orgName,
			OrdererEndpoints: generate.BuildOrdererEndpoints(uint32(party.ID), routerTLSHostname, int(routerPort), assemblerTLSHostname, int(assemblerPort)),
			ConsenterNodes: []cryptogen.Node{{
				Hostname:   party.ConsenterEndpoint,
				CommonName: "consenter",
				PartyName:  partyName,
				SANS:       []string{consenterHost},
			}},
			OrdererNodes: ordererNodes,
		})
	}

	for _, peer := range networkConfig.Peers {
		orgs = append(orgs, cryptogen.OrganizationParameters{
			Name:   peer,
			Domain: peer,
			PeerNodes: []cryptogen.Node{
				newPeer(peer),
			},
		})
	}

	targetPath := filepath.Join(outputDir, "crypto")
	err := os.MkdirAll(targetPath, 0o750)
	if err != nil {
		return nil, errors.Wrap(err, "error creating crypto artifacts folder")
	}

	return cryptogen.CreateOrExtendProfileWithCrypto(&cryptogen.ConfigBlockParameters{
		TargetPath:    targetPath,
		BaseProfile:   configtxgen.SampleFabricX,
		ChannelID:     "arma",
		Organizations: orgs,
	})
}

func newPeer(name string) cryptogen.Node {
	return cryptogen.Node{
		CommonName: name,
		Hostname:   name,
		SANS:       []string{"localhost", "127.0.0.1"},
	}
}

func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(dstFile, srcFile)
	closeErr := dstFile.Close()
	if copyErr != nil {
		os.Remove(dst) // best-effort cleanup of a partial file
		return copyErr
	}
	return closeErr
}

func CreateNewCertificateFromCA(caCertPath string, caPrivateKeyPath string, certType string, pathToNewCert string, pathToNewPrivateKey string, nodesIPs []string) ([]byte, error) {
	var ku x509.KeyUsage
	switch certType {
	case "tls":
		ku = x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature
	case "sign":
		ku = x509.KeyUsageDigitalSignature
	default:
		return nil, fmt.Errorf("unsupported cert type: %s", certType)
	}

	caCertBytes, err := utils.ReadPem(caCertPath)
	if err != nil {
		return nil, err
	}

	caCert, err := utils.Parsex509Cert(caCertBytes)
	if err != nil {
		return nil, err
	}

	caPrivKeyBytes, err := utils.ReadPem(caPrivateKeyPath)
	if err != nil {
		return nil, err
	}

	caPrivateKey, err := tx.CreateECDSAPrivateKey(caPrivKeyBytes)
	if err != nil {
		return nil, err
	}

	ca := &ca.CA{
		Name:               "ca",
		Country:            "US",
		Province:           "California",
		Locality:           "San Francisco",
		OrganizationalUnit: "ARMA",
		StreetAddress:      "addr",
		PostalCode:         "12345",
		Signer:             caPrivateKey,
		SignCert:           caCert,
	}

	// create a new private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed creating a new private key, err: %s", err)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling private key, err: %s", err)
	}

	newCertDir := filepath.Dir(pathToNewCert)
	_, err = ca.SignCertificate(newCertDir, certType, nil, nodesIPs, GetPublicKey(privateKey), ku, []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	})
	if err != nil {
		return nil, err
	}

	err = os.Rename(filepath.Join(newCertDir, fmt.Sprintf("%s-cert.pem", certType)), pathToNewCert)
	if err != nil {
		return nil, err
	}

	err = utils.WritePEMToFile(pathToNewPrivateKey, "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return nil, err
	}

	newCertBytes, err := os.ReadFile(pathToNewCert)
	if err != nil {
		return nil, err
	}

	return newCertBytes, nil
}

func GetPublicKey(priv crypto.PrivateKey) crypto.PublicKey {
	switch kk := priv.(type) {
	case *ecdsa.PrivateKey:
		return &kk.PublicKey
	case ed25519.PrivateKey:
		return kk.Public()
	default:
		panic("unsupported key algorithm")
	}
}
