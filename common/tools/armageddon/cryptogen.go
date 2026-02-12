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
	"strings"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/ca"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/msp"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
)

const (
	cacerts              = "cacerts"
	intermediatecerts    = "intermediatecerts"
	admincerts           = "admincerts"
	signcerts            = "signcerts"
	keystore             = "keystore"
	tlscacerts           = "tlscacerts"
	tlsintermediatecerts = "tlsintermediatecerts"
)

// GenerateCryptoConfig provides all crypto material of Arma network, divided into parties and written to files in a folder structure.
// For each party for all nodes (i.e. Router, Batchers, Consenter and Assembler) it is required to provide TLS certificate and private key for secure communications between nodes using TLS.
// For Batchers and Consenters is it also required to provide a signing certificate with a corresponding private key that are used to sign BAS's by Batchers and blocks by Consenter.
// NOTE: for compatability with Fabric cryptogen tool and for future use, signing certificate and a corresponding private key will be created for all nodes.
//
// Folder structure:
//
// dir
// └── crypto
//
//		└── ordererOrganizations
//		        └── org{partyID}
//		            ├── ca   (sign ca cert + private key)
//		            ├── tlsca  (tls ca cert + private key)
//		            ├── msp
//		                  ├── cacerts
//		                  ├── tlscacerts
//		                  ├── admincerts
//		            ├── orderers
//		            │    └── party{partyID}
//		            │          ├── router
//		            │          │   ├── tls
//		            │          │   └── msp
//		            │          │       ├── cacerts
//		            │          │       ├── intermediatecerts
//		            │          │       ├── admincerts  (ignored)
//		            │          │       ├── keystore
//		            │          │       ├── signcerts
//		            │          │       ├── tlscacerts
//		            │          │       └── tlsintermediatecerts
//		            │          ├── batcher1
//		            │          ├── batcher2
//		            │          ├── ...
//		            │          ├── batcher{shards}
//		            │          ├── consenter
//		            │          └── assembler
//		            └── users
//	                  └── user (admin, orderer loadgen)
//	                       ├── tls
//		                   └── msp
//		                        ├── cacerts
//		                        ├── intermediatecerts
//		                        ├── admincerts  (ignored)
//		                        ├── keystore
//		                        ├── signcerts
//		                        ├── tlscacerts
//		                        └── tlsintermediatecerts
func GenerateCryptoConfig(networkConfig *genconfig.Network, outputDir string) error {
	// create folder structure for the crypto files
	err := generateNetworkCryptoConfigFolderStructure(outputDir, networkConfig)
	if err != nil {
		return fmt.Errorf("error creating directories for crypto material: %s", err)
	}

	// create crypto config material for each party
	err = createNetworkCryptoMaterial(outputDir, networkConfig)
	if err != nil {
		return err
	}

	// for each organization, copy the ca and tlsca directories to the msp/cacerts and msp/tlscacerts for each node.
	err = copyCACerts(networkConfig, outputDir)
	if err != nil {
		return err
	}

	return nil
}

// createNetworkCryptoMaterial creates the crypto files for each party's nodes and write them into files.
func createNetworkCryptoMaterial(dir string, network *genconfig.Network) error {
	// create TLS CA and Sign CA for each party
	// NOTE: a party can have several CA's, meanwhile cryptogen creates only one CA for each party.
	partiesTLSCAs, partiesSignCAs, err := createCAsPerParty(dir, network)
	if err != nil {
		return err
	}

	// collect all node IPs as they are includes as SANs when generating TLS certificates.
	nodesIPs := getNodeIPs(network)

	// create crypto material for each party's nodes and write the crypto into files
	for _, party := range network.Parties {
		// choose a TLS CA and a signing CA of the party
		listOfTLSCAs := partiesTLSCAs[party.ID]
		tlsCA := listOfTLSCAs[0]

		listOfSignCAs := partiesSignCAs[party.ID]
		signCA := listOfSignCAs[0]

		// signing crypto to admin of the organization
		err = createAdminSignCertAndPrivateKey(signCA, dir, party.ID)
		if err != nil {
			return err
		}
		adminCertsOfOrgPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "msp", "admincerts")

		// create crypto material for each party's nodes
		// TLS crypto for router
		err = createTLSCertKeyPairForNode(tlsCA, dir, party.RouterEndpoint, "router", party.ID, nodesIPs)
		if err != nil {
			return err
		}

		// TLS crypto for consenter
		err = createTLSCertKeyPairForNode(tlsCA, dir, party.ConsenterEndpoint, "consenter", party.ID, nodesIPs)
		if err != nil {
			return err
		}

		// TLS crypto for assembler
		err = createTLSCertKeyPairForNode(tlsCA, dir, party.AssemblerEndpoint, "assembler", party.ID, nodesIPs)
		if err != nil {
			return err
		}

		// TLS crypto for batchers
		for j, batcherEndpoint := range party.BatchersEndpoints {
			err = createTLSCertKeyPairForNode(tlsCA, dir, batcherEndpoint, fmt.Sprintf("batcher%d", j+1), party.ID, nodesIPs)
			if err != nil {
				return err
			}
		}

		// TLS crypto for user
		err = createUserTLSCertKeyPair(tlsCA, dir, party.ID, nil)
		if err != nil {
			return err
		}

		// signing crypto to router
		err = createSignCertAndPrivateKeyForNode(signCA, dir, party.RouterEndpoint, "router", party.ID, nil)
		if err != nil {
			return err
		}
		// copy the org admin to router/msp/admincerts
		dst := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "orderers", fmt.Sprintf("party%d", party.ID), "router", "msp", "admincerts")
		err = copyPEMFiles(adminCertsOfOrgPath, dst)
		if err != nil {
			return err
		}

		// signing crypto for batchers
		for j, batcherEndpoint := range party.BatchersEndpoints {
			err = createSignCertAndPrivateKeyForNode(signCA, dir, batcherEndpoint, fmt.Sprintf("batcher%d", j+1), party.ID, nil)
			if err != nil {
				return err
			}
			// copy the org admin to batcher_j/msp/admincerts
			dst = filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "orderers", fmt.Sprintf("party%d", party.ID), fmt.Sprintf("batcher%d", j+1), "msp", "admincerts")
			err = copyPEMFiles(adminCertsOfOrgPath, dst)
			if err != nil {
				return err
			}
		}

		// signing crypto to consenter
		err = createSignCertAndPrivateKeyForNode(signCA, dir, party.ConsenterEndpoint, "consenter", party.ID, nil)
		if err != nil {
			return err
		}
		// copy the org admin to consenter/msp/admincerts
		dst = filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "orderers", fmt.Sprintf("party%d", party.ID), "consenter", "msp", "admincerts")
		err = copyPEMFiles(adminCertsOfOrgPath, dst)
		if err != nil {
			return err
		}

		// signing crypto to assembler
		err = createSignCertAndPrivateKeyForNode(signCA, dir, party.AssemblerEndpoint, "assembler", party.ID, nil)
		if err != nil {
			return err
		}
		// copy the org admin to assembler/msp/admincerts
		dst = filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "orderers", fmt.Sprintf("party%d", party.ID), "assembler", "msp", "admincerts")
		err = copyPEMFiles(adminCertsOfOrgPath, dst)
		if err != nil {
			return err
		}

		// signing crypto to user (non admin client)
		err = createUserSignCertAndPrivateKey(signCA, dir, party.ID, nil)
		if err != nil {
			return err
		}
		// copy the org admin to user/msp/admincerts
		dst = filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "users", "user", "msp", "admincerts")
		err = copyPEMFiles(adminCertsOfOrgPath, dst)
		if err != nil {
			return err
		}
	}
	return nil
}

// createCAsPerParty creates a TLS CA and a signing CA for each party and write the ca's certificates into files.
func createCAsPerParty(dir string, network *genconfig.Network) (map[types.PartyID][]*ca.CA, map[types.PartyID][]*ca.CA, error) {
	partiesTLSCAs := make(map[types.PartyID][]*ca.CA)
	partiesSignCAs := make(map[types.PartyID][]*ca.CA)

	for _, party := range network.Parties {
		// create a TLS CA for the party
		pathToTLSCACert := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "tlsca")
		tlsCA, err := ca.NewCA(pathToTLSCACert, "tlsCA", "tlsca", "US", "California", "San Francisco", "ARMA", "addr", "12345", "ecdsa")
		if err != nil {
			return nil, nil, fmt.Errorf("err: %s, failed creating a TLS CA for party %d", err, party.ID)
		}
		err = copyPEMFiles(pathToTLSCACert, filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "msp", "tlscacerts"))
		if err != nil {
			return nil, nil, err
		}

		partiesTLSCAs[party.ID] = []*ca.CA{tlsCA}

		// create a Signing CA for the party
		pathToSignCACert := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "ca")
		signCA, err := ca.NewCA(pathToSignCACert, "signCA", "ca", "US", "California", "San Francisco", "ARMA", "addr", "12345", "ecdsa")
		if err != nil {
			return nil, nil, fmt.Errorf("err: %s, failed creating a signing CA for party %d", err, party.ID)
		}
		err = copyPEMFiles(pathToSignCACert, filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "msp", "cacerts"))
		if err != nil {
			return nil, nil, err
		}

		partiesSignCAs[party.ID] = []*ca.CA{signCA}
	}

	return partiesTLSCAs, partiesSignCAs, nil
}

// createTLSCertKeyPairForNode creates a TLS cert,key pair signed by a corresponding CA for an Arma node and write them into files.
func createTLSCertKeyPairForNode(ca *ca.CA, dir string, endpoint string, role string, partyID types.PartyID, nodesIPs []string) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for "+role+" node %s", err, endpoint)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for "+role+" node %s", err, endpoint)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role, "tls"), "tls", nil, nodesIPs, GetPublicKey(privateKey), x509.KeyUsageCertSign|x509.KeyUsageCRLSign, []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	})
	err = utils.WritePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role, "tls", "key.pem"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// createUserTLSCertKeyPair creates a TLS cert,key pair signed by a corresponding CA for a user.
func createUserTLSCertKeyPair(ca *ca.CA, dir string, partyID types.PartyID, nodesIPs []string) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for user party %d", err, partyID)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for user for party %d", err, partyID)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "users", "user", "tls"), "user-tls", nil, nodesIPs, GetPublicKey(privateKey), x509.KeyUsageKeyEncipherment|x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	})
	err = utils.WritePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "users", "user", "tls", "user-key.pem"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// createSignCertAndPrivateKeyForNode creates a signed certificate with a corresponding private key used for signing and write them into files.
// Cert and private key for signing are used only by the batchers and the consenters. However, for future use, this pair is also created for the router and the assembler.
func createSignCertAndPrivateKeyForNode(ca *ca.CA, dir string, endpoint string, role string, partyID types.PartyID, nodesIPs []string) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for "+role+" node %s", err, endpoint)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for "+role+" node %s", err, endpoint)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role, "msp", "signcerts"), "sign", nil, nodesIPs, GetPublicKey(privateKey), x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{})
	err = utils.WritePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role, "msp", "keystore", "priv_sk"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// createUserSignCertAndPrivateKey creates for user a signed certificate with a corresponding private key used for signing and write them into files.
func createUserSignCertAndPrivateKey(ca *ca.CA, dir string, partyID types.PartyID, nodesIPs []string) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for user of party %d", err, partyID)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for user of party %d", err, partyID)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "users", "user", "msp", "signcerts"), "sign", nil, nodesIPs, GetPublicKey(privateKey), x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{})
	err = utils.WritePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "users", "user", "msp", "keystore", "priv_sk"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// createAdminSignCertAndPrivateKey creates for admin a signed certificate with a corresponding private key.
// This tool is designed to create an admin for each org and the corresponding certificate appears under org/msp/admincerts.
// The admin certificate is replicated to each organization's path: <org>/orderers/<party>/<node>/msp/admincerts as well as to the <org>/users/admin
func createAdminSignCertAndPrivateKey(ca *ca.CA, dir string, partyID types.PartyID) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for admin of org %d", err, partyID)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for admin of party %d", err, partyID)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "msp", "admincerts"), fmt.Sprintf("Admin@Org%d", partyID), []string{msp.ADMINOU}, nil, GetPublicKey(privateKey), x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{})
	err = copyPEMFiles(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "msp", "admincerts"), filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "users", "admin", "msp", "signcerts"))
	if err != nil {
		return err
	}
	err = utils.WritePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "users", "admin", "msp", "keystore", "priv_sk"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// generateNetworkCryptoConfigFolderStructure creates folders where the crypto material is written to.
func generateNetworkCryptoConfigFolderStructure(dir string, network *genconfig.Network) error {
	var folders []string

	rootDir := filepath.Join(dir, "crypto", "ordererOrganizations")
	folders = append(folders, rootDir)

	for _, p := range network.Parties {
		folders = generateOrdererOrg(rootDir, folders, int(p.ID), len(p.BatchersEndpoints))
	}
	for _, folder := range folders {
		err := os.MkdirAll(folder, 0o755)
		if err != nil {
			return err
		}
	}
	return nil
}

// generateOrdererOrg generates folders for an orderer organization.
// In general, an organization can include one or more parties, but this crypto-gen tool is designed for the scenario where each organization corresponds to a single party.
// A local MSP directory is also created for each party's node (i.e., Router, Batcher, Consenter and Assembler) which defines the identity of that node.
func generateOrdererOrg(rootDir string, folders []string, partyID int, shards int) []string {
	orgDir := filepath.Join(rootDir, fmt.Sprintf("org%d", partyID))
	folders = append(folders, orgDir)
	folders = append(folders, filepath.Join(orgDir, "ca"))
	folders = append(folders, filepath.Join(orgDir, "tlsca"))
	folders = append(folders, filepath.Join(orgDir, "msp", cacerts))
	folders = append(folders, filepath.Join(orgDir, "msp", tlscacerts))
	folders = append(folders, filepath.Join(orgDir, "msp", admincerts))
	orderersDir := filepath.Join(orgDir, "orderers")
	folders = append(folders, orderersDir)

	partyDir := filepath.Join(orderersDir, fmt.Sprintf("party%d", partyID))
	partyNodes := []string{"router"}
	for j := 1; j <= shards; j++ {
		partyNodes = append(partyNodes, fmt.Sprintf("batcher%d", j))
	}
	partyNodes = append(partyNodes, "consenter", "assembler")

	mspSubDirs := []string{
		cacerts,
		intermediatecerts,
		admincerts,
		keystore,
		signcerts,
		tlscacerts,
		tlsintermediatecerts,
	}

	for _, nodeName := range partyNodes {
		nodePath := filepath.Join(partyDir, nodeName)
		mspPath := filepath.Join(nodePath, "msp")
		for _, subDir := range mspSubDirs {
			folders = append(folders, filepath.Join(mspPath, subDir))
		}
		folders = append(folders, filepath.Join(nodePath, "tls"))
	}

	folders = append(folders, filepath.Join(orgDir, "users"))
	userMSPPath := filepath.Join(orgDir, "users", "user", "msp")
	folders = append(folders, userMSPPath)
	for _, subDir := range mspSubDirs {
		folders = append(folders, filepath.Join(userMSPPath, subDir))
	}
	folders = append(folders, filepath.Join(orgDir, "users", "user", "tls"))

	adminUserMSPPath := filepath.Join(orgDir, "users", "admin", "msp")
	folders = append(folders, adminUserMSPPath)
	for _, subDir := range mspSubDirs {
		folders = append(folders, filepath.Join(adminUserMSPPath, subDir))
	}
	folders = append(folders, filepath.Join(orgDir, "users", "admin", "tls"))

	return folders
}

func getNodeIPs(network *genconfig.Network) []string {
	var nodeIPs []string
	for _, party := range network.Parties {
		nodeIPs = append(nodeIPs, utils.TrimPortFromEndpoint(party.RouterEndpoint))
		for _, batcherEndpoint := range party.BatchersEndpoints {
			nodeIPs = append(nodeIPs, utils.TrimPortFromEndpoint(batcherEndpoint))
		}
		nodeIPs = append(nodeIPs, utils.TrimPortFromEndpoint(party.ConsenterEndpoint))
		nodeIPs = append(nodeIPs, utils.TrimPortFromEndpoint(party.AssemblerEndpoint))
	}
	return nodeIPs
}

func copyCACerts(networkConfig *genconfig.Network, outputDir string) error {
	for _, party := range networkConfig.Parties {
		orgDir := filepath.Join(outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID))
		partyDir := filepath.Join(orgDir, "orderers", fmt.Sprintf("party%d", party.ID))
		caDir := filepath.Join(orgDir, "msp", "cacerts")
		tlscaDir := filepath.Join(orgDir, "msp", "tlscacerts")

		roles := []string{"router", "assembler", "consenter"}
		for j := range party.BatchersEndpoints {
			roles = append(roles, fmt.Sprintf("batcher%d", j+1))
		}

		for _, role := range roles {
			caDstDir := filepath.Join(partyDir, role, "msp", "cacerts")
			tlscaDstDir := filepath.Join(partyDir, role, "msp", "tlscacerts")
			err := copyPEMFiles(caDir, caDstDir)
			if err != nil {
				return fmt.Errorf("err copying file from %s to %s", caDir, caDstDir)
			}
			err = copyPEMFiles(tlscaDir, tlscaDstDir)
			if err != nil {
				return fmt.Errorf("err copying file from %s to %s", tlscaDir, tlscaDstDir)
			}
		}
	}

	for _, party := range networkConfig.Parties {
		orgDir := filepath.Join(outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID))
		cacertsDir := filepath.Join(orgDir, "msp", "cacerts")
		cacertsDstDir := filepath.Join(orgDir, "users", "user", "msp", "cacerts")
		err := copyPEMFiles(cacertsDir, cacertsDstDir)
		if err != nil {
			return fmt.Errorf("err copying file from %s to %s", cacertsDir, cacertsDstDir)
		}
	}

	return nil
}

func copyPEMFiles(srcDir, destDir string) error {
	if srcDir == "" {
		return fmt.Errorf("missing source directory")
	}

	if destDir == "" {
		return fmt.Errorf("missing destination directory")
	}

	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("error reading source directory: %w", err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".pem") {
			continue
		}

		srcPath := filepath.Join(srcDir, entry.Name())
		destPath := filepath.Join(destDir, entry.Name())

		if err := copyFile(srcPath, destPath); err != nil {
			return fmt.Errorf("error copying %s: %w", entry.Name(), err)
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func CreateNewCertificateFromCA(caCertPath string, caPrivateKeyPath string, pathToNewTLSCert string, pathToNewTLSKey string, nodesIPs []string) ([]byte, error) {
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

	_, err = ca.SignCertificate(pathToNewTLSCert, "tls", nil, nodesIPs, GetPublicKey(privateKey), x509.KeyUsageCertSign|x509.KeyUsageCRLSign, []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	})
	if err != nil {
		return nil, err
	}

	err = utils.WritePEMToFile(pathToNewTLSKey, "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return nil, err
	}

	newCertBytes, err := os.ReadFile(filepath.Join(pathToNewTLSCert, "tls-cert.pem"))
	if err != nil {
		return nil, err
	}

	return newCertBytes, nil
}

func GetPublicKey(priv crypto.PrivateKey) crypto.PublicKey {
	switch kk := priv.(type) {
	case *ecdsa.PrivateKey:
		return &(kk.PublicKey)
	case ed25519.PrivateKey:
		return kk.Public()
	default:
		panic("unsupported key algorithm")
	}
}
