package armageddon

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"

	"arma/common/types"
	"arma/internal/cryptogen/ca"
)

// GenerateCryptoConfig provides all crypto material of Arma network, divided into parties and written to files in a folder structure.
// For each party for all nodes (i.e. Router, Batchers, Consenter and Assembler) it is required to provide TLS certificate and private key for secure communications between nodes using TLS.
// For Batchers and Asssembler is it also required to provide a signing certificate with a corresponding private key that are used to sign BAS's by Batchers and blocks by Assembler.
//
// Folder structure:
//
// dir
// └── crypto
//
//	└── ordererOrganizations
//	    └── org{partyID}
//	        ├── ca
//	        ├── tlsca
//	        ├── msp
//	        │   └── admincerts (ignored)
//	        ├── orderers
//	        │   └── party{partyID}
//	        │       ├── router
//	        │       ├── batcher1
//	        │       ├── batcher2
//	        │       ├── ...
//	        │       ├── batcher{shards}
//	        │       ├── consenter
//	        │       └── assembler
//	        └── users
func GenerateCryptoConfig(networkConfig *Network, outputDir string) error {
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

	return nil
}

// createNetworkCryptoMaterial creates the crypto files for each party's nodes and write them into files.
func createNetworkCryptoMaterial(dir string, network *Network) error {
	// create TLS CA and Sign CA for each party
	// NOTE: a party can have several CA's, meanwhile armageddon creates only one CA for each party.
	partiesTLSCAs, partiesSignCAs, err := createCAsPerParty(dir, network)
	if err != nil {
		return err
	}

	// create crypto material for each party's nodes and write the crypto into files
	for _, party := range network.Parties {
		// choose a TLS CA and a signing CA of the party
		listOfTLSCAs := partiesTLSCAs[party.ID]
		tlsCA := listOfTLSCAs[0]

		listOfSignCAs := partiesSignCAs[party.ID]
		signCA := listOfSignCAs[0]

		// create crypto material for each party's nodes
		// TLS crypto for router
		err = createTLSCertKeyPairForNode(tlsCA, dir, party.RouterEndpoint, "router", party.ID)
		if err != nil {
			return err
		}

		// TLS crypto for consenter
		err = createTLSCertKeyPairForNode(tlsCA, dir, party.ConsenterEndpoint, "consenter", party.ID)
		if err != nil {
			return err
		}

		// TLS crypto for assembler
		err = createTLSCertKeyPairForNode(tlsCA, dir, party.AssemblerEndpoint, "assembler", party.ID)
		if err != nil {
			return err
		}

		// TLS crypto for batchers
		for j, batcherEndpoint := range party.BatchersEndpoints {
			err = createTLSCertKeyPairForNode(tlsCA, dir, batcherEndpoint, fmt.Sprintf("batcher%d", j+1), party.ID)
			if err != nil {
				return err
			}
		}

		// signing crypto for batchers
		for j, batcherEndpoint := range party.BatchersEndpoints {
			err = createSignCertAndPrivateKeyForNode(signCA, dir, batcherEndpoint, fmt.Sprintf("batcher%d", j+1), party.ID)
			if err != nil {
				return err
			}
		}

		// signing crypto to consenter
		err = createSignCertAndPrivateKeyForNode(signCA, dir, party.ConsenterEndpoint, "consenter", party.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

// createCAsPerParty creates a TLS CA and a signing CA for each party and write the ca's certificates into files.
func createCAsPerParty(dir string, network *Network) (map[types.PartyID][]*ca.CA, map[types.PartyID][]*ca.CA, error) {
	partiesTLSCAs := make(map[types.PartyID][]*ca.CA)
	partiesSignCAs := make(map[types.PartyID][]*ca.CA)

	for i, party := range network.Parties {
		// create a TLS CA for the party
		tlsCA, err := ca.NewCA(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("Org%d", i+1), "tlsca"), "tlsCA", "tlsca", "US", "California", "San Francisco", "ARMA", "addr", "12345", "ecdsa")
		if err != nil {
			return nil, nil, fmt.Errorf("err: %s, failed creating a TLS CA for party %d", err, party.ID)
		}

		partiesTLSCAs[party.ID] = []*ca.CA{tlsCA}

		// create a Signing CA for the party
		signCA, err := ca.NewCA(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("Org%d", i+1), "ca"), "signCA", "ca", "US", "California", "San Francisco", "ARMA", "addr", "12345", "ecdsa")
		if err != nil {
			return nil, nil, fmt.Errorf("err: %s, failed creating a signing CA for party %d", err, party.ID)
		}

		partiesSignCAs[party.ID] = []*ca.CA{signCA}
	}

	return partiesTLSCAs, partiesSignCAs, nil
}

// createTLSCertKeyPairForNode creates a TLS cert,key pair signed by a corresponding CA for an Arma node and write them into files.
func createTLSCertKeyPairForNode(ca *ca.CA, dir string, endpoint string, role string, partyID types.PartyID) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for "+role+" node %s", err, endpoint)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for "+role+" node %s", err, endpoint)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role), "tls", nil, nil, getPublicKey(privateKey), x509.KeyUsageCertSign|x509.KeyUsageCRLSign, []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth,
		x509.ExtKeyUsageServerAuth,
	})
	err = writePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role, "key.pem"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// createSignCertAndPrivateKeyForNode creates a signed certificate with a corresponding private key used for signing and write them into files.
// Cert and private key for signing are used only by the batchers and the consenters.
func createSignCertAndPrivateKeyForNode(ca *ca.CA, dir string, endpoint string, role string, partyID types.PartyID) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating private key for "+role+" node %s", err, endpoint)
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("err: %s, failed marshaling private key for "+role+" node %s", err, endpoint)
	}

	ca.SignCertificate(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role), "signing", nil, nil, getPublicKey(privateKey), x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{})
	err = writePEMToFile(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), role, "signingPrivateKey.pem"), "PRIVATE KEY", privateKeyBytes)
	if err != nil {
		return err
	}
	return nil
}

// generateNetworkCryptoConfigFolderStructure creates folders where the crypto material is written to.
func generateNetworkCryptoConfigFolderStructure(dir string, network *Network) error {
	var folders []string

	rootDir := filepath.Join(dir, "crypto", "ordererOrganizations")
	folders = append(folders, rootDir)

	for i := 1; i <= len(network.Parties); i++ {
		folders = generateOrdererOrg(rootDir, folders, i, len(network.Parties[i-1].BatchersEndpoints))
	}
	for _, folder := range folders {
		err := os.MkdirAll(folder, 0o755)
		if err != nil {
			return err
		}
	}
	return nil
}

// generateOrdererOrg generates folders for an organization.
// An organization can include one or more parties.
// Organization that has one party have a crypto structure of:
func generateOrdererOrg(rootDir string, folders []string, partyID int, shards int) []string {
	orgDir := filepath.Join(rootDir, fmt.Sprintf("org%d", partyID))
	folders = append(folders, orgDir)
	folders = append(folders, filepath.Join(orgDir, "ca"))
	folders = append(folders, filepath.Join(orgDir, "tlsca"))
	mspDir := filepath.Join(orgDir, "msp")
	folders = append(folders, mspDir)
	orderersDir := filepath.Join(orgDir, "orderers")
	folders = append(folders, orderersDir)
	partyDir := filepath.Join(orderersDir, fmt.Sprintf("party%d", partyID))
	folders = append(folders, filepath.Join(partyDir, "router"))
	for j := 1; j <= shards; j++ {
		folders = append(folders, filepath.Join(partyDir, fmt.Sprintf("batcher%d", j)))
	}
	folders = append(folders, filepath.Join(partyDir, "consenter"))
	folders = append(folders, filepath.Join(partyDir, "assembler"))
	folders = append(folders, filepath.Join(orgDir, "users"))
	folders = append(folders, filepath.Join(mspDir, "admincerts")) // ignore this folder, create and use later
	return folders
}

func writePEMToFile(path string, pemType string, bytes []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("err: %s, failed creating %s to %s", err, pemType, path)
	}
	defer file.Close()

	err = pem.Encode(file, &pem.Block{Type: pemType, Bytes: bytes})
	if err != nil {
		return fmt.Errorf("err: %s, failed writing %s to %s", err, pemType, path)
	}

	return nil
}

func getPublicKey(priv crypto.PrivateKey) crypto.PublicKey {
	switch kk := priv.(type) {
	case *ecdsa.PrivateKey:
		return &(kk.PublicKey)
	case ed25519.PrivateKey:
		return kk.Public()
	default:
		panic("unsupported key algorithm")
	}
}
