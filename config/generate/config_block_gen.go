/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/pkg/errors"
)

// CreateGenesisBlock creates a config block and writes it to a file under dir/bootstrap.block
// This function is used for testing only.
func CreateGenesisBlock(blockDir string, baseDir string, sharedConfigYaml *config.SharedConfigYaml, sharedConfigPath string, sampleConfigPath string) (*common.Block, error) {
	// Generate Profile
	profile, err := CreateProfile(baseDir, sharedConfigYaml, sharedConfigPath, sampleConfigPath)
	if err != nil {
		return nil, err
	}

	channelID := "arma"

	// Create block from the profile
	genesisBlock, err := configtxgen.GetOutputBlock(profile, channelID)
	if err != nil {
		return nil, err
	}

	// write block
	blockPath := filepath.Join(blockDir, "bootstrap.block")
	err = configtxgen.WriteOutputBlock(genesisBlock, blockPath)
	if err != nil {
		return nil, err
	}

	return genesisBlock, nil
}

func CreateProfile(dir string, sharedConfigYaml *config.SharedConfigYaml, sharedConfigPath string, sampleConfigPath string) (*configtxgen.Profile, error) {
	// collect relevant shared configuration
	var consenterMapping []*configtxgen.Consenter

	for _, party := range sharedConfigYaml.PartiesConfig {
		consenter := &configtxgen.Consenter{
			ID:            uint32(party.PartyID),
			Host:          party.ConsenterConfig.Host,
			Port:          party.ConsenterConfig.Port,
			MSPID:         "",
			Identity:      party.ConsenterConfig.TLSCert,
			ClientTLSCert: party.ConsenterConfig.TLSCert,
			ServerTLSCert: party.ConsenterConfig.TLSCert,
		}
		consenterMapping = append(consenterMapping, consenter)
	}

	// load SampleFabricX profile
	profile := configtxgen.Load(configtxgen.SampleFabricX, sampleConfigPath)

	// update profile with some more relevant orderer information
	profile.Orderer.Arma.Path = sharedConfigPath

	for i, org := range profile.Application.Organizations {
		org.ID = fmt.Sprintf("org%d", i+1)
		org.Name = fmt.Sprintf("org%d", i+1)
		org.MSPDir = filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "msp")
		// Update policy rules to use correct org names
		for _, policy := range org.Policies {
			policy.Rule = strings.ReplaceAll(policy.Rule, "SampleOrg", fmt.Sprintf("org%d", i+1))
		}
	}

	for i, org := range profile.Orderer.Organizations {
		org.ID = fmt.Sprintf("org%d", i+1)
		org.Name = fmt.Sprintf("org%d", i+1)
		org.MSPDir = filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "msp")
		// TODO: org.OrdererEndpoints in the new format
		// Update policy rules to use correct org names
		for _, policy := range org.Policies {
			policy.Rule = strings.ReplaceAll(policy.Rule, "SampleOrg", fmt.Sprintf("org%d", i+1))
		}
	}

	profile.Orderer.ConsenterMapping = consenterMapping

	pubKeyPath := filepath.Join(dir, "metaNamespaceVerificationKeyPath.pem")
	_, err := generatePublicKey(pubKeyPath)
	if err != nil {
		return nil, errors.Errorf("err: %s, failed creating public key", err)
	}
	profile.Application.MetaNamespaceVerificationKeyPath = pubKeyPath

	return profile, nil
}

func generatePublicKey(path string) ([]byte, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, errors.Errorf("err: %s, failed creating private key", err)
	}

	publicKey := privateKey.PublicKey
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&publicKey)
	if err != nil {
		return nil, errors.Errorf("err: %s, failed marshaling public key", err)
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Bytes: publicKeyBytes, Type: "PUBLIC KEY",
	})

	err = os.WriteFile(path, publicKeyPEM, 0o644)
	if err != nil {
		return nil, errors.Errorf("err: %s, failed to write public key", err)
	}

	return publicKeyPEM, nil
}
