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
	"os"
	"path/filepath"
	"strconv"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/arma/config"
	"github.ibm.com/decentralized-trust-research/fabricx-config/internaltools/configtxgen"
	"github.ibm.com/decentralized-trust-research/fabricx-config/internaltools/configtxgen/genesisconfig"
)

// CreateGenesisBlock creates a config block and writes it to a file under dir/bootstrap.block
// This function is used for testing only.
func CreateGenesisBlock(dir string, sharedConfigYaml *config.SharedConfigYaml, sharedConfigPath string, sampleConfigPath string) (*common.Block, error) {
	// Generate Profile
	profile, err := CreateProfile(dir, sharedConfigYaml, sharedConfigPath, sampleConfigPath)
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
	blockPath := filepath.Join(dir, "bootstrap.block")
	err = configtxgen.WriteOutputBlock(genesisBlock, blockPath)
	if err != nil {
		return nil, err
	}

	return genesisBlock, nil
}

func CreateProfile(dir string, sharedConfigYaml *config.SharedConfigYaml, sharedConfigPath string, sampleConfigPath string) (*genesisconfig.Profile, error) {
	// collect relevant shared configuration
	var consenterMapping []*genesisconfig.Consenter
	var routerAndAssemblerEndpoints []string
	for _, party := range sharedConfigYaml.PartiesConfig {
		routerAndAssemblerEndpoints = append(routerAndAssemblerEndpoints, party.RouterConfig.Host+":"+strconv.Itoa(int(party.RouterConfig.Port)))
		routerAndAssemblerEndpoints = append(routerAndAssemblerEndpoints, party.AssemblerConfig.Host+":"+strconv.Itoa(int(party.AssemblerConfig.Port)))

		consenter := &genesisconfig.Consenter{
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
	profile := genesisconfig.Load(genesisconfig.SampleFabricX, sampleConfigPath)

	// update profile with some more relevant orderer information
	profile.Orderer.Arma.LoadFromPath = sharedConfigPath

	for _, org := range profile.Orderer.Organizations {
		org.OrdererEndpoints = routerAndAssemblerEndpoints
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
