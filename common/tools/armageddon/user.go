/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"fmt"
	"path/filepath"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
)

type RouterUserConfig struct {
	Endpoint   string        `yaml:"Endpoint,omitempty"`
	PartyID    types.PartyID `yaml:"PartyID,omitempty"`
	UserMSPDir string        `yaml:"UserMSPDir,omitempty"`
}

// UserConfig holds the user information needed for connection to routers and assemblers
// Note: a user will be created for each party. One of the users will be chosen as a grpc client that sends tx to all router and receives blocks from the assemblers.
type UserConfig struct {
	TLSPrivateKey      []byte             `yaml:"TLSPrivateKey,omitempty"`
	TLSCertificate     []byte             `yaml:"TLSCertificate,omitempty"`
	RouterUserConfigs  []RouterUserConfig `yaml:"RouterUserConfigs,omitempty"`
	AssemblerEndpoints []string           `yaml:"AssemblerEndpoints,omitempty"`
	TLSCACerts         [][]byte           `yaml:"TLSCACerts,omitempty"`
	UseTLSRouter       string             `yaml:"UseTLSRouter,omitempty"`
	UseTLSAssembler    string             `yaml:"UseTLSAssembler,omitempty"`
}

func NewUserConfig(outputDir string, privateKeyPath string, tlsCertPath string, tlsCACerts [][]byte, network *genconfig.Network) (*UserConfig, error) {
	// collect router and assembler endpoints, required for defining a user
	var routerUserConfigs []RouterUserConfig
	var assemblerEndpoints []string
	for _, party := range network.Parties {
		routerUserConfigs = append(routerUserConfigs, RouterUserConfig{
			Endpoint:   party.RouterEndpoint,
			PartyID:    party.ID,
			UserMSPDir: filepath.Join(outputDir, fmt.Sprintf("crypto/ordererOrganizations/org%d/users/user/msp", party.ID)),
		})
		assemblerEndpoints = append(assemblerEndpoints, party.AssemblerEndpoint)
	}

	privateKey, err := utils.ReadPem(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed load private key: %s", err)
	}

	tlsCert, err := utils.ReadPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed load TLS certificate: %s", err)
	}

	return &UserConfig{
		TLSPrivateKey:      privateKey,
		TLSCertificate:     tlsCert,
		RouterUserConfigs:  routerUserConfigs,
		AssemblerEndpoints: assemblerEndpoints,
		TLSCACerts:         tlsCACerts,
		UseTLSRouter:       network.UseTLSRouter,
		UseTLSAssembler:    network.UseTLSAssembler,
	}, nil
}
