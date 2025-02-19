package armageddon

import (
	"fmt"

	"arma/common/utils"
	genconfig "arma/config/generate"
)

// UserConfig holds the user information needed for connection to routers and assemblers
// Note: a user will be created for each party. One of the users will be chosen as a grpc client that sends tx to all router and receives blocks from the assemblers.
type UserConfig struct {
	TLSPrivateKey      []byte   `yaml:"TLSPrivateKey,omitempty"`
	TLSCertificate     []byte   `yaml:"TLSCertificate,omitempty"`
	RouterEndpoints    []string `yaml:"RouterEndpoints,omitempty"`
	AssemblerEndpoints []string `yaml:"AssemblerEndpoints,omitempty"`
	TLSCACerts         [][]byte `yaml:"TLSCACerts,omitempty"`
	UseTLSRouter       string   `yaml:"UseTLS,omitempty"`
	UseTLSAssembler    string   `yaml:"UseTLS,omitempty"`
}

func NewUserConfig(privateKeyPath string, tlsCertPath string, tlsCACerts [][]byte, network *genconfig.Network, useTLSRouter string, useTLSAssembler string) (*UserConfig, error) {
	// collect router and assembler endpoints, required for defining a user
	var routerEndpoints []string
	var assemblerEndpoints []string
	for _, party := range network.Parties {
		routerEndpoints = append(routerEndpoints, party.RouterEndpoint)
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
		RouterEndpoints:    routerEndpoints,
		AssemblerEndpoints: assemblerEndpoints,
		TLSCACerts:         tlsCACerts,
		UseTLSRouter:       useTLSRouter,
		UseTLSAssembler:    useTLSAssembler,
	}, nil
}
