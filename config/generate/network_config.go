package generate

import (
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/config"
)

// Network describes an Arma network deployment by collecting all endpoints of nodes per party.
// These endpoints are required to the generation of the local and the shared config.
// UseTLSRouter and UseTLSAssembler indicate whether the connection between a client to a router and an assembler is a none|TLS|mTLS.
// This is typically used in a test environment.
type Network struct {
	Parties         []Party `yaml:"Parties"`
	UseTLSRouter    string  `yaml:"UseTLSRouter"`
	UseTLSAssembler string  `yaml:"UseTLSAssembler"`
}

type Party struct {
	ID                types.PartyID `yaml:"ID"`
	AssemblerEndpoint string        `yaml:"AssemblerEndpoint"`
	ConsenterEndpoint string        `yaml:"ConsenterEndpoint"`
	RouterEndpoint    string        `yaml:"RouterEndpoint"`
	BatchersEndpoints []string      `yaml:"BatchersEndpoints"`
}

// NetworkLocalConfig collects the local config of each node per party.
type NetworkLocalConfig struct {
	PartiesLocalConfig []PartyLocalConfig
}

type PartyLocalConfig struct {
	RouterLocalConfig    *config.NodeLocalConfig
	BatchersLocalConfig  []*config.NodeLocalConfig
	ConsenterLocalConfig *config.NodeLocalConfig
	AssemblerLocalConfig *config.NodeLocalConfig
}
