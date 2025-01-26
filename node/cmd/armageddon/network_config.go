package armageddon

import (
	"arma/common/types"
	localconfig "arma/config"
)

// Network describes an Arma network deployment by collecting all endpoints of nodes per party.
// These endpoints are required to the generation of the local and the shared config.
// This is typically used in a test environment.
type Network struct {
	Parties []Party `yaml:"Parties"`
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
	RouterLocalConfig    *localconfig.NodeLocalConfig
	BatchersLocalConfig  []*localconfig.NodeLocalConfig
	ConsenterLocalConfig *localconfig.NodeLocalConfig
	AssemblerLocalConfig *localconfig.NodeLocalConfig
}
