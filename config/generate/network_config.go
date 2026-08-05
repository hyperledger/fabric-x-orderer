/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate

import (
	"gopkg.in/yaml.v3"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
)

// Network describes an Arma network deployment by collecting all endpoints of nodes per party.
// These endpoints are required to the generation of the local and the shared config.
// UseTLSRouter and UseTLSAssembler indicate whether the connection between a client to a router and an assembler is a none|TLS|mTLS.
// This is typically used in a test environment.
type Network struct {
	Parties         []Party       `yaml:"Parties"`
	UseTLSRouter    string        `yaml:"UseTLSRouter"`
	UseTLSAssembler string        `yaml:"UseTLSAssembler"`
	MaxPartyID      types.PartyID `yaml:"MaxPartyID"`
	Peers           []string      `yaml:"Peers"`
}

// UnmarshalYAML defaults Peers to ["peer1"] when the field is absent or empty in the YAML source.
// It is called implicitly by the gopkg.in/yaml.v3 library whenever a *Network is the target of
// yaml.Unmarshal or decoder.Decode.
func (n *Network) UnmarshalYAML(value *yaml.Node) error {
	type networkAlias Network // alias avoids infinite recursion
	alias := &networkAlias{Peers: []string{"peer1"}}
	if err := value.Decode(alias); err != nil {
		return err
	}
	if len(alias.Peers) == 0 {
		alias.Peers = []string{"peer1"}
	}
	*n = Network(*alias)
	return nil
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
	ID                   types.PartyID
	RouterLocalConfig    *config.NodeLocalConfig
	BatchersLocalConfig  []*config.NodeLocalConfig
	ConsenterLocalConfig *config.NodeLocalConfig
	AssemblerLocalConfig *config.NodeLocalConfig
}
