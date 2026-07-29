/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cryptogen

import (
	"github.com/cockroachdb/errors"
	"go.yaml.in/yaml/v3"
)

// Config represents the orderer/peer organization to be generated.
type Config struct {
	OrdererOrgs []OrgSpec `yaml:"OrdererOrgs"`
	PeerOrgs    []OrgSpec `yaml:"PeerOrgs"`
	GenericOrgs []OrgSpec `yaml:"GenericOrgs"`
}

// OrgSpec represents the organization specification.
type OrgSpec struct {
	Name          string       `yaml:"Name"`
	Domain        string       `yaml:"Domain"`
	EnableNodeOUs bool         `yaml:"EnableNodeOUs"`
	CA            NodeSpec     `yaml:"CA"`
	Template      NodeTemplate `yaml:"Template"`
	Specs         []NodeSpec   `yaml:"Specs"`
	Users         UsersSpec    `yaml:"Users"`
}

// NodeSpec represents a certificate specification for a node.
type NodeSpec struct {
	Hostname           string   `yaml:"Hostname"`
	CommonName         string   `yaml:"CommonName"`
	Country            string   `yaml:"Country"`
	Province           string   `yaml:"Province"`
	Locality           string   `yaml:"Locality"`
	OrganizationalUnit string   `yaml:"OrganizationalUnit"`
	StreetAddress      string   `yaml:"StreetAddress"`
	PostalCode         string   `yaml:"PostalCode"`
	SANS               []string `yaml:"SANS"`
	PublicKeyAlgorithm string   `yaml:"PublicKeyAlgorithm"`
	Party              string   `yaml:"Party"`
}

// NodeTemplate represents a template to generate node(s).
type NodeTemplate struct {
	Count              int      `yaml:"Count"`
	Start              int      `yaml:"Start"`
	Hostname           string   `yaml:"Hostname"`
	SANS               []string `yaml:"SANS"`
	PublicKeyAlgorithm string   `yaml:"PublicKeyAlgorithm"`
}

// UsersSpec represents a user(s) specification.
type UsersSpec struct {
	Count              int        `yaml:"Count"`
	PublicKeyAlgorithm string     `yaml:"PublicKeyAlgorithm"`
	Specs              []UserSpec `yaml:"Specs"`
}

// UserSpec Contains User specifications needed to customize the crypto material generation.
type UserSpec struct {
	Name               string `yaml:"Name"`
	PublicKeyAlgorithm string `yaml:"PublicKeyAlgorithm"`
}

// ParseConfig parses config data from string.
func ParseConfig(configData string) (*Config, error) {
	config := &Config{}
	err := yaml.Unmarshal([]byte(configData), &config)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshalling YAML")
	}
	return config, nil
}
