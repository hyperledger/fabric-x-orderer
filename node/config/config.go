package config

import (
	"encoding/base64"
	"os"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"

	"gopkg.in/yaml.v3"
)

type RawBytes []byte

func (bytes RawBytes) MarshalYAML() (interface{}, error) {
	return base64.StdEncoding.EncodeToString(bytes), nil
}

func (bytes *RawBytes) UnmarshalYAML(node *yaml.Node) error {
	value := node.Value
	ba, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return err
	}
	*bytes = ba
	return nil
}

type BatcherInfo struct {
	PartyID    types.PartyID
	Endpoint   string
	TLSCACerts []RawBytes
	PublicKey  RawBytes
	TLSCert    RawBytes
}

type ShardInfo struct {
	ShardId  types.ShardID
	Batchers []BatcherInfo
}

type Network struct {
	Parties []Party
}

type Party struct {
	ID        types.PartyID
	Assembler string
	Consenter string
	Router    string
	Batchers  []string
}

type ConsenterInfo struct {
	PartyID    types.PartyID
	Endpoint   string
	PublicKey  RawBytes
	TLSCACerts []RawBytes
}

type RouterNodeConfig struct {
	// Private config
	PartyID            types.PartyID
	TLSCertificateFile RawBytes
	TLSPrivateKeyFile  RawBytes
	ListenAddress      string
	// Shared config
	Shards                        []ShardInfo
	NumOfConnectionsForBatcher    int
	NumOfgRPCStreamsPerConnection int
	UseTLS                        bool
	ClientAuthRequired            bool
}

type AssemblerNodeConfig struct {
	// Private config
	TLSPrivateKeyFile  RawBytes
	TLSCertificateFile RawBytes
	PartyId            types.PartyID
	Directory          string
	ListenAddress      string
	// Shared config
	Shards             []ShardInfo
	Consenter          ConsenterInfo
	UseTLS             bool
	ClientAuthRequired bool
}

type BatcherNodeConfig struct {
	// Shared config
	Shards        []ShardInfo
	Consenters    []ConsenterInfo
	Directory     string
	ListenAddress string
	// Private config
	PartyId            types.PartyID
	ShardId            types.ShardID
	TLSPrivateKeyFile  RawBytes
	TLSCertificateFile RawBytes
	SigningPrivateKey  RawBytes
	MemPoolMaxSize     uint64
	BatchMaxSize       uint32
	BatchMaxBytes      uint32
	RequestMaxBytes    uint64
	BatchTimeout       time.Duration
}

type ConsenterNodeConfig struct {
	// Shared config
	Shards        []ShardInfo
	Consenters    []ConsenterInfo
	Directory     string
	ListenAddress string
	// Private config
	PartyId            types.PartyID
	TLSPrivateKeyFile  RawBytes
	TLSCertificateFile RawBytes
	SigningPrivateKey  RawBytes
	BatchTimeout       time.Duration
}

func NodeConfigToYAML(config interface{}, path string) error {
	rnc, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, rnc, 0o644)
	if err != nil {
		return err
	}

	return nil
}

func NodeConfigFromYAML(config interface{}, path string) error {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		return err
	}
	return nil
}
