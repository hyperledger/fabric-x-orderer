package node

import (
	"encoding/base64"
	"gopkg.in/yaml.v3"
	"os"
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
	PartyID    uint16
	Endpoint   string
	TLSCACerts []RawBytes
	PublicKey  RawBytes
	TLSCert    RawBytes
}

type ShardInfo struct {
	ShardId  uint16
	Batchers []BatcherInfo
}

type ConsenterInfo struct {
	PartyId    uint16
	Endpoint   string
	PublicKey  RawBytes
	TLSCACerts []RawBytes
}

type RouterNodeConfig struct {
	Shards                        []ShardInfo
	NumOfConnectionsForBatcher    int
	NumOfgRPCStreamsPerConnection int
}

type AssemblerNodeConfig struct {
	// Private config
	TLSPrivateKeyFile  RawBytes
	TLSCertificateFile RawBytes
	PartyId            uint16
	// Shared config
	Shards    []ShardInfo
	Consenter ConsenterInfo
}

type BatcherNodeConfig struct {
	// Shared config
	Shards     []ShardInfo
	Consenters []ConsenterInfo
	// Private config
	PartyId            uint16
	ShardId            uint16
	TLSPrivateKeyFile  RawBytes
	TLSCertificateFile RawBytes
	SigningPrivateKey  RawBytes
}

type ConsenterNodeConfig struct {
	// Shared config
	Shards     []ShardInfo
	Consenters []ConsenterInfo
	// Private config
	PartyId            uint16
	TLSPrivateKeyFile  RawBytes
	TLSCertificateFile RawBytes
	SigningPrivateKey  RawBytes
}

func NodeConfigToYAML(config interface{}, path string) error {
	rnc, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, rnc, 0644)
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
