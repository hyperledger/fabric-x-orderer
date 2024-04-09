package node

import (
	"encoding/base64"
	"gopkg.in/yaml.v3"
)

type RawBytes []byte

func (bytes RawBytes) MarshalYAML() (interface{}, error) {
	return base64.StdEncoding.EncodeToString(bytes), nil
}

func (bytes RawBytes) UnmarshalYAML(node *yaml.Node) error {
	value := node.Value
	ba, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return err
	}
	bytes = ba
	return nil
}

type BatcherInfo struct {
	PartyID    uint16
	Endpoint   string
	TLSCACerts []RawBytes
	PublicKey  RawBytes
}

type ShardInfo struct {
	ShardId  uint16
	Batchers []BatcherInfo
}

type ConsenterInfo struct {
	PartyId   uint16
	Endpoint  string
	PublicKey RawBytes
	TlsCACert []RawBytes
}

type Party struct {
	PartyId   string
	PublicKey RawBytes
}

type RouterNodeConfig struct {
	Shards                        []ShardInfo
	NumOfConnectionsForBatcher    int
	NumOfgRPCStreamsPerConnection int
}

type AssemblerNodeConfig struct {
	// Private config
	PartyId uint16
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
	TlsPrivateKeyFile  RawBytes
	TlsCertificateFile RawBytes
	SigningPrivateKey  RawBytes
}

type ConsenterNodeConfig struct {
	// Shared config
	Shards     []ShardInfo
	Consenters []ConsenterInfo
	// Private config
	PartyId            uint16
	TlsPrivateKeyFile  RawBytes
	TlsCertificateFile RawBytes
	SigningPrivateKey  RawBytes
}
