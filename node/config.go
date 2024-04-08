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

type BatcherConfig struct {
	ShardId    uint16
	Endpoint   string
	TLSCACerts []RawBytes
}

type ConsenterConfigInfo struct {
	Endpoint string
	TlsCert  []RawBytes
}

type ConsenterConfig struct {
	ConsenterConfigInfo ConsenterConfigInfo
	PartyId             uint16
	PublicKey           RawBytes
}

type Party struct {
	PartyId   string
	PublicKey RawBytes
}

type ShardIdAndParties struct {
	ShardId uint16
	Parties []Party
}

type RouterNodeConfig struct {
	Batchers                      []BatcherConfig
	NumOfConnectionsForBatcher    int
	NumOfgRPCStreamsPerConnection int
}

type AssemblerNodeConfig struct {
	PartyId   uint16
	Batchers  []BatcherConfig
	Consenter ConsenterConfigInfo
}

type BatcherNodeConfig struct {
	PartyId            uint16
	Batchers           []BatcherConfig
	Consenters         []ConsenterConfigInfo
	TlsPrivateKeyFile  RawBytes
	TlsCertificateFile RawBytes
	SigningPrivateKey  RawBytes
}

type ConsenterNodeConfig struct {
	PartyId            uint16
	Batchers           []ShardIdAndParties
	Consenters         []ConsenterConfig
	TlsPrivateKeyFile  RawBytes
	TlsCertificateFile RawBytes
	SigningPrivateKey  RawBytes
}
