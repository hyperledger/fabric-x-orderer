/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"encoding/base64"
	"os"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/types"
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
	ConfigStorePath    string
	// Shared config
	Shards                              []ShardInfo
	NumOfConnectionsForBatcher          int
	NumOfgRPCStreamsPerConnection       int
	UseTLS                              bool
	ClientAuthRequired                  bool
	RequestMaxBytes                     uint64
	ClientSignatureVerificationRequired bool
	// Bundle collects resources (e.g., policy manager, configTx validator, etc.) that are used by the router for validation of transactions
	Bundle channelconfig.Resources
}

type AssemblerNodeConfig struct {
	// Private config
	TLSPrivateKeyFile         RawBytes
	TLSCertificateFile        RawBytes
	PartyId                   types.PartyID
	Directory                 string
	ListenAddress             string
	PrefetchBufferMemoryBytes int
	RestartLedgerScanTimeout  time.Duration
	PrefetchEvictionTtl       time.Duration
	PopWaitMonitorTimeout     time.Duration
	ReplicationChannelSize    int
	BatchRequestsChannelSize  int
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
	PartyId                             types.PartyID
	ShardId                             types.ShardID
	TLSPrivateKeyFile                   RawBytes
	TLSCertificateFile                  RawBytes
	SigningPrivateKey                   RawBytes
	MemPoolMaxSize                      uint64
	BatchMaxSize                        uint32
	BatchMaxBytes                       uint32
	RequestMaxBytes                     uint64 // TODO how can this be uint64 when BatchMaxBytes is uint32?
	SubmitTimeout                       time.Duration
	FirstStrikeThreshold                time.Duration
	SecondStrikeThreshold               time.Duration
	AutoRemoveTimeout                   time.Duration
	BatchCreationTimeout                time.Duration
	BatchSequenceGap                    types.BatchSequence
	ClientSignatureVerificationRequired bool
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
	WALDir             string
	BFTConfig          smartbft_types.Configuration
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
