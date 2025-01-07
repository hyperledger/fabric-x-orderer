package config

import (
	"os"

	"arma/common/types"
	"arma/node/comm"

	"gopkg.in/yaml.v3"
)

type NodeLocalConfig struct {
	PartyID         types.PartyID
	GeneralConfig   *GeneralConfig
	FileStore       *FileStore
	RouterParams    RouterParams
	BatcherParams   BatcherParams
	ConsensusParams ConsensusParams
	AssemblerParams AssemblerParams
}

type GeneralConfig struct {
	// ListenAddress is the IP on which to bind to listen
	ListenAddress string
	// ListenPort is the port on which to bind to listen
	ListenPort string
	// TLSConfig is the TLS settings for the GRPC server
	TLSConfig TLSConfig
	// Keepalive is the Keepalive settings for the GRPC server.
	KeepaliveSettings comm.KeepaliveOptions
	// BackoffSettings is the configuration options for backoff GRPC client.
	BackoffSettings comm.BackoffOptions
	// Max message size in bytes the GRPC server and client can receive
	MaxRecvMsgSize int32
	// Max message size in bytes the GRPC server and client can send
	MaxSendMsgSize int32
	// Bootstrap indicates the method by which to obtain the bootstrap configuration and the file containing the bootstrap configuration
	Bootstrap Bootstrap
	// Cluster settings for consenter nodes
	Cluster Cluster
	// The path to the private crypto material needed by Arma
	LocalMSPDir string
	// LocalMSPID is the identity to register the local MSP material with the MSP manager
	LocalMSPID string
	// BCCSP configures the blockchain crypto service providers
	BCCSP BCCSP
	// LogSpec controls the logging level of the node
	LogSpec string
}

type TLSConfig struct {
	Enabled            bool     // Require server-side TLS
	PrivateKey         string   // The file location of the private key of the server TLS certificate.
	Certificate        string   // The file location of the server TLS certificate.
	RootCAs            []string // A list of additional file locations for root certificates used for verifying certificates of other nodes during outbound connections.
	ClientAuthRequired bool     // Require client certificates / mutual TLS for inbound connections
	ClientRootCAs      []string // A list of additional file location for root certificates used for verifying certificates of client connections. relevant for assembler and consensus
}

type Bootstrap struct {
	Method string
	File   string
}

type Cluster struct {
	SendBufferSize    int
	ClientCertificate string
	ClientPrivateKey  string
	ReplicationPolicy string
}

type BCCSP struct {
	Default string            `yaml:"Default,omitempty"`
	SW      *SoftwareProvider `yaml:"SW,omitempty"`
	PKCS11  *PKCS11           `yaml:"PKCS11,omitempty"`
}

type SoftwareProvider struct {
	Hash     string `yaml:"Hash,omitempty"`
	Security int    `yaml:"Security,omitempty"`
}

type PKCS11 struct {
	Hash     string `yaml:"Hash,omitempty"`
	Security int    `yaml:"Security,omitempty"`
	Pin      string `yaml:"Pin,omitempty"`
	Label    string `yaml:"Label,omitempty"`
	Library  string `yaml:"Library,omitempty"`

	AltID  string         `yaml:"AltID,omitempty"`
	KeyIDs []KeyIDMapping `yaml:"KeyIDs,omitempty"`
}

type KeyIDMapping struct {
	SKI string `yaml:"SKI,omitempty"`
	ID  string `yaml:"ID,omitempty"`
}

type FileStore struct {
	Path string
}

type RouterParams struct {
	NumberOfConnectionsPerBatcher int
	NumberOfStreamsPerConnection  int
}

type ConsensusParams struct {
	//  WALDir specifies the location at which Write Ahead Logs for SmartBFT are stored
	WALDir string
}

type BatcherParams struct {
	ShardID types.ShardID
}

type AssemblerParams struct {
	PrefetchBufferMemoryMB int
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
