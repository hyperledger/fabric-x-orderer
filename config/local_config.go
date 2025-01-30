package config

import (
	"fmt"

	"arma/common/types"
	"arma/common/utils"
	"arma/node/comm"
)

// NodeLocalConfig controls the configuration of an Arma node.
// The relevant information will be filled corresponding to the specific node type.
// Every time a node starts, it is expected to load this file.
type NodeLocalConfig struct {
	// PartyID is the party id to which the node belongs
	PartyID types.PartyID `yaml:"PartyID,omitempty"`
	// GeneralConfig configures the address, settings for gRPC, TLS config and crypto material of the node
	GeneralConfig *GeneralConfig `yaml:"General,omitempty"`
	// FileStore controls the configuration of the file store where blocks and databases are stored
	FileStore *FileStore `yaml:"FileStore,omitempty"`
	// RouterParams controls Router specific params. For Bathcer, Consensus or Assembler nodes this field is expected to be empty
	RouterParams *RouterParams `yaml:"Router,omitempty"`
	// BatcherParams controls Batcher specific params. For Router, Consensus or Assembler nodes this field is expected to be empty
	BatcherParams *BatcherParams `yaml:"Batcher,omitempty"`
	// ConsensusParams controls Consensus specific params. For Router, Batcher or Assembler nodes this field is expected to be empty
	ConsensusParams *ConsensusParams `yaml:"Consensus,omitempty"`
	// AssemblerParams controls Assembler specific params. For Router, Batcher or Consensus nodes this field is expected to be empty
	AssemblerParams *AssemblerParams `yaml:"Assembler,omitempty"`
}

type GeneralConfig struct {
	// ListenAddress is the IP on which to bind to listen
	ListenAddress string `yaml:"ListenAddress,omitempty"`
	// ListenPort is the port on which to bind to listen
	ListenPort int `yaml:"ListenPort,omitempty"`
	// TLSConfig is the TLS settings for the GRPC server
	TLSConfig TLSConfig `yaml:"TLS,omitempty"`
	// Keepalive is the Keepalive settings for the GRPC server.
	KeepaliveSettings comm.KeepaliveOptions `yaml:"Keepalive,omitempty"`
	// BackoffSettings is the configuration options for backoff GRPC client.
	BackoffSettings comm.BackoffOptions `yaml:"Backoff,omitempty"`
	// Max message size in bytes the GRPC server and client can receive
	MaxRecvMsgSize int32 `yaml:"MaxRecvMsgSize,omitempty"`
	// Max message size in bytes the GRPC server and client can send
	MaxSendMsgSize int32 `yaml:"MaxSendMsgSize,omitempty"`
	// Bootstrap indicates the method by which to obtain the bootstrap configuration and the file containing the bootstrap configuration
	Bootstrap Bootstrap `yaml:"Bootstrap,omitempty"`
	// Cluster settings for consenter nodes
	Cluster Cluster `yaml:"Cluster,omitempty"`
	// The path to the private crypto material needed by Arma
	LocalMSPDir string `yaml:"LocalMSPDir,omitempty"`
	// LocalMSPID is the identity to register the local MSP material with the MSP manager
	LocalMSPID string `yaml:"LocalMSPID,omitempty"`
	// BCCSP configures the blockchain crypto service providers
	BCCSP BCCSP `yaml:"BCCSP,omitempty"`
	// LogSpec controls the logging level of the node
	LogSpec string `yaml:"LogSpec,omitempty"`
}

type TLSConfig struct {
	Enabled            bool     `yaml:"Enabled,omitempty"`            // Require server-side TLS
	PrivateKey         string   `yaml:"PrivateKey,omitempty"`         // The file location of the private key of the server TLS certificate.
	Certificate        string   `yaml:"Certificate,omitempty"`        // The file location of the server TLS certificate.
	RootCAs            []string `yaml:"RootCAs,omitempty"`            // A list of additional file locations for root certificates used for verifying certificates of other nodes during outbound connections.
	ClientAuthRequired bool     `yaml:"ClientAuthRequired,omitempty"` // Require client certificates / mutual TLS for inbound connections
	ClientRootCAs      []string `yaml:"ClientRootCAs,omitempty"`      // A list of additional file location for root certificates used for verifying certificates of client connections. relevant for Assembler and Consensus
}

// Bootstrap configures how to obtain the bootstrap configuration.
type Bootstrap struct {
	// Method specifies the method by which to obtain the bootstrap configuration.
	// The option can be one of:
	//  1. "block" - path to a file containing the genesis block or config block
	//  2. "yaml" - path to a file containing a YAML boostrap configuration (i.e. ./shared_config.yaml).
	Method string `yaml:"Method,omitempty"`
	//  File is the path for the bootstrap configuration.
	//  The bootstrap file can be the genesis block, and it can also be a config block for late bootstrap.
	File string `yaml:"File,omitempty"`
}

// Cluster defines the settings for ordering service nodes that communicate with other ordering service nodes.
type Cluster struct {
	// SendBufferSize is the maximum number of messages in the egress buffer.
	// Consensus messages are dropped if the buffer is full, and transaction messages are waiting for space to be freed.
	SendBufferSize int `yaml:"SendBufferSize,omitempty"`
	// ClientCertificate governs the file location of the client TLS certificate used to establish mutual TLS connections with other ordering service nodes.
	// If not set, the server General.TLS.Certificate is re-used.
	ClientCertificate string `yaml:"ClientCertificate,omitempty"`
	// ClientPrivateKey governs the file location of the private key of the client TLS certificate.
	// If not set, the server General.TLS.PrivateKey is re-used.
	ClientPrivateKey string `yaml:"ClientPrivateKey,omitempty"`
	// ReplicationPolicy defines how blocks are replicated between orderers.
	ReplicationPolicy string `yaml:"ReplicationPolicy,omitempty"`
}

// BCCSP configures the blockchain crypto service providers.
type BCCSP struct {
	// Default specifies the preferred blockchain crypto service provider to use.
	// If the preferred provider is not available, the software based provider ("SW") will be used.
	// Valid providers are:
	// - SW: a software based crypto provider
	// - PKCS11: a CA hardware security module crypto provider.
	Default string `yaml:"Default,omitempty"`
	// SW configures the software based blockchain crypto provider.
	SW *SwOpts `yaml:"SW,omitempty"`
	// PKCS11 is the settings for the PKCS#11 crypto provider (i.e. when DEFAULT: PKCS11)
	PKCS11 *PKCS11 `yaml:"PKCS11,omitempty"`
}

type SwOpts struct {
	Security     int               `json:"security" yaml:"Security"`
	Hash         string            `json:"hash" yaml:"Hash"`
	FileKeystore *FileKeystoreOpts `json:"filekeystore,omitempty" yaml:"FileKeyStore,omitempty"`
}

type FileKeystoreOpts struct {
	KeyStorePath string `yaml:"KeyStore"`
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

// FileStore sets the configuration of the file store.
type FileStore struct {
	// Path is the directory to store data in, e.g. the blocks and databases.
	Path string `yaml:"Location,omitempty"`
}

type RouterParams struct {
	// NumberOfConnectionsPerBatcher specifies the number of connections between Router and Batcher
	NumberOfConnectionsPerBatcher int `yaml:"NumberOfConnectionsPerBatcher,omitempty"`
	// NumberOfStreamsPerConnection specifies the number of streams per connection that are opened between Router and Batcher
	NumberOfStreamsPerConnection int `yaml:"NumberOfStreamsPerConnection,omitempty"`
}

type ConsensusParams struct {
	//  WALDir specifies the location at which Write Ahead Logs for SmartBFT are stored
	WALDir string `yaml:"WALDir,omitempty"`
}

type BatcherParams struct {
	// ShardID specifies the shard id to which the Batcher is associated
	ShardID types.ShardID `yaml:"ShardID,omitempty"`
}

type AssemblerParams struct {
	// PrefetchBufferMemoryMB is the size of the buffer that is used to store prefetched batches from the Batchers
	PrefetchBufferMemoryMB int `yaml:"PrefetchBufferMemoryMB,omitempty"`
}

func Load(filePath string) (*NodeLocalConfig, error) {
	if filePath == "" {
		return nil, fmt.Errorf("cannot load local node configuration, path: %s is empty", filePath)
	}
	nodeLocalConfig := NodeLocalConfig{}
	err := utils.ReadFromYAML(&nodeLocalConfig, filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot load local node configuration, failed reading config yaml, err: %s", err)
	}

	return &nodeLocalConfig, nil
}
