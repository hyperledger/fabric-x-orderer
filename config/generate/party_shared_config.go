package generate

import "github.ibm.com/decentralized-trust-research/arma/common/types"

type PartyConfig struct {
	// the identity of the party, type unit16, id > 0
	PartyID types.PartyID `yaml:"PartyID,omitempty"`
	// the paths to the certificates of the certificate authorities who generates the party's signing key-pairs
	CACerts []string `yaml:"CACerts,omitempty"`
	// the paths to the certificates of the certificate authorities who generates the party's TLS key-pairs
	TLSCACerts []string `yaml:"TLSCACerts,omitempty"`
	// the shared configuration of the router
	RouterConfig RouterNodeConfig `yaml:"RouterConfig,omitempty"`
	// the shared configuration of the batchers
	BatchersConfig []BatcherNodeConfig `yaml:"BatchersConfig,omitempty"`
	// the shared configuration of the consenter
	ConsenterConfig ConsenterNodeConfig `yaml:"ConsenterConfig,omitempty"`
	// the shared configuration of the assembler
	AssemblerConfig AssemblerNodeConfig `yaml:"AssemblerConfig,omitempty"`
}

type RouterNodeConfig struct {
	// the path to the certificate used to authenticate with clients
	TLSCert string `yaml:"TLSCert,omitempty"`
	// the hostname or IP on which the gRPC server will listen
	Host string `yaml:"Host,omitempty"`
	// the port on which the gRPC server will listen
	Port uint32 `yaml:"Port,omitempty"`
}

type BatcherNodeConfig struct {
	// the ID of the shard to which the batcher is associated
	ShardID types.ShardID `yaml:"ShardID,omitempty"`
	// the path to the public key of the batcher used to authenticate signatures on BAS's
	PublicKey string `yaml:"PublicKey,omitempty"`
	// the path to the certificate used to authenticate with clients
	TLSCert string `yaml:"TLSCert,omitempty"`
	// the hostname or IP on which the gRPC server will listen
	Host string `yaml:"Host,omitempty"`
	// the port on which the gRPC server will listen
	Port uint32 `yaml:"Port,omitempty"`
}

type ConsenterNodeConfig struct {
	// the path to the public key of the consensus used to authenticate signatures on blocks
	PublicKey string `yaml:"PublicKey,omitempty"`
	// the path to the certificate used to authenticate with clients
	TLSCert string `yaml:"TLSCert,omitempty"`
	// the hostname or IP on which the gRPC server will listen
	Host string `yaml:"Host,omitempty"`
	// the port on which the gRPC server will listen
	Port uint32 `yaml:"Port,omitempty"`
}

type AssemblerNodeConfig struct {
	// the path to the certificate used to authenticate with clients
	TLSCert string `yaml:"TLSCert,omitempty"`
	// the hostname or IP on which the gRPC server will listen
	Host string `yaml:"Host,omitempty"`
	// the port on which the gRPC server will listen
	Port uint32 `yaml:"Port,omitempty"`
}
