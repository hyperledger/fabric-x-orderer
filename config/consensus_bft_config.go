package config

import (
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
)

// ConsensusConfig includes the consensus configuration,
type ConsensusConfig struct {
	BFTConfig types.Configuration `yaml:"SmartBFT,omitempty"`
}
