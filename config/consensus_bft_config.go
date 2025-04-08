package config

import (
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

// ConsensusConfig includes the consensus configuration,
type ConsensusConfig struct {
	BFTConfig smartbft_types.Configuration `yaml:"SmartBFT,omitempty"`
}
