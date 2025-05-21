/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

// ConsensusConfig includes the consensus configuration,
type ConsensusConfig struct {
	BFTConfig smartbft_types.Configuration `yaml:"SmartBFT,omitempty"`
}
