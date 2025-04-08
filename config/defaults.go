package config

import (
	"path/filepath"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

var DefaultRouterParams = RouterParams{
	NumberOfConnectionsPerBatcher: 10,
	NumberOfStreamsPerConnection:  5,
}

var DefaultConsenterNodeConfigParams = func(dir string) *ConsensusParams {
	return &ConsensusParams{WALDir: filepath.Join(dir, "wal")}
}

var DefaultArmaBFTConfig = func() smartbft_types.Configuration {
	config := smartbft_types.DefaultConfig

	config.RequestBatchMaxInterval = time.Millisecond * 500
	config.RequestForwardTimeout = time.Second * 10
	config.DecisionsPerLeader = 0
	config.LeaderRotation = false

	return config
}
