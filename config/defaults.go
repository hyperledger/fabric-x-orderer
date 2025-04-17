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

var DefaultBatcherParams = BatcherParams{
	BatchSequenceGap: 10,
}

var DefaultConsenterNodeConfigParams = func(dir string) *ConsensusParams {
	return &ConsensusParams{WALDir: filepath.Join(dir, "wal")}
}

var DefaultAssemblerParams = AssemblerParams{
	PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024,
	RestartLedgerScanTimeout:  5 * time.Second,
	PrefetchEvictionTtl:       time.Hour,
	ReplicationChannelSize:    100,
	BatchRequestsChannelSize:  1000,
}

var DefaultArmaBFTConfig = func() smartbft_types.Configuration {
	config := smartbft_types.DefaultConfig

	config.RequestBatchMaxInterval = time.Millisecond * 500
	config.RequestForwardTimeout = time.Second * 10
	config.DecisionsPerLeader = 0
	config.LeaderRotation = false

	return config
}

var DefaultBatchingConfig = BatchingConfig{
	// TODO: add batch size to batching config
	BatchTimeout: time.Millisecond * 500,
	BatchSize:    BatchSize{},
}
