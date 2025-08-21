/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"path/filepath"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

var DefaultRouterParams = RouterParams{
	NumberOfConnectionsPerBatcher:       10,
	NumberOfStreamsPerConnection:        5,
	ClientSignatureVerificationRequired: false,
}

var DefaultBatcherParams = BatcherParams{
	BatchSequenceGap: 10,
	MemPoolMaxSize:   1000 * 1000,
	SubmitTimeout:    time.Millisecond * 500,
}

var DefaultConsenterNodeConfigParams = func(dir string) *ConsensusParams {
	return &ConsensusParams{WALDir: filepath.Join(dir, "wal")}
}

var DefaultAssemblerParams = AssemblerParams{
	PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024,
	RestartLedgerScanTimeout:  5 * time.Second,
	PrefetchEvictionTtl:       time.Hour,
	PopWaitMonitorTimeout:     time.Second,
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
	BatchTimeouts: BatchTimeouts{
		BatchCreationTimeout:  time.Millisecond * 500,
		FirstStrikeThreshold:  10 * time.Second,
		SecondStrikeThreshold: 10 * time.Second,
		AutoRemoveTimeout:     10 * time.Second,
	},
	BatchSize: BatchSize{
		MaxMessageCount:   1000 * 10,
		AbsoluteMaxBytes:  1024 * 1024 * 10,
		PreferredMaxBytes: 0,
	},
	RequestMaxBytes: 1024 * 1024,
}
