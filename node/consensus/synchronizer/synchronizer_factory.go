/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/pkg/identity"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

//go:generate counterfeiter -o mocks/consenter_support.go . ConsenterSupport

type ConsenterSupport interface {
	identity.SignerSerializer

	// SignatureVerifier returns a function which verifies a signature of a block.
	SignatureVerifier() protoutil.BlockVerifierFunc

	// Block returns a block with the given number,
	// or nil if such a block doesn't exist.
	Block(number uint64) *cb.Block

	// LastConfigBlock returns the config block before or at the given block,
	// or error if such a block cannot be retrieved.
	LastConfigBlock(block *cb.Block) (*cb.Block, error)

	// Height returns the number of blocks in the chain this channel is associated with.
	Height() uint64

	// ChannelID returns the channel ID this support is associated with.
	ChannelID() string

	// Sequence returns the current config sequence.
	Sequence() uint64

	// SharedConfig provides the shared config from the channel's current config block.
	SharedConfig() channelconfig.Orderer

	// WriteBlockSync commits a block to the ledger.
	WriteBlockSync(block *cb.Block, encodedMetadataValue []byte)

	// WriteConfigBlock commits a block to the ledger, and applies the config update inside.
	WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte)
}

type BFTConfigGetter interface {
	BFTConfig() (types.Configuration, []uint64)
}

type SynchronizerFactory interface {
	// CreateSynchronizer creates a new Synchronizer.
	CreateSynchronizer(
		logger *flogging.FabricLogger,
		selfID uint64,
		localConfigCluster config.Cluster,
		rtc BFTConfigGetter,
		blockToDecision func(block *cb.Block) *types.Decision,
		pruneCommittedRequests func(block *cb.Block),
		updateRuntimeConfig func(block *cb.Block) types.Reconfig,
		support ConsenterSupport,
		bccsp bccsp.BCCSP,
		clusterDialer *comm.PredicateDialer,
	) api.Synchronizer
}

type SynchronizerCreator struct{}

func (*SynchronizerCreator) CreateSynchronizer(
	logger *flogging.FabricLogger,
	selfID uint64,
	localConfigCluster config.Cluster,
	rtc BFTConfigGetter,
	blockToDecision func(block *cb.Block) *types.Decision,
	pruneCommittedRequests func(block *cb.Block),
	updateRuntimeConfig func(block *cb.Block) types.Reconfig,
	support ConsenterSupport,
	bccsp bccsp.BCCSP,
	clusterDialer *comm.PredicateDialer,
) api.Synchronizer {
	return newSynchronizer(logger, selfID, localConfigCluster, rtc, blockToDecision, pruneCommittedRequests, updateRuntimeConfig, support, bccsp, clusterDialer)
}

// newSynchronizer creates a new synchronizer
func newSynchronizer(
	logger *flogging.FabricLogger,
	selfID uint64,
	localConfigCluster config.Cluster,
	rtc BFTConfigGetter,
	blockToDecision func(block *cb.Block) *types.Decision,
	pruneCommittedRequests func(block *cb.Block),
	updateRuntimeConfig func(block *cb.Block) types.Reconfig,
	support ConsenterSupport,
	bccsp bccsp.BCCSP,
	clusterDialer *comm.PredicateDialer,
) api.Synchronizer {
	switch localConfigCluster.ReplicationPolicy {
	case "consensus":
		logger.Debug("Creating a BFTSynchronizer")
		return &BFTSynchronizer{
			selfID:          selfID,
			LatestConfig:    rtc.BFTConfig,
			BlockToDecision: blockToDecision,
			OnCommit: func(block *cb.Block) types.Reconfig {
				pruneCommittedRequests(block)
				return updateRuntimeConfig(block)
			},
			Support:             support,
			CryptoProvider:      bccsp,
			ClusterDialer:       clusterDialer,
			LocalConfigCluster:  localConfigCluster,
			BlockPullerFactory:  &BlockPullerCreator{},
			VerifierFactory:     &verifierCreator{},
			BFTDelivererFactory: &bftDelivererCreator{},
			Logger:              logger,
		}

	// TODO consider adding the "simple" CFT ReplicationPolicy

	default:
		logger.Panicf("Unsupported Cluster.ReplicationPolicy: %s", localConfigCluster.ReplicationPolicy)
		return nil
	}
}
