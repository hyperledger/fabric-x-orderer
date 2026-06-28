/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

//go:generate counterfeiter -o mocks/assembler_support.go . AssemblerSupport
type AssemblerSupport interface {
	identity.SignerSerializer
	Height() uint64
	SharedConfig() channelconfig.Orderer
	ChannelID() string
	// Sequence() uint64
	Block(number uint64) *cb.Block
	LastConfigBlock(block *cb.Block) (*cb.Block, error)
	WriteBlockSync(block *cb.Block)
	WriteConfigBlock(block *cb.Block)
	ClientConfig() comm.ClientConfig
}

type BFTConfigGetter interface {
	BFTConfig() (types.Configuration, []uint64)
}

//go:generate counterfeiter -o mocks/synchronizer_with_stop.go . SynchronizerWithStop
type SynchronizerWithStop interface {
	Sync() error
	Stop()
}

//go:generate counterfeiter -o mocks/synchronizer_factory.go . SynchronizerFactory
type SynchronizerFactory interface {
	// CreateSynchronizer creates a new Assembler Synchronizer.
	CreateSynchronizer(
		logger *flogging.FabricLogger,
		selfID uint64,
		localConfigCluster config.Cluster,
		support AssemblerSupport,
		bccsp bccsp.BCCSP,
		targetHeight uint64,
		bootConfigBlock *cb.Block,
	) SynchronizerWithStop
}

type SynchronizerCreator struct{}

func (*SynchronizerCreator) CreateSynchronizer(
	logger *flogging.FabricLogger,
	selfID uint64,
	localConfigCluster config.Cluster,
	support AssemblerSupport,
	bccsp bccsp.BCCSP,
	targetHeight uint64,
	bootConfigBlock *cb.Block,
) SynchronizerWithStop {
	return newSynchronizer(logger, selfID, localConfigCluster, support, bccsp, targetHeight, bootConfigBlock)
}

// newSynchronizer creates a new synchronizer
func newSynchronizer(
	logger *flogging.FabricLogger,
	selfID uint64,
	localConfigCluster config.Cluster,
	support AssemblerSupport,
	bccsp bccsp.BCCSP,
	targetHeight uint64,
	bootConfigBlock *cb.Block,
) SynchronizerWithStop {
	switch localConfigCluster.ReplicationPolicy {
	case "assemblerSync":
		logger.Debug("Creating an assembler BFT Synchronizer for replication policy 'assemblerSync'")
		return &AssemblerBFTSynchronizer{
			SelfPartyID:         selfID,
			TargetHeight:        targetHeight,
			Support:             support,
			CryptoProvider:      bccsp,
			ClusterDialer:       &comm.PredicateDialer{Config: support.ClientConfig()},
			LocalConfigCluster:  localConfigCluster,
			BlockPullerFactory:  &GenesisFetcherCreator{},
			VerifierFactory:     &noopVerifierCreator{}, // TODO replace with real verifier
			BFTDelivererFactory: &bftDelivererCreator{},
			Logger:              logger,
			JoinConfigBlock:     bootConfigBlock,
		}

	default:
		logger.Panicf("Unsupported Cluster.ReplicationPolicy: %s", localConfigCluster.ReplicationPolicy)
		return nil
	}
}
