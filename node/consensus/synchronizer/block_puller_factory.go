/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"github.com/hyperledger/fabric-lib-go/bccsp"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/block_puller.go . BlockPuller

// BlockPuller is used to pull blocks from other OSN
type BlockPuller interface {
	PullBlock(seq uint64) *cb.Block
	HeightsByEndpoints() (map[string]uint64, string, error)
	Close()
}

//go:generate counterfeiter -o mocks/block_puller_factory.go . BlockPullerFactory

type BlockPullerFactory interface {
	// CreateBlockPuller creates a new block puller.
	CreateBlockPuller(
		support ConsenterSupport,
		baseDialer *comm.PredicateDialer,
		clusterConfig config.Cluster,
		bccsp bccsp.BCCSP,
	) (BlockPuller, error)
}

type BlockPullerCreator struct{}

func (*BlockPullerCreator) CreateBlockPuller(
	support ConsenterSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
) (BlockPuller, error) {
	return newBlockPuller(support, baseDialer, clusterConfig, bccsp)
}

// newBlockPuller creates a new block puller
func newBlockPuller(
	support ConsenterSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
) (BlockPuller, error) {
	// TODO implement, see:
	// https://github.com/hyperledger/fabric/blob/13792d5c2d38f9c6d3b1d86692e7aa20359b572b/orderer/consensus/smartbft/block_puller_factory.go#L57

	// TODO prevent staticcheck flags
	_ = support
	_ = baseDialer
	_ = clusterConfig
	_ = bccsp

	return nil, errors.New("not implemented yet")
}
