/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	commonsync "github.com/hyperledger/fabric-x-orderer/common/synchronizer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

//go:generate counterfeiter -o mocks/height_detector.go . HeightDetector

// HeightDetector is used to detect the ledger height of other consenters.
// It is used by the synchronizer in order to establish a target height for pulling blocks.
type HeightDetector interface {
	HeightsByEndpoints() (map[string]uint64, string, error)
	GenesisByEndpoints() (map[string]*cb.Block, error)
	Close()
}

//go:generate counterfeiter -o mocks/height_detector_factory.go . HeightDetectorFactory

type HeightDetectorFactory interface {
	// CreateHeightDetector creates a new height detector.
	CreateHeightDetector(
		myPartyID types.PartyID,
		support ConsenterSupport,
		baseDialer *comm.PredicateDialer,
		clusterConfig config.Cluster,
		bccsp bccsp.BCCSP,
		logger *flogging.FabricLogger,
	) (HeightDetector, error)
}

type HeightDetectorCreator struct{}

func (*HeightDetectorCreator) CreateHeightDetector(
	myPartyID types.PartyID,
	support ConsenterSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (HeightDetector, error) {
	return commonsync.NewBlockPuller(myPartyID, support, baseDialer, clusterConfig, bccsp, logger, config.ExtractConsenterAddresses)
}
