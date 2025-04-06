package config

import (
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/fabricx-config/common/channelconfig"
	"github.ibm.com/decentralized-trust-research/fabricx-config/internaltools/configtxgen"
	"github.ibm.com/decentralized-trust-research/fabricx-config/protoutil"
)

// ReadSharedConfigFromBootstrapConfigBlock reads the shared configuration which is encoded in the consensus metadata in a block.
func ReadSharedConfigFromBootstrapConfigBlock(path string) ([]byte, error) {
	configBlock, err := configtxgen.ReadBlock(path)
	if err != nil {
		return nil, err
	}
	consensusMetadata, err := ReadConsensusMetadataFromConfigBlock(configBlock)
	if err != nil {
		return nil, err
	}

	return consensusMetadata, nil
}

func ReadConsensusMetadataFromConfigBlock(configBlock *common.Block) ([]byte, error) {
	envelope, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return nil, err
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelope, factory.GetDefault())
	if err != nil {
		return nil, err
	}
	orderer, exists := bundle.OrdererConfig()
	if !exists {
		return nil, errors.Wrapf(err, "orderer entry in the config block is empty")
	}

	return orderer.ConsensusMetadata(), nil
}
