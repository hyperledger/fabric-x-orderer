/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtxgen

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"

	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protolator"
	"github.com/hyperledger/fabric-x-common/protolator/protoext/ordererext"
	"github.com/hyperledger/fabric-x-common/protolator/protoext/peerext"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

var logger = util.MustGetLogger("common.tools.configtxgen")

// GetOutputBlock generates a genesis block.
func GetOutputBlock(config *Profile, channelID string) (*cb.Block, error) {
	pgen, err := NewBootstrapper(config)
	if err != nil {
		return nil, errors.WithMessage(err, "could not create bootstrapper")
	}
	logger.Info("Generating genesis block")
	if config.Orderer == nil {
		return nil, errors.New("refusing to generate block which is missing orderer section")
	}
	if config.Consortiums != nil {
		logger.Error("Warning: 'Consortiums' should be nil since system channel is no longer supported in Fabric v3.x")
	} else {
		if config.Application == nil {
			return nil, errors.New("refusing to generate application channel block which is missing application section")
		}
		logger.Info("Creating application channel genesis block")
	}
	genesisBlock := pgen.GenesisBlockForChannel(channelID)
	return genesisBlock, nil
}

// WriteOutputBlock writes a block to a file.
func WriteOutputBlock(block *cb.Block, outputBlock string) error {
	err := writeFile(outputBlock, protoutil.MarshalOrPanic(block), 0o640)
	if err != nil {
		return fmt.Errorf("error writing genesis block: %s", err)
	}
	return nil
}

// DoOutputBlock generates a genesis block and writes it to a file.
func DoOutputBlock(config *Profile, channelID, outputBlock string) error {
	genesisBlock, err := GetOutputBlock(config, channelID)
	if err != nil {
		return err
	}
	logger.Info("Writing genesis block")
	return WriteOutputBlock(genesisBlock, outputBlock)
}

// DoOutputChannelCreateTx generate a config TX and writes it to a file.
func DoOutputChannelCreateTx(conf, baseProfile *Profile, channelID, outputChannelCreateTx string) error {
	logger.Info("Generating new channel configtx")

	var configtx *common.Envelope
	var err error
	if baseProfile == nil {
		configtx, err = MakeChannelCreationTransaction(channelID, nil, conf)
	} else {
		configtx, err = MakeChannelCreationTransactionWithSystemChannelContext(channelID, nil, conf, baseProfile)
	}
	if err != nil {
		return err
	}

	logger.Info("Writing new channel tx")
	err = writeFile(outputChannelCreateTx, protoutil.MarshalOrPanic(configtx), 0o640)
	if err != nil {
		return fmt.Errorf("error writing channel create tx: %s", err)
	}
	return nil
}

// ReadBlock reads a block.
func ReadBlock(blockPath string) (*cb.Block, error) {
	data, err := os.ReadFile(blockPath)
	if err != nil {
		return nil, fmt.Errorf("could not read block %s", blockPath)
	}
	block, err := protoutil.UnmarshalBlock(data)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling to block: %s", err)
	}
	return block, nil
}

// DoInspectBlock inspects a block from a file.
func DoInspectBlock(inspectBlock string) error {
	logger.Info("Inspecting block")
	block, err := ReadBlock(inspectBlock)
	if err != nil {
		return err
	}
	err = protolator.DeepMarshalJSON(os.Stdout, block)
	if err != nil {
		return fmt.Errorf("malformed block contents: %s", err)
	}
	return nil
}

// DoInspectChannelCreateTx inspects a config TX from a file.
func DoInspectChannelCreateTx(inspectChannelCreateTx string) error {
	logger.Info("Inspecting transaction")
	data, err := os.ReadFile(inspectChannelCreateTx)
	if err != nil {
		return fmt.Errorf("could not read channel create tx: %s", err)
	}

	logger.Info("Parsing transaction")
	env, err := protoutil.UnmarshalEnvelope(data)
	if err != nil {
		return fmt.Errorf("Error unmarshalling envelope: %s", err)
	}

	err = protolator.DeepMarshalJSON(os.Stdout, env)
	if err != nil {
		return fmt.Errorf("malformed transaction contents: %s", err)
	}

	return nil
}

// DoPrintOrg prints organization info.
func DoPrintOrg(t *TopLevel, printOrg string) error { //nolint:gocognit // cognitive complexity 20.
	for _, org := range t.Organizations {
		if org.Name == printOrg {
			if len(org.OrdererEndpoints) > 0 {
				// An Orderer OrgGroup
				channelCapabilities := t.Capabilities["Channel"]
				og, err := NewOrdererOrgGroup(org, channelCapabilities)
				if err != nil {
					return errors.Wrapf(err, "bad org definition for org %s", org.Name)
				}

				if err := protolator.DeepMarshalJSON(os.Stdout, &ordererext.DynamicOrdererOrgGroup{ConfigGroup: og}); err != nil {
					return errors.Wrapf(err, "malformed org definition for org: %s", org.Name)
				}
				return nil
			}

			// Otherwise assume it is an Application OrgGroup, where the encoder is not strict whether anchor peers exist or not
			ag, err := NewApplicationOrgGroup(org)
			if err != nil {
				return errors.Wrapf(err, "bad org definition for org %s", org.Name)
			}
			if err := protolator.DeepMarshalJSON(os.Stdout, &peerext.DynamicApplicationOrgGroup{ConfigGroup: ag}); err != nil {
				return errors.Wrapf(err, "malformed org definition for org: %s", org.Name)
			}
			return nil
		}
	}
	return errors.Errorf("organization %s not found", printOrg)
}

func writeFile(filename string, data []byte, perm os.FileMode) error {
	dirPath := filepath.Dir(filename)
	exists, err := dirExists(dirPath)
	if err != nil {
		return err
	}
	if !exists {
		err = os.MkdirAll(dirPath, 0o750)
		if err != nil {
			return err
		}
	}
	return os.WriteFile(filename, data, perm)
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
