/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/msp/mock"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestExtractAppTrustedRootsFromConfigBlock(t *testing.T) {
	t.Run("no application config", func(t *testing.T) {
		bundle := &mocks.FakeConfigResources{}
		bundle.ApplicationConfigReturns(nil, false)
		mockMSPManager := &mock.MSPManager{}
		fakeMsp := &mock.MSP{}
		mockMSPManager.GetMSPsReturns(
			map[string]msp.MSP{
				"test-member-role": fakeMsp,
			},
			nil,
		)
		bundle.MSPManagerReturns(mockMSPManager)
		res := config.ExtractAppTrustedRootsFromConfigBlock(bundle)
		require.Equal(t, res, [][]byte{})
	})

	t.Run("real envelope", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		_ = testutil.CreateNetwork(t, configPath, 4, 2, "mTLS", "mTLS")
		armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

		genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
		data, err := os.ReadFile(genesisBlockPath)
		require.NoError(t, err)
		genesisBlock, err := protoutil.UnmarshalBlock(data)
		require.NoError(t, err)

		env, err := protoutil.ExtractEnvelope(genesisBlock, 0)
		require.NoError(t, err)
		bundle, err := channelconfig.NewBundleFromEnvelope(env, factory.GetDefault())
		require.NoError(t, err)

		res := config.ExtractAppTrustedRootsFromConfigBlock(bundle)
		require.Equal(t, len(res), 4)
	})
}
