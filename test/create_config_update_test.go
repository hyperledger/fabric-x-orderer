/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/onsi/gomega/gexec"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

func TestCreateConfigBlockUpdate(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	defer gexec.CleanupBuildArtifacts()

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, 1, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// Create config update
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	configUpdatePbData := configutil.CreateConfigUpdate(t, dir, genesisBlockPath)
	require.NotEmpty(t, configUpdatePbData)

	// Create a dummy config update envelope
	configUpdateEnvelope := tx.CreateStructuredConfigUpdateEnvelope(configUpdatePbData)
	require.NotNil(t, configUpdateEnvelope)
}
