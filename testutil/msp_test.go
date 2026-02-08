/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/stretchr/testify/require"
)

func TestBuildTestLocalMSP_Create(t *testing.T) {
	// Generate crypto material
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	_ = CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// Override router1 local config yaml to point to the local msp and have a config store
	configYamlPath := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
	storagePath := path.Join(dir, "storage", "party1", "router")
	mspPath := path.Join(dir, "crypto", "ordererOrganizations", "org1", "orderers", "party1", "router", "msp")
	EditDirectoryInNodeConfigYAML(t, configYamlPath, storagePath)
	EditLocalMSPDirForNode(t, configYamlPath, mspPath)

	// Override router2 local config yaml to point to the local msp and have a config store
	configYamlPath = filepath.Join(dir, "config", "party2", "local_config_router.yaml")
	storagePath = path.Join(dir, "storage", "party2", "router")
	mspPath = path.Join(dir, "crypto", "ordererOrganizations", "org2", "orderers", "party2", "router", "msp")
	EditDirectoryInNodeConfigYAML(t, configYamlPath, storagePath)
	EditLocalMSPDirForNode(t, configYamlPath, mspPath)

	// Get router1 configuration
	routerNodeConfigPath1 := filepath.Join(dir, "config", fmt.Sprintf("party%d", 1), "local_config_router.yaml")
	configContent1, _, err := config.ReadConfig(routerNodeConfigPath1, flogging.MustGetLogger("ReadConfigConsensus"))
	require.NoError(t, err)
	require.NotNil(t, configContent1)
	// Build local msp and signer for Router1
	localmsp1, signer1, err := BuildTestLocalMSP(configContent1.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "org1")
	require.NoError(t, err)
	require.NotNil(t, signer1)
	require.NotNil(t, localmsp1)

	// Get router2 configuration
	routerNodeConfigPath2 := filepath.Join(dir, "config", fmt.Sprintf("party%d", 2), "local_config_router.yaml")
	configContent2, _, err := config.ReadConfig(routerNodeConfigPath2, flogging.MustGetLogger("ReadConfigConsensus"))
	require.NoError(t, err)
	require.NotNil(t, configContent2)
	// Build local msp and signer for Router2
	localmsp2, signer2, err := BuildTestLocalMSP(configContent2.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "org2")
	require.NoError(t, err)
	require.NotNil(t, signer2)
	require.NotNil(t, localmsp2)

	msg := []byte("tx")

	// sign on message with signer1 and signer2 and verify the signed messages are different
	signedMsg1, err := signer1.Sign(msg)
	require.NoError(t, err)
	require.NotNil(t, signedMsg1)
	signedMsg2, err := signer2.Sign(msg)
	require.NoError(t, err)
	require.NotNil(t, signedMsg2)
	require.NotEqual(t, signedMsg1, signedMsg2)

	id1, err := localmsp1.GetDefaultSigningIdentity()
	require.NoError(t, err)
	require.NotNil(t, id1)
	err = id1.Verify(msg, signedMsg1)
	require.NoError(t, err)

	id2, err := localmsp2.GetDefaultSigningIdentity()
	require.NoError(t, err)
	require.NotNil(t, id2)
	err = id2.Verify(msg, signedMsg2)
	require.NoError(t, err)

	err = id2.Verify(msg, signedMsg1)
	require.Error(t, err)
}
