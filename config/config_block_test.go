/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-common/protoutil"

	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestReadGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutil.PrepareSharedConfigBinary(t, dir)
	block, err := generate.CreateGenesisBlock(dir, dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)

	blockPath := filepath.Join(dir, "bootstrap.block")
	data, err := os.ReadFile(blockPath)
	require.NoError(t, err)
	configBlock, err := protoutil.UnmarshalBlock(data)
	require.NoError(t, err)
	require.NotNil(t, block)
	consensusMetaData, err := config.ReadSharedConfigFromBootstrapConfigBlock(configBlock)
	require.NoError(t, err)

	var sharedConfigFromBlock protos.SharedConfig
	err = proto.Unmarshal(consensusMetaData, &sharedConfigFromBlock)
	require.NoError(t, err)

	sharedConfigYamlPath := filepath.Join(dir, "bootstrap", "shared_config.yaml")
	actualSharedConfig, _, err := config.LoadSharedConfig(sharedConfigYamlPath)
	require.NoError(t, err)

	proto.Equal(&sharedConfigFromBlock, actualSharedConfig)
}

func TestReadGenesisBlock_Errors(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutil.PrepareSharedConfigBinary(t, dir)
	block, err := generate.CreateGenesisBlock(dir, dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)

	blockPath := filepath.Join(dir, "bootstrap.block")
	data, err := os.ReadFile(blockPath)
	require.NoError(t, err)
	configBlock, err := protoutil.UnmarshalBlock(data)
	require.NoError(t, err)
	require.NotNil(t, block)

	// Mess the configBlock
	configBlock.Data = nil

	// expect error
	c, err := config.ReadSharedConfigFromBootstrapConfigBlock(configBlock)
	require.EqualError(t, err, "block data is nil")
	require.Nil(t, c)
}
