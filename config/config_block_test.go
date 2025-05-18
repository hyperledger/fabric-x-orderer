/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"github.ibm.com/decentralized-trust-research/fabricx-config/protoutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/config"
	"github.ibm.com/decentralized-trust-research/arma/config/generate"
	"github.ibm.com/decentralized-trust-research/arma/config/protos"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
	"github.ibm.com/decentralized-trust-research/arma/testutil/fabric"
	"google.golang.org/protobuf/proto"
)

func TestReadGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutil.PrepareSharedConfigBinary(t, dir)
	block, err := generate.CreateGenesisBlock(dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
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
