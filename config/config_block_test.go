package config_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/cmd/testutils"
	"github.ibm.com/decentralized-trust-research/arma/config"
	"github.ibm.com/decentralized-trust-research/arma/config/generate"
	"github.ibm.com/decentralized-trust-research/arma/config/protos"
	"github.ibm.com/decentralized-trust-research/arma/testutil/fabric"
	"google.golang.org/protobuf/proto"
)

func TestReadGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigBinaryPath := testutils.PrepareSharedConfigBinary(t, dir)
	block, err := generate.CreateGenesisBlock(dir, sharedConfigYaml, sharedConfigBinaryPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)

	blockPath := filepath.Join(dir, "bootstrap.block")
	consensusMetaData, err := config.ReadSharedConfigFromBootstrapConfigBlock(blockPath)
	require.NoError(t, err)

	var sharedConfigFromBlock protos.SharedConfig
	err = proto.Unmarshal(consensusMetaData, &sharedConfigFromBlock)
	require.NoError(t, err)

	sharedConfigYamlPath := filepath.Join(dir, "bootstrap", "shared_config.yaml")
	actualSharedConfig, err := config.LoadSharedConfig(sharedConfigYamlPath)
	require.NoError(t, err)

	proto.Equal(&sharedConfigFromBlock, actualSharedConfig)
}
