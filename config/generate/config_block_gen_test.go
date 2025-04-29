package generate_test

import (
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/testutil/fabric"

	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/config/generate"
)

func TestCreateGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigPath := testutil.PrepareSharedConfigBinary(t, dir)

	block, err := generate.CreateGenesisBlock(dir, sharedConfigYaml, sharedConfigPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)
}
