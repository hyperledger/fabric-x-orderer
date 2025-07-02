/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/stretchr/testify/require"
)

func TestCreateGenesisBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigPath := testutil.PrepareSharedConfigBinary(t, dir)

	block, err := generate.CreateGenesisBlock(dir, sharedConfigYaml, sharedConfigPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)
}
