/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate_test

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/hyperledger/fabric-x-orderer/config/generate"

	"github.com/stretchr/testify/require"
)

func TestSharedConfigGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")

	// 2.
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// 3.
	networkSharedConfig, err := generate.CreateArmaSharedConfig(networkConfig, networkLocalConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkSharedConfig)
}
