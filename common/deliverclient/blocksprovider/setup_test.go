/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider_test

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/stretchr/testify/require"
)

func testSetupBFT(t *testing.T, testDir string) (*common.Config, bccsp.BCCSP, error) {
	networkConfig := generateArmaNetworkConfig()
	sharedConfigYaml, sharedConfigPath := testutil.PrepareSharedConfigBinaryFromNetwork(t, networkConfig, testDir)
	block, err := generate.CreateGenesisBlock(testDir, testDir, sharedConfigYaml, sharedConfigPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	configEnv, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
	require.NoError(t, err)

	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
	require.NoError(t, err)

	return configEnv.Config, cryptoProvider, nil
}

func generateArmaNetworkConfig() generate.Network {
	n := generate.Network{
		UseTLSRouter:    "mTLS",
		UseTLSAssembler: "mTLS",
		MaxPartyID:      4,
	}
	for i := types.PartyID(1); i <= n.MaxPartyID; i++ {
		party := generate.Party{
			ID:                i,
			AssemblerEndpoint: fmt.Sprintf("party%d-assembler:1234", i),
			ConsenterEndpoint: fmt.Sprintf("party%d-consenter:1234", i),
			RouterEndpoint:    fmt.Sprintf("party%d-router:1234", i),
			BatchersEndpoints: []string{fmt.Sprintf("party%d-batcher1:1234", i), fmt.Sprintf("party%d-batcher2:1234", i)},
		}
		n.Parties = append(n.Parties, party)
	}

	return n
}
