/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCreateConfigBlockUpdate(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	defer gexec.CleanupBuildArtifacts()

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 3, 2, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// _, lastConfigBlock, err := config.ReadConfig(filepath.Join(dir, "config", "party1", "local_config_router.yaml"), testutil.CreateLoggerForModule(t, "TestCreateConfigBlockUpdate", zap.DebugLevel))
	// require.NoError(t, err)
	// require.NotNil(t, lastConfigBlock)

	// sharedconfig, err := configutil.ReadConfigEnvelopeFromConfigBlock(lastConfigBlock)
	// require.NoError(t, err)
	// require.NotNil(t, sharedconfig)

	// Create config update
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	newCACerts := [][]byte{[]byte("newCACert")}
	newTLSCACerts := [][]byte{[]byte("newTLSCACert")}

	configUpdateBuilder.UpdateBatchSizeConfig(t, cfgutil.NewBatchSizeConfig(cfgutil.BatchSizeConfigName.MaxMessageCount, 500))
	configUpdateBuilder.UpdateOrderingEndpoint(t, types.PartyID(1), "newIP", 1212)
	configUpdateBuilder.UpdateOrgEndpoints(t, types.PartyID(1))
	configUpdateBuilder.UpdateRouterEndpoint(t, types.PartyID(1), "newIP", 3434)
	configUpdateBuilder.UpdateAssemblerEndpoint(t, types.PartyID(1), "newIP", 3434)
	configUpdateBuilder.UpdateBatcherEndpoint(t, types.PartyID(1), types.ShardID(1), "newIP", 3434)
	configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.AutoRemoveTimeout, "10ms"))
	configUpdateBuilder.UpdateSmartBFTConfig(t, cfgutil.NewSmartBFTConfig(cfgutil.SmartBFTConfigName.RequestMaxBytes, "1048576"))
	configUpdateBuilder.RemoveParty(t, types.PartyID(2))
	configUpdateBuilder.AddNewParty(t, &protos.PartyConfig{
		CACerts:    newCACerts,
		TLSCACerts: newTLSCACerts,
		ConsenterConfig: &protos.ConsenterNodeConfig{
			Host:    "localhost",
			Port:    7050,
			TlsCert: []byte("consenterNewCert"),
		},
		RouterConfig: &protos.RouterNodeConfig{
			Host:    "localhost",
			Port:    8050,
			TlsCert: []byte("routerNewCert"),
		},
		AssemblerConfig: &protos.AssemblerNodeConfig{
			Host:    "localhost",
			Port:    9050,
			TlsCert: []byte("assemblerNewCert"),
		},
		BatchersConfig: []*protos.BatcherNodeConfig{
			{
				ShardID: 1,
				Host:    "localhost",
				Port:    10050,
				TlsCert: []byte("batcherNewCert"),
			},
		},
	})

	configUpdateBuilder.UpdateBatcherSignCert(t, types.PartyID(1), types.ShardID(1), []byte("newSignCert"))
	configUpdateBuilder.UpdateConsenterSignCert(t, types.PartyID(1), []byte("newSignCert"))
	configUpdateBuilder.UpdatePartyTLSCACerts(t, types.PartyID(1), newTLSCACerts)
	configUpdateBuilder.UpdatePartyCACerts(t, types.PartyID(1), newCACerts)

	configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)

	// Verify that the config update contains the expected updates
	configUpdate, err := configtx.UnmarshalConfigUpdate(configUpdatePbData)
	require.NoError(t, err)
	require.NotNil(t, configUpdate.WriteSet.Groups["Orderer"].Values["ConsensusType"].GetValue())

	consensusType := orderer.ConsensusType{}
	err = proto.Unmarshal(configUpdate.WriteSet.Groups["Orderer"].Values["ConsensusType"].GetValue(), &consensusType)
	require.NoError(t, err)

	sharedConfig := protos.SharedConfig{}
	err = proto.Unmarshal(consensusType.GetMetadata(), &sharedConfig)
	require.NoError(t, err)

	partiesConfig := sharedConfig.GetPartiesConfig()
	require.NotNil(t, partiesConfig)
	require.Equal(t, 3, len(partiesConfig))
	// Check new party added
	require.Equal(t, uint32(4), partiesConfig[2].PartyID)
	// Check party removed
	require.Equal(t, uint32(3), partiesConfig[1].PartyID)
	// Check certs updated
	require.Equal(t, newCACerts, partiesConfig[0].CACerts)
	require.Equal(t, newTLSCACerts, partiesConfig[0].TLSCACerts)

	// Further checks can be added here to verify other updates
}
