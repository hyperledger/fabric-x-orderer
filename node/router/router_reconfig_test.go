/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"

	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
)

func TestSendConfigUpdate(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// create the config request.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.AutoRemoveTimeout, "15ms"))
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configEnvelope, err := testSetup.bundle.ConfigtxValidator().ProposeConfigUpdate(configUpdateEnvelope)
	require.NoError(t, err)
	env, err := protoutil.CreateSignedEnvelope(common.HeaderType_CONFIG, testSetup.bundle.ConfigtxValidator().ChannelID(), nil, configEnvelope, int32(0), 0)
	require.NoError(t, err)
	require.NotNil(t, env)
	configReq, err := protoutil.Marshal(env)
	require.NoError(t, err)
	require.NotNil(t, configReq)

	// prepare the decision and deliver it.
	prevHash := protoutil.BlockHeaderHash(testSetup.genesisBlock.Header)
	configBlock, err := consensus.CreateConfigCommonBlock(testSetup.genesisBlock.GetHeader().GetNumber()+1, prevHash, 1, types.DecisionNum(1), 1, 0, configReq)
	require.NoError(t, err)
	shardID := types.ShardID(1)
	header := &state.Header{
		Num:                          1,
		DecisionNumOfLastConfigBlock: 1,
		AvailableCommonBlocks:        []*common.Block{configBlock},
		State:                        &state.State{N: uint16(len(parties)), ShardCount: 1, Shards: []state.ShardTerm{{Shard: shardID, Term: 0}}},
	}
	err = testSetup.stubConsenter.DeliverDecisionFromHeader(header)
	require.NoError(t, err)

	// check that the router has applied the new config and is running with the new config
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)
}

func TestPartyEvicted(t *testing.T) {
	partyId := types.PartyID(1)
	partyToRemove := partyId
	parties := []types.PartyID{partyId}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// create the config request.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configEnvelope, err := testSetup.bundle.ConfigtxValidator().ProposeConfigUpdate(configUpdateEnvelope)
	require.NoError(t, err)
	env, err := protoutil.CreateSignedEnvelope(common.HeaderType_CONFIG, testSetup.bundle.ConfigtxValidator().ChannelID(), nil, configEnvelope, int32(0), 0)
	require.NoError(t, err)
	require.NotNil(t, env)
	configReq, err := protoutil.Marshal(env)
	require.NoError(t, err)
	require.NotNil(t, configReq)

	// prepare the decision and deliver it.
	prevHash := protoutil.BlockHeaderHash(testSetup.genesisBlock.Header)
	configBlock, err := consensus.CreateConfigCommonBlock(testSetup.genesisBlock.GetHeader().GetNumber()+1, prevHash, 1, types.DecisionNum(1), 1, 0, configReq)
	require.NoError(t, err)
	shardID := types.ShardID(1)
	header := &state.Header{
		Num:                          1,
		DecisionNumOfLastConfigBlock: 1,
		AvailableCommonBlocks:        []*common.Block{configBlock},
		State:                        &state.State{N: uint16(len(parties)), ShardCount: 1, Shards: []state.ShardTerm{{Shard: shardID, Term: 0}}},
	}
	err = testSetup.stubConsenter.DeliverDecisionFromHeader(header)
	require.NoError(t, err)

	// check that the router detected that it got evicted and is pending admin.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
}

type reconfigTestSetup struct {
	routerNode         *router.Router
	routerFileStore    string
	stubConsenter      *stub.StubConsenter
	consenterFileStore string
	stubBatcher        *stub.StubBatcher
	batcherFileStore   string
	genesisBlock       *common.Block
	bundle             channelconfig.Resources
}

func (s *reconfigTestSetup) Start() {
	s.stubConsenter.Start()
	s.stubBatcher.Start()
	s.routerNode.StartRouterService()
}

func (s *reconfigTestSetup) Stop() {
	s.routerNode.Stop()
	s.stubBatcher.Stop()
	s.stubConsenter.Stop()
}

func createReconfigTestSetup(t *testing.T, dir string, partyId types.PartyID) *reconfigTestSetup {
	consenterFileStore := t.TempDir()
	consenterNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_consenter.yaml")
	stubConsenter := stub.NewStubConsenterFromConfig(t, consenterFileStore, consenterNodeConfigPath, nil)

	batcherFileStore := t.TempDir()
	batcherNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), fmt.Sprintf("local_config_batcher%d.yaml", 1))
	stubBatcher := stub.NewStubBatcherFromConfig(t, batcherFileStore, batcherNodeConfigPath, nil)

	routerFileStore := t.TempDir()
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_router.yaml")
	routerNode, genesisBlock, routerBundle := createRealRouterFromConfig(t, partyId, routerFileStore, routerNodeConfigPath)

	return &reconfigTestSetup{
		stubConsenter:      stubConsenter,
		consenterFileStore: consenterFileStore,
		stubBatcher:        &stubBatcher,
		batcherFileStore:   batcherFileStore,
		routerNode:         routerNode,
		routerFileStore:    routerFileStore,
		genesisBlock:       genesisBlock,
		bundle:             routerBundle,
	}
}

func createRealRouterFromConfig(t *testing.T, partyID types.PartyID, fileStoreDir string, nodeConfigPath string) (*router.Router, *common.Block, channelconfig.Resources) {
	if fileStoreDir != "" {
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
		require.NoError(t, err)
	}

	config, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigRouter%d", partyID), zap.DebugLevel))
	require.NoError(t, err)
	routerConfig := config.ExtractRouterConfig(lastConfigBlock)
	require.NotNil(t, routerConfig)
	_, signer, err := testutil.BuildTestLocalMSP(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID)
	require.NoError(t, err)
	require.NotNil(t, signer)
	routerLogger := testutil.CreateLogger(t, int(partyID))
	router := router.NewRouter(routerConfig, config, routerLogger, signer, make(chan struct{}), &policy.DefaultConfigUpdateProposer{}, &verify.DefaultOrdererRules{})
	return router, lastConfigBlock, routerConfig.Bundle
}
