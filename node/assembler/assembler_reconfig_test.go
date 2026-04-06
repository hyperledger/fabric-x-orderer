/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSendConfigUpdate(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the assembler to be running.
	require.Eventually(t, func() bool {
		status := testSetup.assembblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 10*time.Second, 100*time.Millisecond)

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
	decisionNum := types.DecisionNum(1)
	s := &state.State{N: uint16(len(parties)), ShardCount: 1, Shards: []state.ShardTerm{{Shard: types.ShardID(1), Term: 0}}}

	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(1, types.ShardIDConsensus, 1, nil),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: configBlock,
			DecisionNum: decisionNum,
		},
	}
	err = testSetup.stubConsenter.DeliverConfigDecisionFromBA(ba, s)
	require.NoError(t, err)

	// check that the assembler has applied the new config and is running with the new config
	require.Eventually(t, func() bool {
		status := testSetup.assembblerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 10*time.Second, 100*time.Millisecond)
}

func TestPartyEvicted(t *testing.T) {
	partyId := types.PartyID(1)
	partyToRemove := partyId
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the assembler to be running.
	require.Eventually(t, func() bool {
		status := testSetup.assembblerNode.GetStatus()
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
	decisionNum := types.DecisionNum(1)
	s := &state.State{N: uint16(len(parties)), ShardCount: 1, Shards: []state.ShardTerm{{Shard: types.ShardID(1), Term: 0}}}

	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(1, types.ShardIDConsensus, 1, nil),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: configBlock,
			DecisionNum: decisionNum,
		},
	}
	err = testSetup.stubConsenter.DeliverConfigDecisionFromBA(ba, s)
	require.NoError(t, err)

	// check that the assembler detected that it got evicted and is pending admin.
	require.Eventually(t, func() bool {
		status := testSetup.assembblerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
}

type reconfigTestSetup struct {
	assembblerNode     *assembler.Assembler
	assemblerFileStore string
	assemblerListener  net.Listener
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
	if s.assemblerListener != nil {
		s.assemblerListener.Close()
	}
	s.assembblerNode.StartAssemblerService()
}

func (s *reconfigTestSetup) Stop() {
	s.assembblerNode.Stop()
	s.stubBatcher.Stop()
	s.stubConsenter.Stop()
}

func createReconfigTestSetup(t *testing.T, dir string, partyId types.PartyID) *reconfigTestSetup {
	numOfShards := 1
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	consenterFileStore := t.TempDir()
	consenterListener := netInfo[testutil.NodeName{PartyID: partyId, NodeType: testutil.Consensus}].Listener
	consenterNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_consenter.yaml")
	stubConsenter := stub.NewStubConsenterFromConfig(t, consenterFileStore, consenterNodeConfigPath, consenterListener)

	batcherFileStore := t.TempDir()
	batcherListener := netInfo[testutil.NodeName{PartyID: partyId, NodeType: testutil.Batcher, ShardID: types.ShardID(1)}].Listener
	batcherNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), fmt.Sprintf("local_config_batcher%d.yaml", 1))
	stubBatcher := stub.NewStubBatcherFromConfig(t, batcherFileStore, batcherNodeConfigPath, batcherListener)

	assemblerFileStore := t.TempDir()
	assemblerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_assembler.yaml")
	assemblerListener := netInfo[testutil.NodeName{PartyID: partyId, NodeType: testutil.Assembler}].Listener
	assemblerNode, genesisBlock, assemblerBundle := createRealAssemblerFromConfig(t, partyId, assemblerFileStore, assemblerNodeConfigPath)

	return &reconfigTestSetup{
		stubConsenter:      stubConsenter,
		consenterFileStore: consenterFileStore,
		stubBatcher:        &stubBatcher,
		batcherFileStore:   batcherFileStore,
		assembblerNode:     assemblerNode,
		assemblerFileStore: assemblerFileStore,
		assemblerListener:  assemblerListener,
		genesisBlock:       genesisBlock,
		bundle:             assemblerBundle,
	}
}

func createRealAssemblerFromConfig(t *testing.T, partyID types.PartyID, fileStoreDir string, nodeConfigPath string) (*assembler.Assembler, *common.Block, channelconfig.Resources) {
	if fileStoreDir != "" {
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
		require.NoError(t, err)
	}

	configuration, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigAssembler%d", partyID), zap.DebugLevel))
	require.NoError(t, err)
	assemblerNodeConfig := configuration.ExtractAssemblerConfig(lastConfigBlock)
	require.NotNil(t, assemblerNodeConfig)
	_, signer, err := testutil.BuildTestLocalMSP(configuration.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configuration.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID)
	require.NoError(t, err)
	require.NotNil(t, signer)
	assemblerLogger := testutil.CreateLogger(t, int(partyID))

	assembler := assembler.NewAssembler(assemblerNodeConfig, configuration, lastConfigBlock, make(chan struct{}), assemblerLogger)
	return assembler, lastConfigBlock, assemblerNodeConfig.Bundle
}
