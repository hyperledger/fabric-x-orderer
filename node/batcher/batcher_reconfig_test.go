/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Scenario:
// 1. Create config and crypto material
// 2. Create Batchers and stub Consenters
// 3. Prepare config block to be received by batchers from stub consenter. The config changes the AutoRemoveTimeout parameter.
// 4. Verify that batchers correctly handle the config tx and reach pending admin state.
// TODO: complete dynamic reconfig scenario when memory pool supports reconfig
func TestBatcherReconfigAutoRemoveTimeoutReachesPendingAdmin(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4, 5}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	netInfo.CleanUp()
	stubConsenters := createStubConsenters(t, dir, parties)
	batchers, genesisBlock, bundle := createBatcherNodes(t, dir, parties, numOfShards, stubConsenters)
	startBatcherNodes(batchers)

	defer func() {
		for _, sc := range stubConsenters {
			sc.StopNet()
		}
		for _, b := range batchers {
			b.Stop()
		}
	}()

	for i := range parties {
		blocks, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Equal(t, 1, len(blocks))
	}

	// create config block that changes the AutoRemoveTimeout parameter
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.AutoRemoveTimeout, "15ms"))
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configBlock, err := cfgutil.CreateConsensusConfigBlock(bundle, configUpdateEnvelope, genesisBlock.Header, 1, types.DecisionNum(1), 1, 0)
	require.NoError(t, err)

	// send the config block to the batchers by the stub consenters
	st := &state.State{N: uint16(len(parties)), Shards: []state.ShardTerm{{Shard: 1, Term: 0}}}
	for i := range parties {
		stubConsenters[i].UpdateStateHeaderWithConfigBlock(types.DecisionNum(1), []*common.Block{configBlock}, st)
	}

	// wait for batchers to append the config tx to the config store
	for j := range parties {
		require.Eventually(t, func() bool {
			block, err1 := batchers[j].ConfigStore.Last()
			blockNumbers, err2 := batchers[j].ConfigStore.ListBlockNumbers()
			return err1 == nil && err2 == nil && block.Header.Number == uint64(1) && len(blockNumbers) == 2
		}, 60*time.Second, 10*time.Millisecond)
	}

	// wait for the batcher to reach pending admin state
	for j := range parties {
		require.Eventually(t, func() bool {
			return batchers[j].GetStatus().GetState() == node_utils.StatePendingAdmin
		}, 60*time.Second, 10*time.Millisecond)
	}
}

// Scenario:
// 1. Create config and crypto material
// 2. Create Batcher and stub Consenter
// 3. Prepare config block to be received by batcher from stub consenter. The config removes the party of the batcher.
// 4. Verify that batcher correctly handle the config tx and that it reached pending admin state.
func TestBatcherReconfigPartyEvictionReachesPendingAdmin(t *testing.T) {
	parties := []types.PartyID{1}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	netInfo.CleanUp()
	stubConsenters := createStubConsenters(t, dir, parties)
	batchers, genesisBlock, bundle := createBatcherNodes(t, dir, parties, numOfShards, stubConsenters)
	startBatcherNodes(batchers)

	defer func() {
		for _, sc := range stubConsenters {
			sc.StopNet()
		}
		for _, b := range batchers {
			b.Stop()
		}
	}()

	// make sure the genesis block is stored in the config store
	for i := range parties {
		blocks, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Equal(t, 1, len(blocks))
	}

	// create config block that removes the batcher
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	partyToRemove := types.PartyID(1)
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configBlock, err := cfgutil.CreateConsensusConfigBlock(bundle, configUpdateEnvelope, genesisBlock.Header, 1, types.DecisionNum(1), 1, 0)
	require.NoError(t, err)

	// send the config block to the batchers by the stub consenters
	st := &state.State{N: uint16(len(parties)), Shards: []state.ShardTerm{{Shard: 1, Term: 0}}}
	for i := range parties {
		stubConsenters[i].UpdateStateHeaderWithConfigBlock(types.DecisionNum(1), []*common.Block{configBlock}, st)
	}

	// wait for batchers to append the config tx to the config store
	for j := range parties {
		require.Eventually(t, func() bool {
			block, err1 := batchers[j].ConfigStore.Last()
			blockNumbers, err2 := batchers[j].ConfigStore.ListBlockNumbers()
			return err1 == nil && err2 == nil && block.Header.Number == uint64(1) && len(blockNumbers) == 2
		}, 60*time.Second, 10*time.Millisecond)
	}

	// wait for the batcher to reach pending admin state
	for j := range parties {
		require.Eventually(t, func() bool {
			return batchers[j].GetStatus().GetState() == node_utils.StatePendingAdmin
		}, 60*time.Second, 10*time.Millisecond)
	}
}

// Scenario:
// 1. Create config and crypto material
// 2. Create Batcher and stub Consenter
// 3. Prepare config block to be received by batcher from stub consenter. The config changes the endpoint of the batcher.
// 4. Verify that batcher correctly handle the config tx and that it reached pending admin state.
func TestBatcherReconfigEndpointChangeReachesPendingAdmin(t *testing.T) {
	parties := []types.PartyID{1}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	netInfo.CleanUp()
	stubConsenters := createStubConsenters(t, dir, parties)
	batchers, genesisBlock, bundle := createBatcherNodes(t, dir, parties, numOfShards, stubConsenters)
	startBatcherNodes(batchers)

	defer func() {
		for _, sc := range stubConsenters {
			sc.StopNet()
		}
		for _, b := range batchers {
			b.Stop()
		}
	}()

	// make sure the genesis block is stored in the config store
	for i := range parties {
		blocks, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Equal(t, 1, len(blocks))
	}

	// create config block that changes the batcher's endpoint
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.UpdateBatcherEndpoint(t, types.PartyID(1), types.ShardID(1), "127.0.0.1", 8080)
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configBlock, err := cfgutil.CreateConsensusConfigBlock(bundle, configUpdateEnvelope, genesisBlock.Header, 1, types.DecisionNum(1), 1, 0)
	require.NoError(t, err)

	// send the config block to the batchers by the stub consenters
	st := &state.State{N: uint16(len(parties)), Shards: []state.ShardTerm{{Shard: 1, Term: 0}}}
	for i := range parties {
		stubConsenters[i].UpdateStateHeaderWithConfigBlock(types.DecisionNum(1), []*common.Block{configBlock}, st)
	}

	// wait for batchers to append the config tx to the config store
	for j := range parties {
		require.Eventually(t, func() bool {
			block, err1 := batchers[j].ConfigStore.Last()
			blockNumbers, err2 := batchers[j].ConfigStore.ListBlockNumbers()
			return err1 == nil && err2 == nil && block.Header.Number == uint64(1) && len(blockNumbers) == 2
		}, 60*time.Second, 10*time.Millisecond)
	}

	// wait for the batcher to reach pending admin state
	for j := range parties {
		require.Eventually(t, func() bool {
			return batchers[j].GetStatus().GetState() == node_utils.StatePendingAdmin
		}, 60*time.Second, 10*time.Millisecond)
	}
}

func createBatcherNodes(t *testing.T, dir string, parties []types.PartyID, numOfShards int, consenters []*stubConsenter) ([]*batcher.Batcher, *common.Block, channelconfig.Resources) {
	batcherNodes := make([]*batcher.Batcher, 0, len(parties))
	var genesisBlock *common.Block
	var bundle channelconfig.Resources
	for i, partyID := range parties {
		for j := 1; j <= numOfShards; j++ {
			nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), fmt.Sprintf("local_config_batcher%d.yaml", j))
			nodeConfig, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigBatcher%d%d", partyID, j), zap.DebugLevel))
			require.NoError(t, err)
			batcherConfig := nodeConfig.ExtractBatcherConfig(lastConfigBlock)
			require.NotNil(t, batcherConfig)
			_, signer, err := testutil.BuildTestLocalMSP(nodeConfig.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, nodeConfig.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID)
			require.NoError(t, err)
			require.NotNil(t, signer)
			batcherLogger := testutil.CreateLogger(t, int(partyID))
			batcher := batcher.CreateBatcher(batcherConfig, nodeConfig, batcherLogger, make(chan struct{}), consenters[i], &batcher.ConsenterControlEventSenderFactory{}, signer)
			batcherNodes = append(batcherNodes, batcher)
			genesisBlock = lastConfigBlock
			bundle = batcherConfig.Bundle
		}
	}
	return batcherNodes, genesisBlock, bundle
}

func startBatcherNodes(batcherNodes []*batcher.Batcher) {
	for _, batcher := range batcherNodes {
		batcher.StartBatcherService()
		batcher.Run()
	}
}

func createStubConsenters(t *testing.T, dir string, parties []types.PartyID) []*stubConsenter {
	consenterNodes := make([]*stubConsenter, 0, len(parties))
	for _, i := range parties {
		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		nodeConfig, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigConsenter%d", i), zap.DebugLevel))
		require.NoError(t, err)
		var partyConfig *ordererpb.PartyConfig
		for _, p := range nodeConfig.SharedConfig.PartiesConfig {
			if types.PartyID(p.PartyID) == i {
				partyConfig = p
				break
			}
		}
		require.NotNil(t, partyConfig)
		consenterConfig := nodeConfig.ExtractConsenterConfig(lastConfigBlock)
		require.NotNil(t, consenterConfig)
		srv := node_utils.CreateGRPCConsensus(consenterConfig)
		require.NotNil(t, srv)
		sk, err := tx.CreateECDSAPrivateKey(consenterConfig.SigningPrivateKey)
		require.NoError(t, err)
		require.NotNil(t, sk)
		pk := utils.GetPublicKeyFromCertificate(partyConfig.ConsenterConfig.SignCert)
		stubConsenter := NewStubConsenter(t, i, &node{
			GRPCServer: srv,
			TLSCert:    consenterConfig.TLSCertificateFile,
			TLSKey:     consenterConfig.TLSPrivateKeyFile,
			sk:         sk,
			pk:         pk,
		})
		consenterNodes = append(consenterNodes, stubConsenter)
	}
	return consenterNodes
}

func updateFileStorePath(t *testing.T, dir string, parties []types.PartyID, numOfShards int) {
	configLogger := testutil.CreateLoggerForModule(t, "LoadLocalConfig", zap.DebugLevel)

	for _, i := range parties {
		for j := 1; j <= numOfShards; j++ {
			fileStoreDir := t.TempDir()
			nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), fmt.Sprintf("local_config_batcher%d.yaml", j))
			localConfig, _, err := config.LoadLocalConfig(nodeConfigPath, configLogger)
			require.NoError(t, err)
			localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
			err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
			require.NoError(t, err)
		}
	}

	for _, i := range parties {
		fileStoreDir := t.TempDir()
		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath, configLogger)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		localConfig.NodeLocalConfig.ConsensusParams.WALDir = config.DefaultConsenterNodeConfigParams(fileStoreDir).WALDir
		err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
		require.NoError(t, err)
	}
}
