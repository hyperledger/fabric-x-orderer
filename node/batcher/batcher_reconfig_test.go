/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
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
// 4. Verify that batchers correctly handle the config tx and apply the new config.
// 5. After reconfiguration, the batcher continue processing transactions.
func TestBatcherReceivesConfigBlockFromConsensusAndApplyConfig_ChangeBatchTimeoutsParam(t *testing.T) {
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

	for i := 0; i < len(parties); i++ {
		blocks, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Equal(t, len(blocks), 1)
	}

	// receive config block from consensus
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.AutoRemoveTimeout, "15ms"))
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configBlock, err := cfgutil.CreateConsensusConfigBlock(bundle, configUpdateEnvelope, genesisBlock.Header, 1, types.DecisionNum(1), 1, 0)
	require.NoError(t, err)
	availableCommonBlocks := []*common.Block{configBlock}
	shardID := types.ShardID(1)
	state := &state.State{N: uint16(len(parties)), ShardCount: 1, Shards: []state.ShardTerm{{Shard: shardID, Term: 0}}}

	for i := range parties {
		stubConsenters[i].UpdateStateHeaderWithConfigBlock(types.DecisionNum(1), availableCommonBlocks, state)
	}

	// wait for batchers to append the config tx to the config store
	for j := range parties {
		require.Eventually(t, func() bool {
			block, err1 := batchers[j].ConfigStore.Last()
			blockNumbers, err2 := batchers[j].ConfigStore.ListBlockNumbers()
			return err1 == nil && err2 == nil && block.Header.Number == uint64(1) && len(blockNumbers) == 2
		}, 60*time.Second, 10*time.Millisecond)
	}

	// wait for the batchers initialized with the new AutoRemoveTimeout parameter
	for j := range parties {
		require.Eventually(t, func() bool {
			return batchers[j].GetConfig().AutoRemoveTimeout == 15*time.Millisecond
		}, 60*time.Second, 10*time.Millisecond)
	}

	// wait for the batcher to initialize
	for j := range parties {
		require.Eventually(t, func() bool {
			return batchers[j].GetStatus() == "Running"
		}, 60*time.Second, 10*time.Millisecond)
	}

	// find the primary
	var primaryBatcher *batcher.Batcher
	primaryID := batchers[0].GetPrimaryID()
	for _, b := range batchers {
		if b.GetPrimaryID() == primaryID {
			primaryBatcher = b
			break
		}
	}

	// submit request
	req := tx.CreateStructuredRequestWithConfigSeq([]byte{2}, 1)
	require.Eventually(t, func() bool {
		resp, err := primaryBatcher.Submit(context.Background(), req)
		return err == nil && resp.Error == ""
	}, 60*time.Second, 10*time.Millisecond)

	// make sure request was batched
	for _, b := range batchers {
		require.Eventually(t, func() bool {
			return b.Ledger.Height(primaryID) == uint64(1)
		}, 30*time.Second, 10*time.Millisecond)
	}

	// make sure consenters received the required BAF
	for _, sc := range stubConsenters {
		require.Eventually(t, func() bool {
			return sc.BAFCount() == len(parties)
		}, 30*time.Second, 10*time.Millisecond)
	}
}

// Scenario:
// 1. Create config and crypto material
// 2. Create Batcher and stub Consenter
// 3. Prepare config block to be received by batcher from stub consenter. The config removes the party of the batcher.
// 4. Verify that batcher correctly handle the config tx, detects that it has been removed and admin operation is required as a result.
func TestBatcherPartyEvicted(t *testing.T) {
	parties := []types.PartyID{1}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	for _, n := range netInfo {
		if n.Listener != nil {
			_ = n.Listener.Close()
		}
	}

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	stubConsenters := createStubConsenters(t, dir, parties)
	batchers, genesisBlock, bundle := createBatcherNodes(t, dir, parties, numOfShards, stubConsenters)
	startBatcherNodes(batchers)

	defer func() {
		for _, sc := range stubConsenters {
			sc.Stop()
		}
		for _, b := range batchers {
			b.Stop()
		}
	}()

	// create config block that removes the batcher
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	partyToRemove := types.PartyID(1)
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)
	require.NotNil(t, configUpdatePbData)
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configBlock, err := cfgutil.CreateConsensusConfigBlock(bundle, configUpdateEnvelope, genesisBlock.Header, 1, types.DecisionNum(1), 1, 0)
	require.NoError(t, err)

	// TODO: instead of Add, SoftStop and ApplyConfig do the full path, and use State to discover PendingAdmin
	// append block to the config store
	err = batchers[0].ConfigStore.Add(configBlock)
	require.NoError(t, err)

	// soft stop batcher
	batchers[0].SoftStop()

	// apply config
	isAdminOperationRequired, err := batchers[0].ApplyConfig(configBlock)
	require.NoError(t, err)
	require.True(t, isAdminOperationRequired)
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
		var partyConfig *protos.PartyConfig
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
	for _, i := range parties {
		for j := 1; j <= numOfShards; j++ {
			fileStoreDir := t.TempDir()
			nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), fmt.Sprintf("local_config_batcher%d.yaml", j))
			localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
			require.NoError(t, err)
			localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
			err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
			require.NoError(t, err)
		}
	}

	for _, i := range parties {
		fileStoreDir := t.TempDir()
		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
		require.NoError(t, err)
	}
}
