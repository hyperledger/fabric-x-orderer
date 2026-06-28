/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	top_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Scenario:
//  1. Build a 4-party Armageddon network; start 4 source assemblers with stub batchers/consenters.
//  2. Drive numBlocks regular blocks through every source assembler.
//  3. Add party 5 via a config update (PrepareAndAddNewParty). Deliver the config block to all
//     4 source consenters and wait for them to apply the new configuration.
//  4. Create a joining assembler for party 5 with an empty ledger. Because
//     syncTriggerBlock.Number == configBlockNum > 0, initLedger detects the gap and runs the
//     real AssemblerBFTSynchronizer, which fetches all blocks from the running source assemblers.
//  5. Verify the joining assembler's TxCount matches the sources after sync.
func TestAssemblerSync_RealSynchronizer(t *testing.T) {
	const numParties = 4
	const numBlocks = 5

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	armaNodesInfo := testutil.CreateNetwork(t, configPath, numParties, 1, "TLS", "TLS")
	require.NotNil(t, armaNodesInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	shardID := types.ShardID(1)
	batchersStub, batcherInfos, batcherCleanup := createStubBatchersAndInfos(t, numParties, shardID, ca)
	defer batcherCleanup()
	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	sourceAssemblers := make([]*assembler.Assembler, numParties)
	sourceConsenters := make([]*stubConsenter, numParties)
	var genesisBlock *cb.Block
	for i := 0; i < numParties; i++ {
		partyID := types.PartyID(i + 1)
		consenter := NewStubConsenter(t, partyID, ca)
		defer consenter.Stop()
		sourceConsenters[i] = consenter
		asm, gb := createSourceAssemblerWithStubs(t, partyID, dir, armaNodesInfo, shards, consenter.consenterInfo)
		defer asm.Stop()
		sourceAssemblers[i] = asm
		if i == 0 {
			genesisBlock = gb
		}
	}

	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == 1
		}, 5*time.Second, 100*time.Millisecond)
	}

	obaCreator, _ := NewOrderedBatchAttestationCreatorWithGenesis(genesisBlock)
	var lastOBA *state.AvailableBatchOrdered
	for i := 0; i < numBlocks; i++ {
		batch := types.NewSimpleBatch(shardID, 1, types.BatchSequence(i),
			types.BatchedRequests{[]byte{byte(i)}}, 0, nil)
		batchersStub[0].SetNextBatch(batch)
		oba := obaCreator.Append(batch, types.DecisionNum(i+1), 0, 1)
		lastOBA = oba
		for _, c := range sourceConsenters {
			c.SetNextDecision(oba)
		}
		for _, c := range sourceConsenters {
			<-c.decisionSentCh
		}
	}

	expectedTxCountAfterBlocks := uint64(1 + numBlocks)
	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == expectedTxCountAfterBlocks
		}, 10*time.Second, 100*time.Millisecond)
	}

	addedPartyID, configBlock := addPartyViaConfigUpdate(t, dir, numParties, genesisBlock,
		protoutil.BlockHeaderHash(lastOBA.OrderingInformation.CommonBlock.Header),
		uint64(numBlocks+1), types.DecisionNum(numBlocks+1),
		shardID, sourceConsenters)

	expectedTxCountAfterConfig := expectedTxCountAfterBlocks + 1
	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == expectedTxCountAfterConfig
		}, 20*time.Second, 100*time.Millisecond)
	}

	waitForSourcesRunningWithConfig(t, sourceAssemblers, 1)

	joiningAsm := startJoiningAssembler(t, dir, addedPartyID, shardID, batcherInfos, ca, configBlock)
	defer joiningAsm.Stop()

	require.Eventually(t, func() bool {
		return joiningAsm.GetTxCount() == expectedTxCountAfterConfig
	}, 30*time.Second, 100*time.Millisecond,
		"joining assembler should sync all blocks including config block")
}

// Scenario:
//  1. Build a 4-party Armageddon network; start 4 source assemblers.
//  2. Skip regular blocks — drive no data blocks before the config update.
//  3. Add party 5 via a config update at block 1 (genesis is block 0).
//  4. Create a joining assembler for party 5 with an empty ledger and verify it syncs
//     genesis block + config block (TxCount == 2).
func TestAssemblerSync_NoRegularBlocks(t *testing.T) {
	const numParties = 4

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	armaNodesInfo := testutil.CreateNetwork(t, configPath, numParties, 1, "TLS", "TLS")
	require.NotNil(t, armaNodesInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	shardID := types.ShardID(1)
	_, batcherInfos, batcherCleanup := createStubBatchersAndInfos(t, numParties, shardID, ca)
	defer batcherCleanup()
	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	sourceAssemblers := make([]*assembler.Assembler, numParties)
	sourceConsenters := make([]*stubConsenter, numParties)
	var genesisBlock *cb.Block
	for i := 0; i < numParties; i++ {
		partyID := types.PartyID(i + 1)
		consenter := NewStubConsenter(t, partyID, ca)
		defer consenter.Stop()
		sourceConsenters[i] = consenter
		asm, gb := createSourceAssemblerWithStubs(t, partyID, dir, armaNodesInfo, shards, consenter.consenterInfo)
		defer asm.Stop()
		sourceAssemblers[i] = asm
		if i == 0 {
			genesisBlock = gb
		}
	}

	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == 1
		}, 5*time.Second, 100*time.Millisecond)
	}

	// Config block is at number 1: no regular blocks precede it, so prevHash comes from genesis.
	addedPartyID, configBlock := addPartyViaConfigUpdate(t, dir, numParties, genesisBlock,
		protoutil.BlockHeaderHash(genesisBlock.Header),
		1, types.DecisionNum(1),
		shardID, sourceConsenters)

	const expectedTxCount = uint64(2) // genesis + config block
	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == expectedTxCount
		}, 20*time.Second, 100*time.Millisecond)
	}

	waitForSourcesRunningWithConfig(t, sourceAssemblers, 1)

	joiningAsm := startJoiningAssembler(t, dir, addedPartyID, shardID, batcherInfos, ca, configBlock)
	defer joiningAsm.Stop()

	require.Eventually(t, func() bool {
		return joiningAsm.GetTxCount() == expectedTxCount
	}, 30*time.Second, 100*time.Millisecond,
		"joining assembler should sync genesis block and config block")
}

// Scenario:
//  1. Build a 4-party Armageddon network; start 4 source assemblers.
//  2. Drive numBlocks regular blocks and a config update (same as TestAssemblerSync_RealSynchronizer).
//  3. Stop one source assembler (party 1) before the joining assembler begins sync.
//  4. Verify that the joining assembler still completes sync using the remaining 3 sources.
//     For a 4-party BFT network (f=1) this is the minimum fault-tolerant scenario.
func TestAssemblerSync_SomeSourcesUnavailable(t *testing.T) {
	const numParties = 4
	const numBlocks = 3

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	armaNodesInfo := testutil.CreateNetwork(t, configPath, numParties, 1, "TLS", "TLS")
	require.NotNil(t, armaNodesInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	shardID := types.ShardID(1)
	batchersStub, batcherInfos, batcherCleanup := createStubBatchersAndInfos(t, numParties, shardID, ca)
	defer batcherCleanup()
	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	sourceAssemblers := make([]*assembler.Assembler, numParties)
	sourceConsenters := make([]*stubConsenter, numParties)
	var genesisBlock *cb.Block
	for i := 0; i < numParties; i++ {
		partyID := types.PartyID(i + 1)
		consenter := NewStubConsenter(t, partyID, ca)
		defer consenter.Stop()
		sourceConsenters[i] = consenter
		asm, gb := createSourceAssemblerWithStubs(t, partyID, dir, armaNodesInfo, shards, consenter.consenterInfo)
		defer asm.Stop()
		sourceAssemblers[i] = asm
		if i == 0 {
			genesisBlock = gb
		}
	}

	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == 1
		}, 5*time.Second, 100*time.Millisecond)
	}

	obaCreator, _ := NewOrderedBatchAttestationCreatorWithGenesis(genesisBlock)
	var lastOBA *state.AvailableBatchOrdered
	for i := 0; i < numBlocks; i++ {
		batch := types.NewSimpleBatch(shardID, 1, types.BatchSequence(i),
			types.BatchedRequests{[]byte{byte(i)}}, 0, nil)
		batchersStub[0].SetNextBatch(batch)
		oba := obaCreator.Append(batch, types.DecisionNum(i+1), 0, 1)
		lastOBA = oba
		for _, c := range sourceConsenters {
			c.SetNextDecision(oba)
		}
		for _, c := range sourceConsenters {
			<-c.decisionSentCh
		}
	}

	expectedTxCountAfterBlocks := uint64(1 + numBlocks)
	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == expectedTxCountAfterBlocks
		}, 10*time.Second, 100*time.Millisecond)
	}

	addedPartyID, configBlock := addPartyViaConfigUpdate(t, dir, numParties, genesisBlock,
		protoutil.BlockHeaderHash(lastOBA.OrderingInformation.CommonBlock.Header),
		uint64(numBlocks+1), types.DecisionNum(numBlocks+1),
		shardID, sourceConsenters)

	expectedTxCountAfterConfig := expectedTxCountAfterBlocks + 1
	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			return asm.GetTxCount() == expectedTxCountAfterConfig
		}, 20*time.Second, 100*time.Millisecond)
	}

	waitForSourcesRunningWithConfig(t, sourceAssemblers, 1)

	// Stop party 1's assembler. The joining assembler must sync using only parties 2–4 (3/4 sources).
	// For a 4-party BFT network with f=1 this is the minimum number of correct replicas.
	// Stop() is idempotent, so the deferred Stop() at cleanup is a safe no-op.
	sourceAssemblers[0].Stop()

	joiningAsm := startJoiningAssembler(t, dir, addedPartyID, shardID, batcherInfos, ca, configBlock)
	defer joiningAsm.Stop()

	require.Eventually(t, func() bool {
		return joiningAsm.GetTxCount() == expectedTxCountAfterConfig
	}, 60*time.Second, 100*time.Millisecond,
		"joining assembler should sync despite one unavailable source")
}

// addPartyViaConfigUpdate builds a config block that adds a new party (via PrepareAndAddNewParty),
// delivers it to all source consenters, and returns the config block and the new party's ID.
func addPartyViaConfigUpdate(
	t *testing.T,
	dir string,
	numParties int,
	genesisBlock *cb.Block,
	prevHash []byte,
	configBlockNum uint64,
	configDecisionNum types.DecisionNum,
	shardID types.ShardID,
	sourceConsenters []*stubConsenter,
) (types.PartyID, *cb.Block) {
	t.Helper()

	bootstrapBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, bootstrapBlockPath)
	addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
	for _, info := range addedNetInfo {
		if info != nil && info.Listener != nil {
			info.Listener.Close()
		}
	}

	configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
	signingParties := make([]types.PartyID, numParties)
	for i := range signingParties {
		signingParties[i] = types.PartyID(i + 1)
	}

	bundle := bundleFromBlock(t, genesisBlock)
	configUpdateEnv := cfgutil.CreateConfigTX(t, dir, signingParties, 1, configUpdatePbData)
	configEnvelope, err := bundle.ConfigtxValidator().ProposeConfigUpdate(configUpdateEnv)
	require.NoError(t, err)
	env, err := protoutil.CreateSignedEnvelope(
		cb.HeaderType_CONFIG,
		bundle.ConfigtxValidator().ChannelID(),
		nil, configEnvelope, int32(0), 0,
	)
	require.NoError(t, err)
	configReq, err := protoutil.Marshal(env)
	require.NoError(t, err)

	configBlock, err := consensus.CreateConfigCommonBlock(configBlockNum, prevHash, 0, configDecisionNum, 1, 0, configReq)
	require.NoError(t, err)

	configBA := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(1, types.ShardIDConsensus, 1, nil),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: configBlock,
			DecisionNum: configDecisionNum,
		},
	}
	st := &state.State{
		N:      uint16(numParties + 1),
		Shards: []state.ShardTerm{{Shard: shardID, Term: 0}},
	}
	for _, c := range sourceConsenters {
		c.SetNextConfigDecision(configBA, st)
	}
	for _, c := range sourceConsenters {
		<-c.decisionSentCh
	}

	return addedPartyID, configBlock
}

// waitForSourcesRunningWithConfig waits until all source assemblers are back in StateRunning
// with the expected config sequence number. The gRPC Deliver server is briefly unavailable
// between net.Stop() and the new StartAssemblerService() during the restart triggered by
// ProcessNewConfigBlock; the joining assembler's sync would fail if it starts mid-restart.
func waitForSourcesRunningWithConfig(t *testing.T, sourceAssemblers []*assembler.Assembler, configSeq uint64) {
	t.Helper()
	for _, asm := range sourceAssemblers {
		require.Eventually(t, func() bool {
			status := asm.GetStatus()
			return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == configSeq
		}, 20*time.Second, 100*time.Millisecond, "source assembler should restart with new config")
	}
}

// startJoiningAssembler creates and starts a joining assembler for the given party, adding a
// stub batcher for that party to the shard info, and returns the started assembler.
func startJoiningAssembler(
	t *testing.T,
	dir string,
	partyID types.PartyID,
	shardID types.ShardID,
	existingBatcherInfos []config.BatcherInfo,
	ca tlsgen.CA,
	syncConfigBlock *cb.Block,
) *assembler.Assembler {
	t.Helper()

	numExistingParties := len(existingBatcherInfos)
	allParties := make([]types.PartyID, numExistingParties+1)
	for i := range allParties {
		allParties[i] = types.PartyID(i + 1)
	}

	joiningBatcher := NewStubBatcher(t, shardID, partyID, allParties, ca)
	t.Cleanup(joiningBatcher.Stop)

	joiningShards := []config.ShardInfo{{ShardId: shardID, Batchers: append(existingBatcherInfos, joiningBatcher.batcherInfo)}}
	joiningConsenter := NewStubConsenter(t, partyID, ca)
	t.Cleanup(joiningConsenter.Stop)

	joiningAsm := createJoiningAssembler(t, partyID, dir, joiningShards, joiningConsenter.consenterInfo, syncConfigBlock)
	joiningAsm.StartAssemblerService()
	return joiningAsm
}

// createSourceAssemblerWithStubs creates a real assembler from armageddon-generated config,
// overriding Shards and Consenter so it uses in-test stubs rather than the armageddon-generated
// batcher/consenter endpoints. The pre-allocated gRPC listener for this party is closed so the
// assembler can bind on the same port.
// Returns the assembler and the genesis block (block 0) so callers can seed the OBA creator
// with the correct hash chain.
func createSourceAssemblerWithStubs(
	t *testing.T,
	partyID types.PartyID,
	dir string,
	armaNodesInfo testutil.ArmaNodesInfoMap,
	shards []config.ShardInfo,
	consenterInfo config.ConsenterInfo,
) (*assembler.Assembler, *cb.Block) {
	t.Helper()

	// Close the pre-allocated listener so StartAssemblerService can bind on that port.
	nodeName := testutil.NodeName{PartyID: partyID, NodeType: testutil.Assembler}
	if info := armaNodesInfo[nodeName]; info != nil && info.Listener != nil {
		info.Listener.Close()
	}

	nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "local_config_assembler.yaml")

	// Point the file store to a fresh, empty ledger directory.
	fileStoreDir := t.TempDir()
	localConfig, _, err := top_config.LoadLocalConfig(nodeConfigPath)
	require.NoError(t, err)
	localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
	require.NoError(t, utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath))

	configuration, lastConfigBlock, err := top_config.ReadConfig(
		nodeConfigPath,
		testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigSource%d", partyID), zap.DebugLevel),
	)
	require.NoError(t, err)

	assemblerNodeConfig := configuration.ExtractAssemblerConfig(lastConfigBlock)
	require.NotNil(t, assemblerNodeConfig)

	// Replace batcher/consenter endpoints with in-test stubs.
	assemblerNodeConfig.Shards = shards
	assemblerNodeConfig.Consenter = consenterInfo

	_, signer, err := testutil.BuildTestLocalMSP(
		configuration.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir,
		configuration.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID,
	)
	require.NoError(t, err)

	asm := assembler.NewAssembler(
		assemblerNodeConfig, configuration, lastConfigBlock,
		make(chan struct{}),
		testutil.CreateLogger(t, int(partyID)),
		signer,
	)
	asm.StartAssemblerService()
	// lastConfigBlock is the armageddon genesis block; return it so callers can initialise the
	// OBA creator with the correct header hash.
	return asm, lastConfigBlock
}

// createJoiningAssembler creates an assembler for partyID with an empty ledger that must sync
// from running source assemblers before it can start. syncConfigBlock must be a real config block
// (Number > 0) whose data contains a valid config envelope so the BFT synchronizer can extract
// peer addresses and TLS credentials.
// Callers should call StartAssemblerService() on the returned assembler.
func createJoiningAssembler(
	t *testing.T,
	partyID types.PartyID,
	dir string,
	shards []config.ShardInfo,
	consenterInfo config.ConsenterInfo,
	syncConfigBlock *cb.Block,
) *assembler.Assembler {
	t.Helper()

	nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "local_config_assembler.yaml")

	// Use a brand-new, empty ledger directory so config.ReadConfig falls back to the
	// bootstrap genesis block (not the source assembler's populated ledger).
	joiningLedgerDir := t.TempDir()
	localConfig, _, err := top_config.LoadLocalConfig(nodeConfigPath)
	require.NoError(t, err)
	localConfig.NodeLocalConfig.FileStore.Path = joiningLedgerDir
	require.NoError(t, utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath))

	configuration, lastConfigBlock, err := top_config.ReadConfig(
		nodeConfigPath,
		testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigJoining%d", partyID), zap.DebugLevel),
	)
	require.NoError(t, err)

	assemblerNodeConfig := configuration.ExtractAssemblerConfig(lastConfigBlock)
	require.NotNil(t, assemblerNodeConfig)

	// Fresh listen address so we don't conflict with the source assembler for the same party.
	assemblerNodeConfig.ListenAddress = testutil.AllocateLocalhostAddress(t)

	// Use in-test stubs for batch/consenter after sync.
	assemblerNodeConfig.Shards = shards
	assemblerNodeConfig.Consenter = consenterInfo

	_, signer, err := testutil.BuildTestLocalMSP(
		configuration.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir,
		configuration.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID,
	)
	require.NoError(t, err)

	// syncConfigBlock.Number > 0 causes initLedger to enter the sync path. The block must carry
	// a real config envelope so the BFT synchronizer can extract assembler addresses and TLS creds.
	return assembler.NewAssembler(
		assemblerNodeConfig, configuration, syncConfigBlock,
		make(chan struct{}),
		testutil.CreateLogger(t, int(partyID)),
		signer,
	)
}

// bundleFromBlock creates a channelconfig.Resources bundle directly from a config block,
// without opening any ledger database.
func bundleFromBlock(t *testing.T, configBlock *cb.Block) channelconfig.Resources {
	t.Helper()
	envelope, err := protoutil.ExtractEnvelope(configBlock, 0)
	require.NoError(t, err)
	bundle, err := channelconfig.NewBundleFromEnvelope(envelope, factory.GetDefault())
	require.NoError(t, err)
	return bundle
}
