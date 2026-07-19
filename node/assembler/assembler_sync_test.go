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
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Scenario:
//  1. Build a 4-party network with stub batchers/consenters and 4 source assemblers.
//  2. Commit 5 regular blocks in every source assembler.
//  3. Add party 5 via a config update; the config is delivered to all source consenters.
//  4. Start a joining assembler for party 5 with an empty ledger. Because the join config block
//     number > 0, initLedger runs the real AssemblerBFTSynchronizer, which fetches all blocks
//     (genesis, the 5 data blocks and the config block) from the running source assemblers.
//  5. Verify the joining assembler's TxCount matches the sources after sync.
//  6. Commit a couple more blocks in the joining assembler and verify it keeps committing them,
//     i.e. it continues normally once sync has finished.
func TestAssemblerSync_RealSynchronizer(t *testing.T) {
	s := newSyncTestSetup(t, 4)

	s.commitDataBlocks(5)
	addedPartyID, configBlock := s.addParty()
	s.startJoining(addedPartyID, configBlock)

	s.waitJoiningTxCount(s.txCount, "joining assembler should sync all blocks including config block")

	// After sync, the joining assembler must continue processing new decisions and batches.
	s.commitJoiningDataBlocks(2)
	s.waitJoiningTxCount(s.txCount+2, "joining assembler should keep committing blocks after sync")
}

// Scenario:
//  1. Build a 4-party network and start 4 source assemblers.
//  2. Skip regular blocks - commit no data blocks before the config update.
//  3. Add party 5 via a config update at block 1 (genesis is block 0).
//  4. Start a joining assembler for party 5 with an empty ledger and verify it syncs
//     genesis block + config block (TxCount == 2), then continues normally.
func TestAssemblerSync_NoRegularBlocks(t *testing.T) {
	s := newSyncTestSetup(t, 4)

	// Config block is at number 1: no regular blocks precede it, so prevHash comes from genesis.
	addedPartyID, configBlock := s.addParty()
	s.startJoining(addedPartyID, configBlock)

	// genesis + config block.
	s.waitJoiningTxCount(s.txCount, "joining assembler should sync genesis block and config block")

	s.commitJoiningDataBlocks(2)
	s.waitJoiningTxCount(s.txCount+2, "joining assembler should keep committing blocks after sync")
}

// Scenario:
//  1. Build a 4-party network and start 4 source assemblers.
//  2. Commit 3 regular blocks and a config update (same as TestAssemblerSync_RealSynchronizer).
//  3. Stop one source assembler (party 1) before the joining assembler begins sync.
//  4. Verify that the joining assembler still completes sync using the remaining 3 sources.
//     For a 4-party BFT network (f=1) this is the minimum fault-tolerant scenario.
//  5. Verify the joining assembler continues normally after sync.
func TestAssemblerSync_SomeSourcesUnavailable(t *testing.T) {
	s := newSyncTestSetup(t, 4)

	s.commitDataBlocks(3)
	addedPartyID, configBlock := s.addParty()

	// Stop party 1's assembler. The joining assembler must sync using only parties 2-4 (3/4 sources).
	// For a 4-party BFT network with f=1 this is the minimum number of correct replicas.
	// Stop() is idempotent, so the deferred Stop() at cleanup is a safe no-op.
	s.sourceAssemblers[0].Stop()

	s.startJoining(addedPartyID, configBlock)

	s.waitJoiningTxCount(s.txCount, "joining assembler should sync despite one unavailable source")

	// The joining assembler is independent of the stopped source, so it keeps committing.
	s.commitJoiningDataBlocks(2)
	s.waitJoiningTxCount(s.txCount+2, "joining assembler should keep committing blocks after sync")
}

// Scenario (catch-up from a non-empty ledger):
//  1. Build a 4-party network and commit 5 regular blocks in the sources.
//  2. Add party 5 via a config update, so the config block is number 6.
//  3. Pre-seed the joining assembler's ledger with the genesis block + all 5 data blocks (height 6),
//     so its ledger is non-empty and its top block is a data block.
//  4. Start the joining assembler. Because the join config block number (6) equals the ledger height
//     (6), initLedger takes the sync path (this is the boundary the assembler.go `>=` check covers),
//     and the synchronizer skips the genesis-bootstrap phase (startHeight > 0) and pulls only the
//     remaining config block.
//  5. Verify the joining assembler reaches the same TxCount as the sources and then keeps committing.
func TestAssemblerSync_CatchUpFromNonEmptyLedger(t *testing.T) {
	s := newSyncTestSetup(t, 4)

	s.commitDataBlocks(5)

	// Pre-seed before adding the party: the ledger prefix is genesis + the 5 data blocks (height 6).
	joiningLedgerDir := s.preSeedJoiningLedger()

	addedPartyID, configBlock := s.addParty() // config block at number 6 == pre-seeded ledger height.
	s.startJoiningWithLedger(addedPartyID, configBlock, joiningLedgerDir)

	// Each block contains one transaction, so the source ledger height equals
	// s.txCount. Assert on height (not TxCount) because the pre-seeded prefix bypasses the counter.
	s.waitJoiningHeight(s.txCount, "joining assembler should catch up the config block from a non-empty ledger")

	s.commitJoiningDataBlocks(2)
	s.waitJoiningHeight(s.txCount+2, "joining assembler should keep committing blocks after catch-up")
}

// Scenario (too few sources to sync):
//  1. Build a 4-party network, commit 3 regular blocks, and add party 5.
//  2. Stop 3 of the 4 source assemblers, leaving only 1 available - fewer than the f+1 (=2) matching
//     genesis blocks the synchronizer requires for a 5-node cluster (f=1).
//  3. Verify the stopped sources report StateStopped and the remaining one is still StateRunning.
//  4. Verify that building the joining assembler panics: sync cannot fetch an agreed-upon genesis
//     block, so it fails rather than silently starting with an incomplete ledger.
func TestAssemblerSync_TooManySourcesUnavailable(t *testing.T) {
	s := newSyncTestSetup(t, 4)

	s.commitDataBlocks(3)
	addedPartyID, configBlock := s.addParty()

	// Leave only party 4's assembler running (1 of 4 < f+1 = 2 required matching genesis blocks).
	for i := 0; i < 3; i++ {
		s.sourceAssemblers[i].Stop()
	}

	// The three stopped sources must report StateStopped, and party 4 must still be running.
	for i := 0; i < 3; i++ {
		asm := s.sourceAssemblers[i]
		require.Eventually(t, func() bool {
			return asm.GetStatus().State == node_utils.StateStopped
		}, 20*time.Second, 100*time.Millisecond, "stopped source assembler should report StateStopped")
	}

	require.Equal(t, s.sourceAssemblers[3].GetStatus().State, node_utils.StateRunning, "the remaining source assembler should still be running")

	joiningShards, joiningConsenterInfo := s.prepareJoining(addedPartyID)
	require.Panics(t, func() {
		createJoiningAssembler(t, addedPartyID, s.dir, joiningShards, joiningConsenterInfo, configBlock, "")
	}, "sync should fail when fewer than f+1 sources are available to agree on the genesis block")
}

// dataBlockRecord captures a regular block committed in the assemblers: the batch and the ordered
// batch attestation. Together they reproduce the exact block the sources committed, so a joining
// node's ledger can be pre-seeded with the same prefix.
type dataBlockRecord struct {
	batch types.Batch
	oba   *state.AvailableBatchOrdered
}

// syncTestSetup holds all the parts of an assembler-sync test: the generated network,
// the in-test stub batchers/consenters, the source assemblers, and (once started) the joining
// assembler with its own stub consenter/batcher. It also tracks the hash chain / block numbering
// so successive helper calls produce a consistent ledger.
type syncTestSetup struct {
	t             *testing.T
	dir           string
	numParties    int
	ca            tlsgen.CA
	shardID       types.ShardID
	armaNodesInfo testutil.ArmaNodesInfoMap

	batchersStub []*stubBatcher
	batcherInfos []config.BatcherInfo
	shards       []config.ShardInfo

	sourceAssemblers []*assembler.Assembler
	sourceConsenters []*stubConsenter

	genesisBlock *cb.Block
	obaCreator   *OrderedBatchAttestationCreator

	// dataBlocks records every regular block committed in the sources, so a joining node's
	// ledger can be pre-seeded with an identical prefix (see preSeedJoiningLedger).
	dataBlocks []dataBlockRecord

	// Block numbering / expected-count bookkeeping.
	nextSeq int    // sequence for the next data batch
	nextNum uint64 // number (and decision number) for the next block
	txCount uint64 // expected TxCount across the source assemblers

	// Joining node (populated by startJoining).
	joiningPartyID   types.PartyID
	joiningConsenter *stubConsenter
	joiningBatcher   *stubBatcher
	joiningAsm       *assembler.Assembler
}

// newSyncTestSetup builds a numParties network, starts stub batchers/consenters and source
// assemblers, waits for them to append the genesis block, and seeds the OBA creator from the
// genesis hash chain. All started components are stopped via t.Cleanup.
func newSyncTestSetup(t *testing.T, numParties int) *syncTestSetup {
	t.Helper()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	armaNodesInfo := testutil.CreateNetwork(t, configPath, numParties, 1, "TLS", "TLS")
	require.NotNil(t, armaNodesInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	shardID := types.ShardID(1)
	batchersStub, batcherInfos, batcherCleanup := createStubBatchersAndInfos(t, numParties, shardID, ca)
	t.Cleanup(batcherCleanup)
	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	sourceAssemblers := make([]*assembler.Assembler, numParties)
	sourceConsenters := make([]*stubConsenter, numParties)
	var genesisBlock *cb.Block
	for i := 0; i < numParties; i++ {
		partyID := types.PartyID(i + 1)
		consenter := NewStubConsenter(t, partyID, ca)
		t.Cleanup(consenter.Stop)
		sourceConsenters[i] = consenter
		asm, gb := createSourceAssemblerWithStubs(t, partyID, dir, armaNodesInfo, shards, consenter.consenterInfo)
		t.Cleanup(asm.Stop)
		sourceAssemblers[i] = asm
		if i == 0 {
			genesisBlock = gb
		}
	}

	s := &syncTestSetup{
		t:                t,
		dir:              dir,
		numParties:       numParties,
		ca:               ca,
		shardID:          shardID,
		armaNodesInfo:    armaNodesInfo,
		batchersStub:     batchersStub,
		batcherInfos:     batcherInfos,
		shards:           shards,
		sourceAssemblers: sourceAssemblers,
		sourceConsenters: sourceConsenters,
		genesisBlock:     genesisBlock,
		nextSeq:          0,
		nextNum:          1,
		txCount:          1, // genesis block
	}

	s.waitSourcesTxCount(s.txCount, "source assemblers should append genesis block")

	obaCreator, _ := NewOrderedBatchAttestationCreatorWithGenesis(genesisBlock)
	s.obaCreator = obaCreator

	return s
}

// commitDataBlocks commits n regular blocks in every source assembler and waits for them to
// be committed. Each block carries a single batched request from party 1's batcher.
func (s *syncTestSetup) commitDataBlocks(n int) {
	s.t.Helper()
	for i := 0; i < n; i++ {
		batch := types.NewSimpleBatch(s.shardID, 1, types.BatchSequence(s.nextSeq),
			types.BatchedRequests{[]byte{byte(s.nextSeq)}}, 0, nil)
		s.batchersStub[0].SetNextBatch(batch)
		oba := s.obaCreator.Append(batch, types.DecisionNum(s.nextNum), 0, 1)
		for _, c := range s.sourceConsenters {
			c.SetNextDecision(oba)
		}
		for _, c := range s.sourceConsenters {
			<-c.decisionSentCh
		}
		s.dataBlocks = append(s.dataBlocks, dataBlockRecord{batch: batch, oba: oba})
		s.nextSeq++
		s.nextNum++
		s.txCount++
	}
	s.waitSourcesTxCount(s.txCount, "source assemblers should append data blocks")
}

// addParty builds a config block that adds a new party (via PrepareAndAddNewParty), delivers it
// to all source consenters, waits for the sources to apply it and restart with the new config,
// and returns the new party's ID together with the config block. The config block is chained from
// the last block the OBA creator produced (or genesis, when no data blocks were committed).
func (s *syncTestSetup) addParty() (types.PartyID, *cb.Block) {
	t := s.t
	t.Helper()

	bootstrapBlockPath := filepath.Join(s.dir, "bootstrap", "bootstrap.block")
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, s.dir, bootstrapBlockPath)
	addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, s.dir)
	for _, info := range addedNetInfo {
		if info != nil && info.Listener != nil {
			info.Listener.Close()
		}
	}

	configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
	signingParties := make([]types.PartyID, s.numParties)
	for i := range signingParties {
		signingParties[i] = types.PartyID(i + 1)
	}

	bundle := bundleFromBlock(t, s.genesisBlock)
	configUpdateEnv := cfgutil.CreateConfigTX(t, s.dir, signingParties, 1, configUpdatePbData)
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

	configBlockNum := s.nextNum
	configDecisionNum := types.DecisionNum(s.nextNum)
	configBlock, err := consensus.CreateConfigCommonBlock(configBlockNum, s.obaCreator.CurrentHeaderHash(), 0, configDecisionNum, 1, 0, configReq)
	require.NoError(t, err)

	configBA := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(1, types.ShardIDConsensus, 1, nil),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: configBlock,
			DecisionNum: configDecisionNum,
		},
	}
	st := &state.State{
		N:      uint16(s.numParties + 1),
		Shards: []state.ShardTerm{{Shard: s.shardID, Term: 0}},
	}
	for _, c := range s.sourceConsenters {
		c.SetNextConfigDecision(configBA, st)
	}
	for _, c := range s.sourceConsenters {
		<-c.decisionSentCh
	}

	s.nextNum++
	s.txCount++ // config TX

	s.waitSourcesTxCount(s.txCount, "source assemblers should append the config block")
	s.waitSourcesRunningWithConfig(1)

	return addedPartyID, configBlock
}

// prepareJoining creates the joining party's own stub batcher and consenter (registering their
// stops via t.Cleanup) and records them on the setup. It returns the shard info (existing batchers
// plus the joining batcher) and consenter info used to build the joining assembler.
func (s *syncTestSetup) prepareJoining(partyID types.PartyID) ([]config.ShardInfo, config.ConsenterInfo) {
	t := s.t
	t.Helper()

	numExistingParties := len(s.batcherInfos)
	allParties := make([]types.PartyID, numExistingParties+1)
	for i := range allParties {
		allParties[i] = types.PartyID(i + 1)
	}

	joiningBatcher := NewStubBatcher(t, s.shardID, partyID, allParties, s.ca)
	t.Cleanup(joiningBatcher.Stop)

	joiningShards := []config.ShardInfo{{ShardId: s.shardID, Batchers: append(s.batcherInfos, joiningBatcher.batcherInfo)}}
	joiningConsenter := NewStubConsenter(t, partyID, s.ca)
	t.Cleanup(joiningConsenter.Stop)

	s.joiningPartyID = partyID
	s.joiningConsenter = joiningConsenter
	s.joiningBatcher = joiningBatcher

	return joiningShards, joiningConsenter.consenterInfo
}

// startJoining creates and starts a joining assembler for the given party with an empty ledger
// (so it syncs from height 0), and records it on the setup.
func (s *syncTestSetup) startJoining(partyID types.PartyID, syncConfigBlock *cb.Block) {
	s.startJoiningWithLedger(partyID, syncConfigBlock, "")
}

// startJoiningWithLedger creates and starts a joining assembler whose ledger lives at ledgerDir
// (empty string means a fresh empty ledger). Pre-seeding ledgerDir lets the joining node start at
// a height > 0, exercising the catch-up (non-genesis-bootstrap) path of the synchronizer. The OBA
// creator is advanced past the join config block so blocks committed after sync chain from it.
func (s *syncTestSetup) startJoiningWithLedger(partyID types.PartyID, syncConfigBlock *cb.Block, ledgerDir string) {
	t := s.t
	t.Helper()

	joiningShards, joiningConsenterInfo := s.prepareJoining(partyID)

	joiningAsm := createJoiningAssembler(t, partyID, s.dir, joiningShards, joiningConsenterInfo, syncConfigBlock, ledgerDir)
	joiningAsm.StartAssemblerService()
	t.Cleanup(joiningAsm.Stop)

	s.joiningAsm = joiningAsm

	// Post-sync blocks must chain from the join config block, which was built outside the OBA
	// creator via CreateConfigCommonBlock.
	s.obaCreator.AdvancePastConfigBlock(syncConfigBlock)
}

// preSeedJoiningLedger builds a fresh ledger directory populated with the genesis block and every
// data block committed so far, reproducing the source ledgers' prefix, and returns its path. Because
// the OBA creator does not set LAST_CONFIG metadata (real consensus does, via CreateDataCommonBlock),
// it is added here pointing at the genesis block: the BFT deliverer reads LAST_CONFIG off the
// ledger's top block when starting from a non-empty ledger. Metadata is not part of the header
// hash, so the block-to-block linkage the sources produced is preserved.
func (s *syncTestSetup) preSeedJoiningLedger() string {
	t := s.t
	t.Helper()

	ledgerDir := t.TempDir()
	lg := testutil.CreateLoggerForModule(t, "PreSeedJoiningLedger", zap.InfoLevel)
	led, err := node_ledger.NewAssemblerLedger(lg, ledgerDir)
	require.NoError(t, err)
	defer led.Close()

	// Write blocks directly through the underlying ledger: the AssemblerLedger's metrics are wired
	// up by the assembler (NewMetrics), not by NewAssemblerLedger, so AppendBlock would nil-deref.

	// Block 0: the genesis config block.
	require.NoError(t, led.Ledger.Append(s.genesisBlock))

	// Blocks 1..N: the recorded data blocks with the same headers the sources committed.
	for _, rec := range s.dataBlocks {
		block := &cb.Block{
			Header:   rec.oba.OrderingInformation.CommonBlock.Header,
			Data:     &cb.BlockData{Data: rec.batch.Requests()},
			Metadata: rec.oba.OrderingInformation.CommonBlock.Metadata,
		}
		if block.Metadata == nil {
			protoutil.InitBlockMetadata(block)
		}
		block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&cb.Metadata{
			Value: protoutil.MarshalOrPanic(&cb.LastConfig{Index: 0}),
		})
		require.NoError(t, led.Ledger.Append(block))
	}

	return ledgerDir
}

// commitJoiningDataBlocks commits n regular blocks in the joining assembler only (via its own
// stub consenter and a batch from party 1's batcher, which is part of the joining shard). It does
// not commit blocks in the source assemblers, so only the joining assembler's TxCount advances.
func (s *syncTestSetup) commitJoiningDataBlocks(n int) {
	s.t.Helper()
	require.NotNil(s.t, s.joiningConsenter, "startJoining must be called before commitJoiningDataBlocks")
	for i := 0; i < n; i++ {
		batch := types.NewSimpleBatch(s.shardID, 1, types.BatchSequence(s.nextSeq),
			types.BatchedRequests{[]byte{byte(s.nextSeq)}}, 0, nil)
		s.batchersStub[0].SetNextBatch(batch)
		oba := s.obaCreator.Append(batch, types.DecisionNum(s.nextNum), 0, 1)
		s.joiningConsenter.SetNextDecision(oba)
		<-s.joiningConsenter.decisionSentCh
		s.nextSeq++
		s.nextNum++
	}
}

// waitSourcesTxCount waits until every source assembler reaches the expected TxCount.
func (s *syncTestSetup) waitSourcesTxCount(count uint64, msg string) {
	s.t.Helper()
	for _, asm := range s.sourceAssemblers {
		require.Eventually(s.t, func() bool {
			return asm.GetTxCount() == count
		}, 20*time.Second, 100*time.Millisecond, msg)
	}
}

// waitJoiningTxCount waits until the joining assembler reaches the expected TxCount.
func (s *syncTestSetup) waitJoiningTxCount(count uint64, msg string) {
	s.t.Helper()
	require.Eventually(s.t, func() bool {
		return s.joiningAsm.GetTxCount() == count
	}, 60*time.Second, 100*time.Millisecond, msg)
}

// waitJoiningHeight waits until the joining assembler's ledger reaches the expected height. This is
// used instead of TxCount when the ledger was pre-seeded, since GetTxCount only reflects blocks
// appended through the running assembler's ledger instance, not a pre-seeded prefix.
func (s *syncTestSetup) waitJoiningHeight(height uint64, msg string) {
	s.t.Helper()
	require.Eventually(s.t, func() bool {
		return s.joiningAsm.LedgerHeight() == height
	}, 60*time.Second, 100*time.Millisecond, msg)
}

// waitSourcesRunningWithConfig waits until all source assemblers are back in StateRunning with the
// expected config sequence number. The gRPC Deliver server is briefly unavailable between
// net.Stop() and the new StartAssemblerService() during the restart triggered by
// ProcessNewConfigBlock; the joining assembler's sync would fail if it starts mid-restart.
func (s *syncTestSetup) waitSourcesRunningWithConfig(configSeq uint64) {
	s.t.Helper()
	for _, asm := range s.sourceAssemblers {
		require.Eventually(s.t, func() bool {
			status := asm.GetStatus()
			return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == configSeq
		}, 20*time.Second, 100*time.Millisecond, "source assembler should restart with new config")
	}
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
	ledgerDir string,
) *assembler.Assembler {
	t.Helper()

	nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "local_config_assembler.yaml")

	// Use the provided ledger directory, or a brand-new empty one so config.ReadConfig falls back
	// to the bootstrap genesis block (not the source assembler's populated ledger). A pre-seeded
	// directory lets the joining node start at height > 0 and exercise the catch-up sync path.
	joiningLedgerDir := ledgerDir
	if joiningLedgerDir == "" {
		joiningLedgerDir = t.TempDir()
	}
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
