/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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

// Scenario: evict the shard primary, then add a new party, verifying the shard keeps ordering txs across both
// reconfigurations. Each reconfiguration changes the party count, so consensus bumps the shard term, which in
// turn moves the primary (primary index = (shardID + term) % N over the parties sorted by ID).
//  1. Create 4 parties (one shard) and verify all batchers run at config sequence 0 (term 0, primary party 2).
//  2. Evict the shard primary (party 2): the evicted batcher reaches pending admin, the survivors reconfigure to
//     config sequence 1, the term bumps to 1 so they elect party 4 as primary, and they batch and replicate a tx.
//  3. Add a new party (party 5) via a config block chained on the post-eviction config: the survivors reconfigure
//     to config sequence 2 and their configuration now includes party 5.
//  4. Bring up the added party's batcher so it joins the shard. The term bumps to 2, so all parties elect the
//     added party (party 5) as the new primary.
//  5. Verify that all parties in the new configuration (1, 3, 4, 5) batch and replicate a normal tx.
func TestBatcherReconfigPrimaryEvictionAndAddParty(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4}
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

	// make sure all batchers are running with the initial config sequence 0
	for j := range parties {
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// the shard primary is batchers[(shardID + term) % N] once sorted by party ID; for shard 1, term 0, N 4 this is party 2
	partyToRemove := types.PartyID(2)

	// create config block that evicts the shard primary
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
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

	// wait for the evicted batcher to reach pending admin state, and for the surviving batchers to reconfigure
	// and return to running state with the new config sequence
	for j := range parties {
		partyID := parties[j]
		if partyID == partyToRemove {
			require.Eventually(t, func() bool {
				return batchers[j].GetStatus().GetState() == node_utils.StatePendingAdmin
			}, 60*time.Second, 10*time.Millisecond)
			continue
		}
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(1)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// because the party count changed, consensus bumps the shard term (see DefaultConfigApplier.ApplyConfigToState).
	// After a reconfigured batcher restarts it waits for a fresh state, so deliver the post-reconfig state (N 3,
	// term 1) to the surviving batchers.
	survivingParties := []types.PartyID{1, 3, 4}
	stEvicted := &state.State{N: uint16(len(survivingParties)), Shards: []state.ShardTerm{{Shard: 1, Term: 1}}}
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		stubConsenters[j].UpdateState(stEvicted)
	}

	// with the bumped term the surviving batchers (sorted by party ID: [1, 3, 4]) elect a new primary:
	// batchers[(shardID + term) % N] = batchers[(1 + 1) % 3] = party 4
	newPrimary := types.PartyID(4)
	for j := range parties {
		partyID := parties[j]
		if partyID == partyToRemove {
			continue
		}
		require.Eventually(t, func() bool {
			return batchers[j].GetPrimaryID() == newPrimary
		}, 60*time.Second, 10*time.Millisecond)
	}

	// verify that the reconfigured shard can handle a normal tx: submit a request to the new primary and wait for
	// the surviving batchers to batch and replicate it into their ledgers.
	newPrimaryIdx := indexOfParty(parties, newPrimary)
	routerCtx := routerContextForParty(t, dir, newPrimary)

	// batches produced by the new primary are stored in the ledger part keyed by its party ID, which starts empty
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		require.Equal(t, uint64(0), batchers[j].Ledger.Height(newPrimary))
	}

	// the request must carry the batcher's current config sequence (1 after the reconfig), otherwise the batcher's
	// request verifier rejects it with a config sequence mismatch
	req := tx.CreateStructuredRequest([]byte{42})
	req.ConfigSeq = 1
	resp, err := batchers[newPrimaryIdx].Submit(routerCtx, req)
	require.NoError(t, err)
	require.Empty(t, resp.Error)

	// after batching and replicating the tx, each surviving batcher holds a single batch from the new primary
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		require.Eventually(t, func() bool {
			return batchers[j].Ledger.Height(newPrimary) == uint64(1)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// verify that the evicted party can no longer forward transactions to the primary: when the new primary
	// reconfigured it rebuilt its TLS trust from the new configuration, which no longer includes the evicted
	// party's CA, so the evicted party's connection is rejected at the TLS handshake and never reaches the
	// primary's request handling. Build a request connector using the evicted party's identity (its config still
	// carries the pre-eviction batcher endpoints and certificates), point it at the new primary, and confirm the
	// primary's ledger does not advance.
	evictedIdx := indexOfParty(parties, partyToRemove)
	evictedConfig := batchers[evictedIdx].GetConfig()
	evictedConnector := batcher.CreatePrimaryReqConnector(newPrimary, testutil.CreateLogger(t, int(partyToRemove)), evictedConfig, batcher.GetBatchersEndpointsAndCerts(evictedConfig.Shards[0].Batchers), context.Background(), 2*time.Second, 100*time.Millisecond, 500*time.Millisecond)
	evictedConnector.ConnectToPrimary()
	defer evictedConnector.Stop()

	evictedReq := tx.CreateStructuredRequest([]byte{99})
	evictedReq.ConfigSeq = 1
	evictedRawReq, err := proto.Marshal(evictedReq)
	require.NoError(t, err)
	evictedConnector.SendReq(evictedRawReq)

	// the primary's ledger must stay at the single batch created above; the forwarded request from the evicted
	// party is rejected and never batched
	require.Never(t, func() bool {
		return batchers[newPrimaryIdx].Ledger.Height(newPrimary) != uint64(1)
	}, 5*time.Second, 200*time.Millisecond)

	// second reconfig: add a new party (party 5) to the shard. The add-party config update must be chained on top
	// of the post-eviction configuration (parties 1, 3, 4), so base a new config update builder on the eviction
	// config block and derive its bundle for building the next config block.
	evictionBundle := bundleFromBlock(t, configBlock)
	evictionBlockPath := filepath.Join(dir, "eviction_config.block")
	evictionBlockBytes, err := proto.Marshal(configBlock)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(evictionBlockPath, evictionBlockBytes, 0o644))

	addPartyBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, evictionBlockPath)
	addedPartyID, addedNetInfo := addPartyBuilder.PrepareAndAddNewParty(t, dir)
	// free the ports reserved for the added party's nodes so its batcher can bind them
	for _, info := range addedNetInfo {
		if info != nil && info.Listener != nil {
			info.Listener.Close()
		}
	}
	require.Equal(t, types.PartyID(5), addedPartyID)

	addPartyPbData := addPartyBuilder.ConfigUpdatePBData(t)
	require.NotNil(t, addPartyPbData)

	// the surviving parties (1, 3, 4) sign the config update to reach majority
	addPartyEnvelope := cfgutil.CreateConfigTX(t, dir, survivingParties, 1, addPartyPbData)
	addPartyBlock, err := cfgutil.CreateConsensusConfigBlock(evictionBundle, addPartyEnvelope, configBlock.Header, 1, types.DecisionNum(2), 1, 0)
	require.NoError(t, err)

	// all parties in the new configuration (1, 3, 4, 5) participate in the shard
	allParties := []types.PartyID{1, 3, 4, 5}

	// deliver the add-party config block to the surviving batchers' stub consenters. The state bundled with a
	// config block is not used to drive the term (a reconfigured batcher restarts from a zero state), so the term
	// carried here is irrelevant; the post-reconfig term is delivered explicitly below.
	stAddConfig := &state.State{N: uint16(len(allParties)), Shards: []state.ShardTerm{{Shard: 1, Term: 0}}}
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		stubConsenters[j].UpdateStateHeaderWithConfigBlock(types.DecisionNum(2), []*common.Block{addPartyBlock}, stAddConfig)
	}

	// wait for the surviving batchers to append the add-party config block and reconfigure to config sequence 2
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		require.Eventually(t, func() bool {
			block, err1 := batchers[j].ConfigStore.Last()
			blockNumbers, err2 := batchers[j].ConfigStore.ListBlockNumbers()
			return err1 == nil && err2 == nil && block.Header.Number == uint64(2) && len(blockNumbers) == 3
		}, 60*time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(2)
		}, 60*time.Second, 10*time.Millisecond)

		// the surviving batchers' shard configuration now includes the added party 5
		require.True(t, shardContainsParty(batchers[j].GetConfig(), types.ShardID(1), addedPartyID))
	}

	// rebuild the surviving consenters' TLS trust so that they accept the added party's connections. The stub
	// consenters of parties 1, 3, 4 were created before party 5 existed, so their gRPC servers (which require
	// client certificates) do not trust party 5's TLS certificate. Restart them trusting the client root CAs of
	// the new configuration, so the added party's batcher can reach a consenter quorum when sending BAFs.
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		clientRootCAs := consenterClientRootCAs(t, dir, parties[j], addPartyBlock)
		stubConsenters[j].RestartWithClientRootCAs(clientRootCAs)
	}

	// bring up the added party's batcher so it can join the shard. It bootstraps from the add-party config block
	// (which contains parties 1, 3, 4, 5), so redirect its file stores and point its bootstrap file at that block.
	addedParties := []types.PartyID{addedPartyID}
	addPartyBlockPath := filepath.Join(dir, "add_party_config.block")
	addPartyBlockBytes, err := proto.Marshal(addPartyBlock)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(addPartyBlockPath, addPartyBlockBytes, 0o644))
	updateFileStorePath(t, dir, addedParties, numOfShards)
	prepareAddedPartyConfig(t, dir, addedPartyID, numOfShards, addPartyBlockPath)

	addedStubConsenters := createStubConsenters(t, dir, addedParties)
	addedBatchers, _, _ := createBatcherNodes(t, dir, addedParties, numOfShards, addedStubConsenters)
	startBatcherNodes(addedBatchers)
	addedBatcher := addedBatchers[0]

	defer func() {
		for _, sc := range addedStubConsenters {
			sc.StopNet()
		}
		addedBatcher.Stop()
	}()

	// map every party in the new configuration to its batcher
	allBatchers := map[types.PartyID]*batcher.Batcher{addedPartyID: addedBatcher}
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		allBatchers[parties[j]] = batchers[j]
	}

	// deliver the post-add-party state to all parties. Adding a party changes the party count, so consensus bumps
	// the shard term again: it is now 2. With N 4 and term 2 the parties (sorted by party ID: [1, 3, 4, 5]) elect
	// batchers[(shardID + term) % N] = batchers[(1 + 2) % 4] = the added party (party 5) as the new primary.
	stAdd := &state.State{N: uint16(len(allParties)), Shards: []state.ShardTerm{{Shard: 1, Term: 2}}}
	for j := range parties {
		if parties[j] == partyToRemove {
			continue
		}
		stubConsenters[j].UpdateState(stAdd)
	}
	addedStubConsenters[0].UpdateState(stAdd)
	newPrimaryAfterAdd := addedPartyID

	// wait for the added party's batcher to run with config sequence 2 and for all parties to elect the new primary
	require.Eventually(t, func() bool {
		status := addedBatcher.GetStatus()
		return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(2)
	}, 60*time.Second, 10*time.Millisecond)
	for _, partyID := range allParties {
		b := allBatchers[partyID]
		require.Eventually(t, func() bool {
			return b.GetPrimaryID() == newPrimaryAfterAdd
		}, 60*time.Second, 10*time.Millisecond)
	}

	// submit another normal tx under the new configuration and verify that all parties in it (1, 3, 4, 5) batch
	// and replicate it into their ledgers. The added party is now the primary, so it batches the tx and the others
	// pull and replicate it.
	routerCtx2 := routerContextForParty(t, dir, newPrimaryAfterAdd)
	req2 := tx.CreateStructuredRequest([]byte{43})
	req2.ConfigSeq = 2
	resp2, err := allBatchers[newPrimaryAfterAdd].Submit(routerCtx2, req2)
	require.NoError(t, err)
	require.Empty(t, resp2.Error)

	for _, partyID := range allParties {
		b := allBatchers[partyID]
		require.Eventually(t, func() bool {
			return b.Ledger.Height(newPrimaryAfterAdd) == uint64(1)
		}, 60*time.Second, 10*time.Millisecond)
	}
}

// prepareAddedPartyConfig points the bootstrap file of the added party's batchers and consenter at blockPath (so
// the node bootstraps from the add-party config block instead of the original genesis block) and disables client
// signature verification, matching the other parties (created with TLS "none") so that the test's dummy-signed
// requests pass the batcher's request verifier.
func prepareAddedPartyConfig(t *testing.T, dir string, partyID types.PartyID, numOfShards int, blockPath string) {
	configLogger := testutil.CreateLoggerForModule(t, "PrepareAddedPartyConfig", zap.DebugLevel)

	nodeConfigPaths := []string{filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "local_config_consenter.yaml")}
	for j := 1; j <= numOfShards; j++ {
		nodeConfigPaths = append(nodeConfigPaths, filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), fmt.Sprintf("local_config_batcher%d.yaml", j)))
	}

	for _, nodeConfigPath := range nodeConfigPaths {
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath, configLogger)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File = blockPath
		localConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = false
		require.NoError(t, utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath))
	}
}

// shardContainsParty reports whether the batcher configuration lists a batcher for partyID in the given shard.
func shardContainsParty(conf *node_config.BatcherNodeConfig, shardID types.ShardID, partyID types.PartyID) bool {
	for _, shard := range conf.Shards {
		if shard.ShardId != shardID {
			continue
		}
		for _, b := range shard.Batchers {
			if b.PartyID == partyID {
				return true
			}
		}
	}
	return false
}

// indexOfParty returns the index of partyID within parties.
func indexOfParty(parties []types.PartyID, partyID types.PartyID) int {
	for i := range parties {
		if parties[i] == partyID {
			return i
		}
	}
	return -1
}

// routerContextForParty builds a context that carries the router TLS certificate of the given party, so that
// a request submitted through it passes the batcher's router authentication.
func routerContextForParty(t *testing.T, dir string, partyID types.PartyID) context.Context {
	nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "local_config_router.yaml")
	localConfig, _, err := config.LoadLocalConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("LoadLocalConfigRouter%d", partyID), zap.DebugLevel))
	require.NoError(t, err)
	return testutil.ContextWithClientTLSCert(t, localConfig.TLSConfig.Certificate)
}

// bundleFromBlock builds a channelconfig.Resources bundle directly from a config block, so a subsequent config
// update can be validated and chained on top of it.
func bundleFromBlock(t *testing.T, configBlock *common.Block) channelconfig.Resources {
	envelope, err := protoutil.ExtractEnvelope(configBlock, 0)
	require.NoError(t, err)
	bundle, err := channelconfig.NewBundleFromEnvelope(envelope, factory.GetDefault())
	require.NoError(t, err)
	return bundle
}

// Scenario:
//  1. Create config and crypto material for 4 parties, one shard.
//  2. Create Batchers and stub Consenters, and verify that all batchers are running with config sequence 0.
//  3. Prepare a config block that changes the endpoint of the shard primary (party 2), and have the stub
//     consenters deliver it.
//  4. Verify that the batcher whose endpoint changed reaches pending admin state, while the other batchers
//     reconfigure and return to running state with the new config sequence.
func TestBatcherReconfigPrimaryEndpointChange(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4}
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

	// make sure all batchers are running with the initial config sequence 0
	for j := range parties {
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// the shard primary is batchers[(shardID + term) % N] once sorted by party ID; for shard 1, term 0, N 4 this is party 2
	partyToChange := types.PartyID(2)

	// create config block that changes the shard primary's endpoint
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.UpdateBatcherEndpoint(t, partyToChange, types.ShardID(1), "127.0.0.1", 8080)
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

	// wait for the batcher whose endpoint changed to reach pending admin state, and for the other batchers to
	// reconfigure and return to running state with the new config sequence
	for j := range parties {
		partyID := parties[j]
		if partyID == partyToChange {
			require.Eventually(t, func() bool {
				return batchers[j].GetStatus().GetState() == node_utils.StatePendingAdmin
			}, 60*time.Second, 10*time.Millisecond)
			continue
		}
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(1)
		}, 60*time.Second, 10*time.Millisecond)
	}
}

// Scenario:
//  1. Create config and crypto material for 4 parties, one shard.
//  2. Create Batchers and stub Consenters, and verify that all batchers are running with config sequence 0.
//  3. Generate a new TLS certificate and a new signing certificate for the shard primary (party 2), both signed
//     by the party's existing CAs, and build a config block that sets them.
//  4. Have the stub consenters deliver the config block.
//  5. Verify the batcher whose certificates changed reaches pending admin state (a change to a batcher's own
//     identity requires an admin restart), while the other batchers reconfigure and return to running state with
//     the new config sequence.
func TestBatcherReconfigPrimaryCertChange(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	// capture the node IPs before cleaning up the network; they are used as SANs for the generated certificates
	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

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

	// make sure all batchers are running with the initial config sequence 0
	for j := range parties {
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// the shard primary is batchers[(shardID + term) % N] once sorted by party ID; for shard 1, term 0, N 4 this is party 2
	partyToChange := types.PartyID(2)
	shardToChange := types.ShardID(1)

	// generate a new TLS certificate and a new signing certificate for the shard primary's batcher, each signed by
	// the party's existing CA so that the config update is accepted
	org := fmt.Sprintf("org%d", partyToChange)
	tlsCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", org, "tlsca", fmt.Sprintf("tls%s-CA-cert.pem", org))
	tlsCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", org, "tlsca", "priv_sk")
	signCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", org, "ca", fmt.Sprintf("%s-CA-cert.pem", org))
	signCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", org, "ca", "priv_sk")

	partyDir := filepath.Join(dir, "crypto", "ordererOrganizations", org, "orderers", fmt.Sprintf("party%d", partyToChange), fmt.Sprintf("batcher%d", shardToChange))
	newBatcherTLSCert, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", filepath.Join(partyDir, "tls", "server.crt"), filepath.Join(partyDir, "tls", "server.key"), nodesIPs)
	require.NoError(t, err)
	require.NotNil(t, newBatcherTLSCert)

	newBatcherSignCert, err := armageddon.CreateNewCertificateFromCA(signCACertPath, signCAPrivKeyPath, "sign", filepath.Join(partyDir, "msp", "signcerts", fmt.Sprintf("batcher%d-cert.pem", shardToChange)), filepath.Join(partyDir, "msp", "keystore", "priv_sk"), nodesIPs)
	require.NoError(t, err)
	require.NotNil(t, newBatcherSignCert)

	// create a config block that changes the shard primary's TLS and signing certificates
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdateBuilder.UpdateBatcherTLSCert(t, partyToChange, shardToChange, newBatcherTLSCert)
	configUpdatePbData := configUpdateBuilder.UpdateBatcherSignCert(t, partyToChange, shardToChange, newBatcherSignCert)
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

	// wait for the batcher whose certificates changed to reach pending admin state, and for the other batchers to
	// reconfigure and return to running state with the new config sequence
	for j := range parties {
		partyID := parties[j]
		if partyID == partyToChange {
			require.Eventually(t, func() bool {
				return batchers[j].GetStatus().GetState() == node_utils.StatePendingAdmin
			}, 60*time.Second, 10*time.Millisecond)
			continue
		}
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(1)
		}, 60*time.Second, 10*time.Millisecond)
	}
}

// Scenario:
//  1. Create config and crypto material for 4 parties, one shard.
//  2. Create Batchers and stub Consenters, and verify that all batchers are running with config sequence 0.
//  3. Generate a fresh CA for party 1 and build a config block that appends its signing CA certificate to the
//     party's CACerts and its TLS CA certificate to the party's TLSCACerts.
//  4. Have the stub consenters deliver the config block.
//  5. Verify that a party CA change is applied as a normal reconfiguration: all batchers (including party 1's own
//     batcher) reconfigure and return to running state with the new config sequence, none reach pending admin, and
//     the new CA certificates are reflected in the batchers' shared configuration.
func TestBatcherReconfigCACerts(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	partyToChange := types.PartyID(1)
	// build the party's network config (endpoints) before cleaning up the network; it is used to regenerate a fresh
	// CA for the party
	partyNetworkConfig := partyNetworkConfigFromNetInfo(t, netInfo, partyToChange, numOfShards)

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

	// make sure all batchers are running with the initial config sequence 0
	for j := range parties {
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// generate a fresh CA for the party in a temporary directory and read its new signing and TLS CA certificates
	configUpdateDir := filepath.Join(dir, "config_update")
	require.NoError(t, os.MkdirAll(configUpdateDir, 0o755))
	_, err := armageddon.GenerateCryptoConfigWithProfile(&generate.Network{Parties: []generate.Party{*partyNetworkConfig}}, configUpdateDir)
	require.NoError(t, err)

	org := fmt.Sprintf("org%d", partyToChange)
	newSignCACert, err := os.ReadFile(filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", org, "msp", "cacerts", fmt.Sprintf("%s-CA-cert.pem", org)))
	require.NoError(t, err)
	newTLSCACert, err := os.ReadFile(filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", org, "msp", "tlscacerts", fmt.Sprintf("tls%s-CA-cert.pem", org)))
	require.NoError(t, err)

	// build a config block that appends the new CA certificates to the party's CACerts and TLSCACerts. Appending
	// (rather than replacing) keeps the party's existing node certificates valid.
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdateBuilder.AppendPartyCACerts(t, partyToChange, [][]byte{newSignCACert})
	configUpdatePbData := configUpdateBuilder.AppendPartyTLSCACerts(t, partyToChange, [][]byte{newTLSCACert})
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

	// a party CA change is not an identity change, so every batcher (including party 1's own batcher) reconfigures
	// and returns to running state with the new config sequence; none reach pending admin
	for j := range parties {
		require.Eventually(t, func() bool {
			status := batchers[j].GetStatus()
			return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(1)
		}, 60*time.Second, 10*time.Millisecond)
	}

	// verify that the new CA certificates are reflected in every batcher's shared configuration for the changed party
	for j := range parties {
		caCerts, tlsCACerts := partyCACertsFromBatcher(t, batchers[j], partyToChange)
		require.True(t, containsCert(caCerts, newSignCACert), "new signing CA cert not found in party %d CACerts of batcher %d", partyToChange, parties[j])
		require.True(t, containsCert(tlsCACerts, newTLSCACert), "new TLS CA cert not found in party %d TLSCACerts of batcher %d", partyToChange, parties[j])
	}
}

// partyNetworkConfigFromNetInfo builds a generate.Party describing the given party's node endpoints, read from the
// network information. It is used to regenerate crypto material (a fresh CA) for that party.
func partyNetworkConfigFromNetInfo(t *testing.T, netInfo testutil.ArmaNodesInfoMap, partyID types.PartyID, numOfShards int) *generate.Party {
	router := netInfo[testutil.NodeName{PartyID: partyID, NodeType: testutil.Router}]
	require.NotNil(t, router)
	consenter := netInfo[testutil.NodeName{PartyID: partyID, NodeType: testutil.Consensus}]
	require.NotNil(t, consenter)
	assembler := netInfo[testutil.NodeName{PartyID: partyID, NodeType: testutil.Assembler}]
	require.NotNil(t, assembler)

	var batchersEndpoints []string
	for shardID := types.ShardID(1); int(shardID) <= numOfShards; shardID++ {
		b := netInfo[testutil.NodeName{PartyID: partyID, NodeType: testutil.Batcher, ShardID: shardID}]
		require.NotNil(t, b)
		batchersEndpoints = append(batchersEndpoints, b.Listener.Addr().String())
	}

	return &generate.Party{
		ID:                partyID,
		RouterEndpoint:    router.Listener.Addr().String(),
		ConsenterEndpoint: consenter.Listener.Addr().String(),
		AssemblerEndpoint: assembler.Listener.Addr().String(),
		BatchersEndpoints: batchersEndpoints,
	}
}

// partyCACertsFromBatcher extracts the CACerts and TLSCACerts of the given party from the batcher's shared
// configuration (the SharedConfig carried in the batcher's config bundle).
func partyCACertsFromBatcher(t *testing.T, b *batcher.Batcher, partyID types.PartyID) (caCerts [][]byte, tlsCACerts [][]byte) {
	ordererConfig, ok := b.GetConfig().Bundle.OrdererConfig()
	require.True(t, ok, "failed to extract orderer config from the batcher's bundle")

	sharedConfig := ordererpb.SharedConfig{}
	require.NoError(t, proto.Unmarshal(ordererConfig.ConsensusMetadata(), &sharedConfig))

	for _, partyConfig := range sharedConfig.GetPartiesConfig() {
		if partyConfig.PartyID == uint32(partyID) {
			return partyConfig.GetCACerts(), partyConfig.GetTLSCACerts()
		}
	}

	require.FailNow(t, fmt.Sprintf("party %d not found in the batcher's shared configuration", partyID))
	return nil, nil
}

// containsCert reports whether certs contains a certificate equal to target.
func containsCert(certs [][]byte, target []byte) bool {
	for _, cert := range certs {
		if bytes.Equal(cert, target) {
			return true
		}
	}
	return false
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

// consenterClientRootCAs derives, from the given config block, the set of client root CAs a consenter of the
// given party should trust. It mirrors how a consenter computes its ClientRootCAs in production, so the returned
// set includes the TLS CAs of every party present in the block (including a newly added party).
func consenterClientRootCAs(t *testing.T, dir string, partyID types.PartyID, configBlock *common.Block) [][]byte {
	nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "local_config_consenter.yaml")
	nodeConfig, _, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigConsenterForTrust%d", partyID), zap.DebugLevel))
	require.NoError(t, err)
	updatedConfig, _, err := nodeConfig.NewUpdatedConfigurationFromBlock(configBlock)
	require.NoError(t, err)
	consenterConfig := updatedConfig.ExtractConsenterConfig(configBlock)
	require.NotNil(t, consenterConfig)
	caCerts := make([][]byte, 0, len(consenterConfig.ClientRootCAs))
	caCerts = append(caCerts, consenterConfig.ClientRootCAs...)
	return caCerts
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
