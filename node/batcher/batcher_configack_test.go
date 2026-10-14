/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"

	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Start a single-party, single-shard batcher with its stub consenter at config sequence 0.
//  2. Record the ConfigAck the batcher sends by setting an AckConfigHandler on the stub consenter.
//  3. Deliver a config update (an AutoRemoveTimeout change) through the consenter path.
//  4. Verify the batcher sent exactly one ConfigAck identifying itself as the batcher
//     (NodeType=BATCHER, Shard=1) on config sequence 1.
//
// SubmitConfigAck runs before the batching-params/eviction checks in ApplyConfig, so the
// batcher acks the new config even though this change subsequently sends it to pending admin.
func TestBatcherSendsConfigAck(t *testing.T) {
	party := types.PartyID(1)

	dir := t.TempDir()
	setup := createConfigAckTestSetup(t, dir, party)

	recorder := &configAckRecorder{}
	setup.stubConsenter.SetAckConfigHandler(func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		recorder.record(req)
		return &protos.ConfigAckResponse{}, nil
	})

	startBatcherNodes([]*batcher.Batcher{setup.batcher})

	// wait for the batcher to be running with the initial config sequence.
	require.Eventually(t, func() bool {
		status := setup.batcher.GetStatus()
		return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
	}, 60*time.Second, 10*time.Millisecond)

	setup.deliverConfigUpdate(t, dir, autoRemoveTimeoutConfigUpdate(t, dir))

	// the batcher applies (and acks) the new config, then reaches pending admin because a
	// batching parameter changed.
	require.Eventually(t, func() bool {
		return setup.batcher.GetStatus().GetState() == node_utils.StatePendingAdmin
	}, 60*time.Second, 10*time.Millisecond)

	acks := recorder.all()
	require.Len(t, acks, 1)
	require.Equal(t, protos.NodeType_BATCHER, acks[0].NodeType)
	require.EqualValues(t, 1, acks[0].Shard)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Start a single-party, single-shard batcher with its stub consenter at config sequence 0.
//  2. Define an AckConfigHandler that rejects the first few attempts, then accepts and records.
//  3. Deliver a config update through the consenter path.
//  4. Verify the batcher's sender retried (more than one attempt) and the consenter eventually
//     recorded exactly one ConfigAck (NodeType=BATCHER) on config sequence 1.
func TestBatcherConfigAckRetriesUntilConsenterAccepts(t *testing.T) {
	party := types.PartyID(1)

	dir := t.TempDir()
	setup := createConfigAckTestSetup(t, dir, party)

	const failuresBeforeSuccess = 3
	var callCount int32
	recorder := &configAckRecorder{}
	setup.stubConsenter.SetAckConfigHandler(func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		if atomic.AddInt32(&callCount, 1) <= failuresBeforeSuccess {
			return nil, fmt.Errorf("temporary failure")
		}
		recorder.record(req)
		return &protos.ConfigAckResponse{}, nil
	})

	startBatcherNodes([]*batcher.Batcher{setup.batcher})

	require.Eventually(t, func() bool {
		status := setup.batcher.GetStatus()
		return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
	}, 60*time.Second, 10*time.Millisecond)

	setup.deliverConfigUpdate(t, dir, autoRemoveTimeoutConfigUpdate(t, dir))

	require.Eventually(t, func() bool {
		return setup.batcher.GetStatus().GetState() == node_utils.StatePendingAdmin
	}, 60*time.Second, 10*time.Millisecond)

	require.Greater(t, atomic.LoadInt32(&callCount), int32(1))
	acks := recorder.all()
	require.Len(t, acks, 1)
	require.Equal(t, protos.NodeType_BATCHER, acks[0].NodeType)
	require.EqualValues(t, 1, acks[0].ConfigSeq)
}

// Scenario:
//  1. Start a single-party, single-shard batcher with its stub consenter at config sequence 0.
//  2. Set an AckConfigHandler that holds back the consenter's response.
//  3. Deliver a config update (an AutoRemoveTimeout change) through the consenter path, and wait for
//     the batcher's ConfigAck.
//  4. Verify the batcher stays soft stopped, serving under the current config, and does not move on
//     to the new config while it waits for the consenter's response.
//  5. Let the consenter respond, and verify the batcher then moves on (here, to pending admin, because
//     a batching parameter changed).
func TestBatcherConfigAckWaitsForConsenterResponse(t *testing.T) {
	party := types.PartyID(1)

	dir := t.TempDir()
	setup := createConfigAckTestSetup(t, dir, party)

	acked := make(chan struct{}, 1)
	respond := make(chan struct{})
	setup.stubConsenter.SetAckConfigHandler(func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		select {
		case acked <- struct{}{}:
		default:
		}
		<-respond
		return &protos.ConfigAckResponse{}, nil
	})

	startBatcherNodes([]*batcher.Batcher{setup.batcher})

	require.Eventually(t, func() bool {
		status := setup.batcher.GetStatus()
		return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
	}, 60*time.Second, 10*time.Millisecond)

	setup.deliverConfigUpdate(t, dir, autoRemoveTimeoutConfigUpdate(t, dir))

	select {
	case <-acked:
	case <-time.After(60 * time.Second):
		t.Fatal("the batcher did not send a ConfigAck")
	}

	require.Never(t, func() bool {
		return setup.batcher.GetStatus().GetState() != node_utils.StateSoftStopped
	}, 2*time.Second, 100*time.Millisecond, "the batcher moved on to the new config before the consenter responded to its ack")

	close(respond)

	require.Eventually(t, func() bool {
		return setup.batcher.GetStatus().GetState() == node_utils.StatePendingAdmin
	}, 60*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. Start a single-party, single-shard batcher with its stub consenter at config sequence 0.
//  2. Set an AckConfigHandler that holds back the consenter's response.
//  3. Deliver a config update the batcher applies dynamically (a BatchCreationTimeout change)
//     through the consenter path, and wait for the batcher's ConfigAck.
//  4. Stop the batcher while it waits for the consenter's response, and verify it stays stopped:
//     a stop is not a consenter that did not respond in time, so the batcher must not apply the
//     new config and reopen its network and ledger.
func TestBatcherConfigAckStopWhileWaitingForConsenterResponse(t *testing.T) {
	party := types.PartyID(1)

	dir := t.TempDir()
	setup := createConfigAckTestSetup(t, dir, party)

	acked := make(chan struct{}, 1)
	respond := make(chan struct{})
	t.Cleanup(func() { close(respond) }) // release the blocked handler before the stub consenter stops
	setup.stubConsenter.SetAckConfigHandler(func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		select {
		case acked <- struct{}{}:
		default:
		}
		<-respond
		return &protos.ConfigAckResponse{}, nil
	})

	startBatcherNodes([]*batcher.Batcher{setup.batcher})

	require.Eventually(t, func() bool {
		status := setup.batcher.GetStatus()
		return status.GetState() == node_utils.StateRunning && status.ConfigSequenceNumber == uint64(0)
	}, 60*time.Second, 10*time.Millisecond)

	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.BatchCreationTimeout, "2s"))
	require.NotNil(t, configUpdatePbData)
	setup.deliverConfigUpdate(t, dir, configUpdatePbData)

	select {
	case <-acked:
	case <-time.After(60 * time.Second):
		t.Fatal("the batcher did not send a ConfigAck")
	}

	setup.batcher.Stop()

	require.Never(t, func() bool {
		return setup.batcher.GetStatus().GetState() != node_utils.StateStopped
	}, 3*time.Second, 100*time.Millisecond, "the batcher applied the new config after it was stopped")
}

// createConfigAckTestSetup creates the crypto/config material for a single-party,
// single-shard network and builds one real batcher plus its stub consenter. It
// returns the batcher, the stub consenter, and the genesis block needed to
// build a config update for that network.
func createConfigAckTestSetup(t *testing.T, dir string, party types.PartyID) *batcherWithStubs {
	parties := []types.PartyID{party}
	numOfShards := 1

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties, numOfShards)

	netInfo.CleanUp()
	stubConsenters := createStubConsenters(t, dir, parties)
	t.Cleanup(func() { stubConsenters[0].StopNet() })

	batchers, genesisBlock, bundle := createBatcherNodes(t, dir, parties, numOfShards, stubConsenters)
	t.Cleanup(func() { batchers[0].Stop() })

	return &batcherWithStubs{
		parties:       parties,
		batcher:       batchers[0],
		stubConsenter: stubConsenters[0],
		genesisBlock:  genesisBlock,
		bundle:        bundle,
	}
}

type batcherWithStubs struct {
	parties       []types.PartyID
	batcher       *batcher.Batcher
	stubConsenter *stubConsenter
	genesisBlock  *common.Block
	bundle        channelconfig.Resources
}

// deliverConfigUpdate builds a config block from the given raw config update and
// delivers it to the batcher through its stub consenter, exactly as a real
// consenter would replicate a decision carrying a config block.
func (s *batcherWithStubs) deliverConfigUpdate(t *testing.T, dir string, configUpdatePbData []byte) {
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, s.parties, 1, configUpdatePbData)
	configBlock, err := cfgutil.CreateConsensusConfigBlock(s.bundle, configUpdateEnvelope, s.genesisBlock.Header, 1, types.DecisionNum(1), 1, 0)
	require.NoError(t, err)

	st := &state.State{N: uint16(len(s.parties)), Shards: []state.ShardTerm{{Shard: 1, Term: 0}}}
	s.stubConsenter.UpdateStateHeaderWithConfigBlock(types.DecisionNum(1), []*common.Block{configBlock}, st)
}

// configAckRecorder captures the ConfigAck requests a batcher sends to the stub
// consenter. AckConfig is served on the consenter's gRPC goroutine.
type configAckRecorder struct {
	mu   sync.Mutex
	acks []*protos.ConfigAck
}

func (r *configAckRecorder) record(req *protos.ConfigAck) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks = append(r.acks, req)
}

func (r *configAckRecorder) all() []*protos.ConfigAck {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*protos.ConfigAck(nil), r.acks...)
}
