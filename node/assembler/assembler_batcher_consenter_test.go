/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestAssemblerHandlesConsenterReconnect(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)
	shardID := types.ShardID(1)

	batchersStub, batcherInfos, cleanup := createStubBatchersAndInfos(t, numParties, shardID, ca)
	defer cleanup()

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Shutdown()

	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := createTestBatchWithSize(1, 1, 1, []int{1})
	batchersStub[0].SetNextBatch(batch1)
	oba1 := obaCreator.Append(batch1, 1, 1, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// stop consenter and send next batch
	consenterStub.Stop()

	batch2 := createTestBatchWithSize(1, 1, 2, []int{2, 3})
	batchersStub[0].SetNextBatch(batch2)

	// let assembler retry while consenter is down
	time.Sleep(2 * time.Second)

	// restart consenter and send matching decision
	consenterStub.Restart()

	oba2 := obaCreator.Append(batch2, 2, 1, 1)
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 3*time.Second, 100*time.Millisecond)

	// send next decision and restart consenter
	batch3 := createTestBatchWithSize(1, 1, 3, []int{4})
	oba3 := obaCreator.Append(batch3, 3, 1, 1)
	consenterStub.SetNextDecision(oba3.(*state.AvailableBatchOrdered))

	// wait for decision to be sent
	time.Sleep(time.Second)

	consenterStub.Stop()
	consenterStub.Restart()

	// send matching batch
	batchersStub[0].SetNextBatch(batch3)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 5
	}, 3*time.Second, 100*time.Millisecond)
}

func TestAssemblerHandlesBatcherReconnect(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)
	shardID := types.ShardID(1)

	batchersStub, batcherInfos, cleanup := createStubBatchersAndInfos(t, numParties, shardID, ca)
	defer cleanup()

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Shutdown()

	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := types.NewSimpleBatch(1, 1, 1, types.BatchedRequests{[]byte{1}})
	batchersStub[0].SetNextBatch(batch1)
	oba1 := obaCreator.Append(batch1, 1, 1, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// stop batcher
	batchersStub[0].Stop()

	// let assembler retry while batcher is down
	time.Sleep(2 * time.Second)

	// send next decision
	batch2 := types.NewSimpleBatch(2, 1, 1, types.BatchedRequests{[]byte{2}, []byte{3}})
	oba2 := obaCreator.Append(batch2, 2, 1, 1)
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))

	// restart batcher and send matching batch
	batchersStub[0].Restart()
	batchersStub[0].SetNextBatch(batch2)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 3*time.Second, 100*time.Millisecond)

	// send next batch and restart batcher
	batch3 := types.NewSimpleBatch(3, 1, 1, types.BatchedRequests{[]byte{4}})
	batchersStub[0].SetNextBatch(batch3)

	// wait for batch to be sent
	time.Sleep(time.Second)

	batchersStub[0].Stop()
	batchersStub[0].Restart()

	// send matching decision
	oba3 := obaCreator.Append(batch3, 3, 1, 1)
	consenterStub.SetNextDecision(oba3.(*state.AvailableBatchOrdered))

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 5
	}, 3*time.Second, 100*time.Millisecond)
}

func TestAssemblerBatchProcessingAcrossParties(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)

	batchersStubShard0, batcherInfosShard0, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(0), ca)
	defer cleanup()

	_, batcherInfosShard1, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(1), ca)
	defer cleanup()

	shards := []config.ShardInfo{
		{ShardId: types.ShardID(0), Batchers: batcherInfosShard0},
		{ShardId: types.ShardID(1), Batchers: batcherInfosShard1},
	}

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Shutdown()

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and decision from party 1 in shard 0
	batch1 := types.NewSimpleBatch(0, 0, 2, types.BatchedRequests{[]byte{1}})
	batchersStubShard0[0].SetNextBatch(batch1)

	oba1 := obaCreator.Append(batch1, 1, 1, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// send batch and decision from party 2 in shard 0
	// assembler (party 1) should find the batch in another party
	batch2 := types.NewSimpleBatch(1, 0, 2, types.BatchedRequests{[]byte{2}, []byte{3}})
	batchersStubShard0[1].SetNextBatch(batch2)

	oba2 := obaCreator.Append(batch2, 2, 1, 1)
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))

	// assembler did not process the batch from party 2 (bug)
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 3
	}, 60*time.Second, 100*time.Millisecond)
}

func newAssemblerTest(t *testing.T, partyID types.PartyID, ca tlsgen.CA, shards []config.ShardInfo, consenterInfo config.ConsenterInfo) *assembler.Assembler {
	genesisBlock := utils.EmptyGenesisBlock("arma")
	genesisBlock.Metadata = &common.BlockMetadata{
		Metadata: [][]byte{nil, nil, []byte("dummy"), []byte("dummy")},
	}

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	nodeConfig := &config.AssemblerNodeConfig{
		TLSPrivateKeyFile:         ckp.Key,
		TLSCertificateFile:        ckp.Cert,
		PartyId:                   partyID,
		Directory:                 t.TempDir(),
		ListenAddress:             "127.0.0.1:0",
		PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024,
		RestartLedgerScanTimeout:  5 * time.Second,
		PrefetchEvictionTtl:       time.Hour,
		ReplicationChannelSize:    100,
		BatchRequestsChannelSize:  1000,
		Shards:                    shards,
		Consenter:                 consenterInfo,
		UseTLS:                    true,
		ClientAuthRequired:        false,
	}

	assemblerGRPC := node.CreateGRPCAssembler(nodeConfig)

	assembler := assembler.NewAssembler(nodeConfig, assemblerGRPC, genesisBlock, testutil.CreateLogger(t, int(partyID)))

	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)
	go func() {
		err := assemblerGRPC.Start()
		require.NoError(t, err)
	}()

	return assembler
}

func createStubBatchersAndInfos(t *testing.T, numParties int, shardID types.ShardID, ca tlsgen.CA) ([]*stubBatcher, []config.BatcherInfo, func()) {
	var batchers []*stubBatcher
	var batcherInfos []config.BatcherInfo

	for i := 1; i <= numParties; i++ {
		b := NewStubBatcher(t, shardID, types.PartyID(i), ca)
		batchers = append(batchers, b)
		batcherInfos = append(batcherInfos, b.batcherInfo)
	}

	return batchers, batcherInfos, func() {
		for _, b := range batchers {
			b.Shutdown()
		}
	}
}
