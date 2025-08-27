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

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, 20*time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := types.NewSimpleBatch(0, 1, 1, types.BatchedRequests{[]byte{1}})
	batchersStub[0].SetNextBatch(batch1)
	<-batchersStub[0].batchSentCh

	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// stop consenter and send next batch
	consenterStub.Stop()

	batch2 := types.NewSimpleBatch(1, 1, 1, types.BatchedRequests{[]byte{2}, []byte{3}})
	batchersStub[0].SetNextBatch(batch2)
	<-batchersStub[0].batchSentCh

	// let assembler retry while consenter is down
	time.Sleep(time.Second)

	// restart consenter, wait for reconnect
	consenterStub.Restart()
	time.Sleep(time.Second)

	// send matching decision
	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 30*time.Second, 100*time.Millisecond)
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

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, 20*time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := types.NewSimpleBatch(0, 1, 1, types.BatchedRequests{[]byte{1}})
	batchersStub[0].SetNextBatch(batch1)
	<-batchersStub[0].batchSentCh

	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// stop batcher
	batchersStub[0].Stop()

	// let assembler retry while batcher is down
	time.Sleep(time.Second)

	// restart batcher, wait for reconnect
	batchersStub[0].Restart()
	time.Sleep(time.Second)

	// send next batch and decision
	batch2 := types.NewSimpleBatch(1, 1, 1, types.BatchedRequests{[]byte{2}, []byte{3}})
	batchersStub[0].SetNextBatch(batch2)
	<-batchersStub[0].batchSentCh

	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 30*time.Second, 10*time.Millisecond)
}

func TestAssemblerBatchProcessingAcrossParties(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	numParties := 4
	partyID := types.PartyID(1)

	batchersStubShard0, batcherInfosShard0, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(0), ca)
	defer cleanup()

	batchersStubShard1, batcherInfosShard1, cleanup := createStubBatchersAndInfos(t, numParties, types.ShardID(1), ca)
	defer cleanup()

	shards := []config.ShardInfo{
		{ShardId: types.ShardID(0), Batchers: batcherInfosShard0},
		{ShardId: types.ShardID(1), Batchers: batcherInfosShard1},
	}

	consenterStub := NewStubConsenter(t, partyID, ca)
	defer consenterStub.Shutdown()

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch from party 2 on shard 0
	// assembler (party 1) should fetch it from another party
	batch1 := types.NewSimpleBatch(0, 0, 2, types.BatchedRequests{[]byte{1}})
	batchersStubShard0[1].SetNextBatch(batch1)

	// send matching decision
	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	// wait until the batch is sent
	<-batchersStubShard0[1].batchSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 10*time.Second, 100*time.Millisecond)

	// send another batch and decision from party 1 on shard 0
	batch2 := types.NewSimpleBatch(1, 0, 2, types.BatchedRequests{[]byte{2}, []byte{3}})
	batchersStubShard0[0].SetNextBatch(batch2)
	<-batchersStubShard0[0].batchSentCh

	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 3*time.Second, 100*time.Millisecond)

	// send batch and decision from party 1 on shard 1
	batch3 := types.NewSimpleBatch(0, 1, 2, types.BatchedRequests{[]byte{5}})
	batchersStubShard1[0].SetNextBatch(batch3)
	<-batchersStubShard1[0].batchSentCh

	oba3 := obaCreator.Append(batch3, 3, 0, 1)
	consenterStub.SetNextDecision(oba3.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 5
	}, 3*time.Second, 100*time.Millisecond)
}

func TestAssembler_DifferentDigestSameSeq(t *testing.T) {
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

	assembler := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, 500*time.Millisecond)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and decision
	batch1 := types.NewSimpleBatch(0, 1, 1, types.BatchedRequests{[]byte{1}})
	batchersStub[0].SetNextBatch(batch1)
	<-batchersStub[0].batchSentCh

	// also send batch1 via another batcher in case the assembler starts searching
	batchersStub[2].SetNextBatch(batch1)

	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// create another batch with same <Sh, Pr, Seq> but different digest
	batch2 := types.NewSimpleBatch(0, 1, 1, types.BatchedRequests{[]byte{2}})
	require.NotEqual(t, batch1.Digest(), batch2.Digest())

	// send next batch through another batcher
	batchersStub[1].SetNextBatch(batch2)
	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	// send decision
	consenterStub.SetNextDecision(oba2.(*state.AvailableBatchOrdered))
	<-consenterStub.decisionSentCh
	<-batchersStub[1].batchSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 3
	}, 5*time.Second, 100*time.Millisecond)
}

func newAssemblerTest(t *testing.T, partyID types.PartyID, ca tlsgen.CA, shards []config.ShardInfo, consenterInfo config.ConsenterInfo, popWaitMonitorTimeout time.Duration) *assembler.Assembler {
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
		PopWaitMonitorTimeout:     popWaitMonitorTimeout,
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
