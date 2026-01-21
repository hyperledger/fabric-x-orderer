/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"fmt"
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
	defer consenterStub.Stop()

	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	assembler, _ := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, 20*time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := types.NewSimpleBatch(1, 1, 0, types.BatchedRequests{[]byte{1}}, 0)
	batchersStub[0].SetNextBatch(batch1)

	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1)
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// stop consenter and send next batch
	consenterStub.Stop()

	batch2 := types.NewSimpleBatch(1, 1, 1, types.BatchedRequests{[]byte{2}, []byte{3}}, 0)
	batchersStub[0].SetNextBatch(batch2)

	// let assembler retry while consenter is down
	time.Sleep(time.Second)

	// restart consenter, wait for reconnect
	consenterStub.Restart()
	time.Sleep(time.Second)

	// send matching decision
	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	consenterStub.SetNextDecision(oba2)
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
	defer consenterStub.Stop()

	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	assembler, _ := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, 20*time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := types.NewSimpleBatch(1, 1, 0, types.BatchedRequests{[]byte{1}}, 0)
	batchersStub[0].SetNextBatch(batch1)

	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1)
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
	batch2 := types.NewSimpleBatch(1, 1, 1, types.BatchedRequests{[]byte{2}, []byte{3}}, 0)
	batchersStub[0].SetNextBatch(batch2)

	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	consenterStub.SetNextDecision(oba2)
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
	defer consenterStub.Stop()

	assembler, _ := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, time.Second)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch from party 2 on shard 0
	// assembler (party 1) should fetch it from another party
	batch1 := types.NewSimpleBatch(0, 2, 0, types.BatchedRequests{[]byte{1}}, 0)
	batchersStubShard0[1].SetNextBatch(batch1)
	batchersStubShard0[0].SetNextBatch(batch1)

	// send matching decision
	oba1 := obaCreator.Append(batch1, 1, 0, 1)
	consenterStub.SetNextDecision(oba1)
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 10*time.Second, 100*time.Millisecond)

	// send another batch and decision from party 1 on shard 0
	batch2 := types.NewSimpleBatch(0, 2, 1, types.BatchedRequests{[]byte{2}, []byte{3}}, 0)
	batchersStubShard0[0].SetNextBatch(batch2)

	oba2 := obaCreator.Append(batch2, 2, 0, 1)
	consenterStub.SetNextDecision(oba2)
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 3*time.Second, 100*time.Millisecond)

	// send batch and decision from party 1 on shard 1
	batch3 := types.NewSimpleBatch(1, 2, 0, types.BatchedRequests{[]byte{5}}, 0)
	batchersStubShard1[0].SetNextBatch(batch3)

	oba3 := obaCreator.Append(batch3, 3, 0, 1)
	consenterStub.SetNextDecision(oba3)
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
	defer consenterStub.Stop()

	shards := []config.ShardInfo{{ShardId: shardID, Batchers: batcherInfos}}

	assembler, _ := newAssemblerTest(t, partyID, ca, shards, consenterStub.consenterInfo, 500*time.Millisecond)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	var batches0to10 []*types.SimpleBatch
	// send batch 0 and decisions
	batch0 := types.NewSimpleBatch(1, 1, 0, types.BatchedRequests{[]byte{1}}, 0)
	batchersStub[0].SetNextBatch(batch0)
	batches0to10 = append(batches0to10, batch0)

	oba1 := obaCreator.Append(batch0, 1, 0, 1)
	consenterStub.SetNextDecision(oba1)
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// Send batch 1-10 and decisions via batcher 0
	for i := types.BatchSequence(1); i <= 10; i++ {
		batch := types.NewSimpleBatch(1, 1, i, types.BatchedRequests{[]byte{byte(i)}}, 0)
		batchersStub[0].SetNextBatch(batch)

		oba := obaCreator.Append(batch, types.DecisionNum(1+i), 0, 1)
		consenterStub.SetNextDecision(oba)
		<-consenterStub.decisionSentCh

		require.Eventually(t, func() bool {
			return assembler.GetTxCount() == uint64(2+i)
		}, 3*time.Second, 100*time.Millisecond)

		batches0to10 = append(batches0to10, batch)
	}

	// Create another batch with same <Sh, Pr, Seq> = <1,1,10> but different digest
	batch10dup := types.NewSimpleBatch(1, 1, 10, types.BatchedRequests{[]byte{100}}, 0)
	require.NotEqual(t, batch10dup.Digest(), batches0to10[10].Digest())

	// Batchers 1,2 have the same batches with sequence 0-9, but differ on the batch with sequence 10
	for i, b := range batches0to10 {
		if i == 10 {
			break
		}
		batchersStub[1].SetNextBatch(b)
		batchersStub[2].SetNextBatch(b)
	}
	// Send the duplicate batch through batchers 1,2 (not 0, which provided the previous one).
	batchersStub[1].SetNextBatch(batch10dup)
	batchersStub[2].SetNextBatch(batch10dup)
	oba10dup := obaCreator.Append(batch10dup, 12, 0, 1)
	// send decision respective
	consenterStub.SetNextDecision(oba10dup)
	<-consenterStub.decisionSentCh

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 13
	}, 10*time.Second, 100*time.Millisecond, fmt.Sprintf("TXs: %d", assembler.GetTxCount()))
}

func newAssemblerTest(t *testing.T, partyID types.PartyID, ca tlsgen.CA, shards []config.ShardInfo, consenterInfo config.ConsenterInfo, popWaitMonitorTimeout time.Duration) (*assembler.Assembler, string) {
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
		MonitoringListenAddress:   "127.0.0.1:0",
	}

	assemblerGRPC := node.CreateGRPCAssembler(nodeConfig)

	assembler := assembler.NewAssembler(nodeConfig, assemblerGRPC, genesisBlock, testutil.CreateLogger(t, int(partyID)))

	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)
	go func() {
		err := assemblerGRPC.Start()
		require.NoError(t, err)
	}()

	return assembler, assemblerGRPC.Address()
}

func createStubBatchersAndInfos(t *testing.T, numParties int, shardID types.ShardID, ca tlsgen.CA) ([]*stubBatcher, []config.BatcherInfo, func()) {
	var batchers []*stubBatcher
	var batcherInfos []config.BatcherInfo

	var parties []types.PartyID
	for p := types.PartyID(1); int(p) <= numParties; p++ {
		parties = append(parties, p)
	}

	for i := 1; i <= numParties; i++ {
		b := NewStubBatcher(t, shardID, types.PartyID(i), parties, ca)
		batchers = append(batchers, b)
		batcherInfos = append(batcherInfos, b.batcherInfo)
	}

	return batchers, batcherInfos, func() {
		for _, b := range batchers {
			b.Stop()
		}
	}
}
