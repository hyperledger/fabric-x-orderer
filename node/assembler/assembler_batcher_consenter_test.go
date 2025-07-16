/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
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

	assembler := newAssemblerTest(t, partyID, shardID, ca, batcherInfos, consenterStub.consenterInfo)
	defer assembler.Stop()

	// wait for genesis block
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// send batch and matching decision
	batch1 := testutil.CreateMockBatch(1, 1, 1, []int{1})
	batchersStub[0].SetNextBatch(batch1)
	oba1 := obaCreator.Append(batch1, 1, 1, 1)
	consenterStub.SetNextDecision(oba1)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// stop consenter and send next batch
	consenterStub.Stop()

	batch2 := testutil.CreateMockBatch(1, 1, 2, []int{2, 3})
	batchersStub[0].SetNextBatch(batch2)

	// restart consenter and send matching decision
	consenterStub.Restart()

	oba2 := obaCreator.Append(batch2, 2, 1, 1)
	consenterStub.SetNextDecision(oba2)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 3*time.Second, 100*time.Millisecond)
}

func newAssemblerTest(t *testing.T, partyID types.PartyID, shardID types.ShardID, ca tlsgen.CA, batchersInfo []config.BatcherInfo, consenterInfo config.ConsenterInfo) *assembler.Assembler {
	genesisBlock := utils.EmptyGenesisBlock("arma")
	genesisBlock.Metadata = &common.BlockMetadata{
		Metadata: [][]byte{nil, nil, []byte("dummy"), []byte("dummy")},
	}

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	shards := []config.ShardInfo{
		{
			ShardId:  shardID,
			Batchers: batchersInfo,
		},
	}

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
			b.Stop()
		}
	}
}
