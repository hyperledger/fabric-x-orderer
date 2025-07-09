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

func TestAssemblerAppendBlockAndIgnoreDuplicate(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherStub := NewStubBatcher(t, types.ShardID(1), types.PartyID(1), ca)
	defer batcherStub.Stop()

	consenterStub := NewStubConsenter(t, types.PartyID(1), ca)
	defer consenterStub.Stop()

	assembler, clean := newAssemblerTest(t, 1, 1, ca, batcherStub.batcherInfo, consenterStub.consenterInfo)
	defer clean()

	// genesis block added
	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 1
	}, 3*time.Second, 100*time.Millisecond)

	obaCreator, _ := NewOrderedBatchAttestationCreator()

	// create batch with 1 req, send from batcher and consenter
	batch1 := testutil.CreateMockBatch(1, 1, 1, []int{1})
	batcherStub.SetNextBatch(batch1)

	oba1 := obaCreator.Append(batch1, 1, 1, 1)
	consenterStub.SetNextDecision(oba1)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 2
	}, 3*time.Second, 100*time.Millisecond)

	// create batch with 2 reqs, send from batcher and consenter
	batch2 := testutil.CreateMockBatch(1, 1, 2, []int{2, 3})
	batcherStub.SetNextBatch(batch2)

	oba2 := obaCreator.Append(batch2, 2, 1, 1)
	consenterStub.SetNextDecision(oba2)

	require.Eventually(t, func() bool {
		return assembler.GetTxCount() == 4
	}, 3*time.Second, 100*time.Millisecond)

	// send duplicate batch+oba, should be ignored
	batcherStub.SetNextBatch(batch2)
	consenterStub.SetNextDecision(oba2)

	require.Never(t, func() bool {
		return assembler.GetTxCount() > 4
	}, 3*time.Millisecond, 100*time.Millisecond)
}

func newAssemblerTest(t *testing.T, partyID int, shardID int, ca tlsgen.CA, batcherInfo config.BatcherInfo, consenterInfo config.ConsenterInfo) (*assembler.Assembler, func()) {
	genesisBlock := utils.EmptyGenesisBlock("arma")
	genesisBlock.Metadata = &common.BlockMetadata{
		Metadata: [][]byte{nil, nil, []byte("dummy"), []byte("dummy")},
	}

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	shards := []config.ShardInfo{
		{
			ShardId: types.ShardID(shardID),
			Batchers: []config.BatcherInfo{
				batcherInfo,
			},
		},
	}

	nodeConfig := &config.AssemblerNodeConfig{
		TLSPrivateKeyFile:         ckp.Key,
		TLSCertificateFile:        ckp.Cert,
		PartyId:                   types.PartyID(partyID),
		Directory:                 t.TempDir(),
		ListenAddress:             "0.0.0.0:0",
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

	assembler := assembler.NewAssembler(nodeConfig, assemblerGRPC, genesisBlock, testutil.CreateLogger(t, partyID))

	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)
	go func() {
		err := assemblerGRPC.Start()
		require.NoError(t, err)
	}()

	return assembler, func() {
		assembler.Stop()
	}
}
