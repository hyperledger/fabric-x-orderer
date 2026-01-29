/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	assembler_mocks "github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	delivery_mocks "github.com/hyperledger/fabric-x-orderer/node/delivery/mocks"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	ledger_mocks "github.com/hyperledger/fabric-x-orderer/node/ledger/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type assemblerTest struct {
	logger                         types.Logger
	shards                         []types.ShardID
	party                          types.PartyID
	ledgerDir                      string
	genesisBlock                   *common.Block
	nodeConfig                     *config.AssemblerNodeConfig
	orderedBatchAttestationCreator *OrderedBatchAttestationCreator
	expectedLedgerBA               []*state.AvailableBatchOrdered
	assembler                      *assembler.Assembler
	shardToBatcherChan             map[types.ShardID]chan types.Batch
	consensusBAChan                chan *state.AvailableBatchOrdered
	batchBringerMock               *assembler_mocks.FakeBatchBringer
	ledgerMock                     *ledger_mocks.FakeAssemblerLedgerReaderWriter
	prefetcherMock                 *assembler_mocks.FakePrefetcherController
	prefetchIndexMock              *assembler_mocks.FakePrefetchIndexer
	consensusBringerMock           *delivery_mocks.FakeConsensusBringer
}

type dummyAssemblerStopper struct{}

func (d *dummyAssemblerStopper) Stop() {}

func generateRandomBytes(t *testing.T, size int) []byte {
	b := make([]byte, size)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func setupAssemblerTest(t *testing.T, shards []types.ShardID, parties []types.PartyID, myParty types.PartyID, genesisBlock *common.Block) *assemblerTest {
	orderedBatchAttestationCreator, _ := NewOrderedBatchAttestationCreator()
	test := &assemblerTest{
		logger:                         testutil.CreateLoggerForModule(t, "assembler", zap.DebugLevel),
		shards:                         shards,
		party:                          myParty,
		ledgerDir:                      t.TempDir(),
		genesisBlock:                   genesisBlock,
		orderedBatchAttestationCreator: orderedBatchAttestationCreator,
		expectedLedgerBA:               []*state.AvailableBatchOrdered{},
		batchBringerMock:               &assembler_mocks.FakeBatchBringer{},
		ledgerMock:                     &ledger_mocks.FakeAssemblerLedgerReaderWriter{},
		prefetcherMock:                 &assembler_mocks.FakePrefetcherController{},
		prefetchIndexMock:              &assembler_mocks.FakePrefetchIndexer{},
		consensusBringerMock:           &delivery_mocks.FakeConsensusBringer{},
	}
	assemblerEndpoint := "assembler"
	consenterEndpoint := "consenter"

	shardsInfo := []config.ShardInfo{}
	batcherInfo := []config.BatcherInfo{}
	for _, partyId := range parties {
		batcherInfo = append(batcherInfo, config.BatcherInfo{
			PartyID: partyId,
		})
	}
	for _, shardId := range shards {
		shardsInfo = append(shardsInfo, config.ShardInfo{
			ShardId:  shardId,
			Batchers: batcherInfo,
		})
	}
	test.nodeConfig = &config.AssemblerNodeConfig{
		TLSPrivateKeyFile:         generateRandomBytes(t, 16),
		TLSCertificateFile:        generateRandomBytes(t, 16),
		PartyId:                   test.party,
		Directory:                 test.ledgerDir,
		ListenAddress:             assemblerEndpoint,
		PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024, // 1GB
		RestartLedgerScanTimeout:  5 * time.Second,
		PrefetchEvictionTtl:       time.Hour,
		PopWaitMonitorTimeout:     time.Second,
		ReplicationChannelSize:    100,
		BatchRequestsChannelSize:  1000,
		Shards:                    shardsInfo,
		MonitoringListenAddress:   "127.0.0.1:0",
		Consenter: config.ConsenterInfo{
			PartyID:    myParty,
			Endpoint:   consenterEndpoint,
			PublicKey:  generateRandomBytes(t, 16),
			TLSCACerts: []config.RawBytes{generateRandomBytes(t, 16)},
		},
		UseTLS:             true,
		ClientAuthRequired: false,
	}

	return test
}

func (at *assemblerTest) SendBAToAssembler(oba *state.AvailableBatchOrdered) {
	at.consensusBAChan <- oba
	at.expectedLedgerBA = append(at.expectedLedgerBA, oba)
}

func (at *assemblerTest) SendBatchToAssembler(batch types.Batch) {
	at.shardToBatcherChan[batch.Shard()] <- batch
}

func (at *assemblerTest) StopAssembler() {
	for _, batcherChan := range at.shardToBatcherChan {
		close(batcherChan)
	}
	close(at.consensusBAChan)
	at.assembler.Stop()
}

func (at *assemblerTest) StartAssembler() {
	at.shardToBatcherChan = make(map[types.ShardID]chan types.Batch)
	at.consensusBAChan = make(chan *state.AvailableBatchOrdered, 100_000)

	prefetchIndexerFactory := &assembler.DefaultPrefetchIndexerFactory{}

	prefetcherFactoryMock := &assembler_mocks.FakePrefetcherFactory{}
	prefetcherFactoryMock.CreateCalls(func(si []types.ShardID, pi1 []types.PartyID, pi2 assembler.PrefetchIndexer, bb assembler.BatchBringer, l types.Logger) assembler.PrefetcherController {
		return at.prefetcherMock
	})

	batchBringerFactoryMock := &assembler_mocks.FakeBatchBringerFactory{}
	batchBringerFactoryMock.CreateCalls(func(m map[types.ShardID]map[types.PartyID]types.BatchSequence, anc *config.AssemblerNodeConfig, l types.Logger) assembler.BatchBringer {
		return at.batchBringerMock
	})
	for _, shardId := range at.shards {
		batchChan := make(chan types.Batch, 100_000)
		at.shardToBatcherChan[shardId] = batchChan
	}
	at.batchBringerMock.ReplicateCalls(func(si types.ShardID) <-chan types.Batch {
		return at.shardToBatcherChan[si]
	})

	at.prefetcherMock.StartCalls(func() {
		for _, shard := range at.shards {
			rep := at.batchBringerMock.Replicate(types.ShardID(shard))
			go func(repCh <-chan types.Batch) {
				for b := range rep {
					at.prefetchIndexMock.Put(b)
				}
			}(rep)
		}
	})

	consensusBringerFactoryMock := &delivery_mocks.FakeConsensusBringerFactory{}
	consensusBringerFactoryMock.CreateCalls(func(rb1 []config.RawBytes, rb2, rb3 config.RawBytes, s string, al node_ledger.AssemblerLedgerReaderWriter, l types.Logger) delivery.ConsensusBringer {
		return at.consensusBringerMock
	})
	at.consensusBringerMock.ReplicateCalls(func() <-chan *state.AvailableBatchOrdered {
		return at.consensusBAChan
	})

	at.assembler = assembler.NewDefaultAssembler(
		at.logger,
		&dummyAssemblerStopper{},
		at.nodeConfig,
		at.genesisBlock,
		&node_ledger.DefaultAssemblerLedgerFactory{},
		prefetchIndexerFactory,
		prefetcherFactoryMock,
		batchBringerFactoryMock,
		consensusBringerFactoryMock,
	)
}

func TestAssembler_StartPanicsSinceGenesisBlockIsNil(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], nil)

	// Act
	require.PanicsWithValue(t, "Error creating Assembler1, config block is nil", func() {
		test.StartAssembler()
	})
}

func TestAssembler_StartAndThenStopShouldOnlyWriteGenesisBlockToLedger(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// Act
	test.StartAssembler()
	<-time.After(100 * time.Millisecond)
	test.StopAssembler()

	// Assert
	al, err := node_ledger.NewAssemblerLedger(test.logger, test.ledgerDir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), al.Ledger.Height())
	genesisBlock, err := al.Ledger.RetrieveBlockByNumber(0)
	require.NoError(t, err)
	require.True(t, protoutil.IsConfigBlock(genesisBlock))
	al.Close()
}

func TestAssembler_SkipNonGenesisConfigBlock(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	var blockNumber uint64 = 7
	configBlock := tx.CreateConfigBlock(blockNumber, []byte("config block data"))

	test := setupAssemblerTest(t, shards, parties, parties[0], configBlock)

	// Act
	test.StartAssembler()
	<-time.After(100 * time.Millisecond)
	test.StopAssembler()

	// Assert
	al, err := node_ledger.NewAssemblerLedger(test.logger, test.ledgerDir)
	require.NoError(t, err)
	require.Equal(t, uint64(0), al.Ledger.Height())
	genesisBlock, err := al.Ledger.RetrieveBlockByNumber(blockNumber)
	require.ErrorContains(t, err, "no such block number")
	require.Nil(t, genesisBlock)
	al.Close()
}

func TestAssembler_RestartWithoutAddingBatchesShouldWork(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// Act & Assert
	test.StartAssembler()
	<-time.After(100 * time.Millisecond)
	test.StopAssembler()
	test.StartAssembler()
	<-time.After(100 * time.Millisecond)
	test.StopAssembler()
}

func TestAssembler_StopCallsAllSubcomponents(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// Act
	test.StartAssembler()
	<-time.After(100 * time.Millisecond)
	test.StopAssembler()

	// Assert
	require.Equal(t, 1, test.consensusBringerMock.StopCallCount())
	require.Equal(t, 1, test.prefetcherMock.StopCallCount())
}

func TestAssembler_RecoveryWhenPartialDecisionWrittenToLedger(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))
	test.StartAssembler()
	batches := []types.Batch{
		createTestBatchWithSize(1, 1, 1, []int{1}),
		createTestBatchWithSize(1, 1, 2, []int{1}),
	}

	// Act
	test.SendBAToAssembler(test.orderedBatchAttestationCreator.Append(batches[0], 1, 0, 2))
	test.SendBatchToAssembler(batches[0])

	require.Eventually(t, func() bool {
		return test.prefetcherMock.StartCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)

	require.Eventually(t, func() bool {
		return test.batchBringerMock.ReplicateCallCount() == 2
	}, eventuallyTimeout, eventuallyTick)

	require.Eventually(t, func() bool {
		return test.prefetchIndexMock.PutCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)

	test.StopAssembler()
	test.StartAssembler()

	// Assert
	require.Eventually(t, func() bool {
		return test.consensusBringerMock.ReplicateCallCount() == 2
	}, eventuallyTimeout, eventuallyTick)
}
