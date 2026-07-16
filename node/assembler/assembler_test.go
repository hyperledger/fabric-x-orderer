/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	orderer_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	assembler_mocks "github.com/hyperledger/fabric-x-orderer/node/assembler/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/assembler/synchronizer"
	synchronizer_mocks "github.com/hyperledger/fabric-x-orderer/node/assembler/synchronizer/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	delivery_mocks "github.com/hyperledger/fabric-x-orderer/node/delivery/mocks"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	ledger_mocks "github.com/hyperledger/fabric-x-orderer/node/ledger/mocks"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
)

type assemblerTest struct {
	logger                         *flogging.FabricLogger
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
	synchronizerFactoryMock        *synchronizer_mocks.FakeSynchronizerFactory
}

type dummyAssemblerStopper struct{}

func (d *dummyAssemblerStopper) Stop() {}
func (d *dummyAssemblerStopper) Address() string {
	return ""
}

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
		synchronizerFactoryMock:        &synchronizer_mocks.FakeSynchronizerFactory{},
	}
	test.synchronizerFactoryMock.CreateSynchronizerReturns(&synchronizer_mocks.FakeSynchronizerWithStop{})
	assemblerEndpoint := ""
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
		Operations: &operations.Operations{
			ListenAddress: "127.0.0.1:0",
		},
		Metrics: &operations.Metrics{
			Provider:           "disabled",
			MetricsLogInterval: 10 * time.Second,
		},
		Consenter: config.ConsenterInfo{
			PartyID:    myParty,
			Endpoint:   consenterEndpoint,
			PublicKey:  generateRandomBytes(t, 16),
			TLSCACerts: []config.RawBytes{generateRandomBytes(t, 16)},
		},
		UseTLS:             false,
		ClientAuthRequired: false,
		Bundle:             testutil.CreateAssemblerBundleForTest(0),
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

func (at *assemblerTest) SoftStopAssembler() {
	for _, batcherChan := range at.shardToBatcherChan {
		close(batcherChan)
	}
	close(at.consensusBAChan)
	at.assembler.SoftStop()
}

func (at *assemblerTest) StartAssembler() {
	at.shardToBatcherChan = make(map[types.ShardID]chan types.Batch)
	at.consensusBAChan = make(chan *state.AvailableBatchOrdered, 100_000)

	prefetchIndexerFactory := &assembler.DefaultPrefetchIndexerFactory{}

	prefetcherFactoryMock := &assembler_mocks.FakePrefetcherFactory{}
	prefetcherFactoryMock.CreateCalls(func(si []types.ShardID, pi1 []types.PartyID, pi2 assembler.PrefetchIndexer, bb assembler.BatchBringer, m *assembler.Metrics, l *flogging.FabricLogger) assembler.PrefetcherController {
		return at.prefetcherMock
	})

	batchBringerFactoryMock := &assembler_mocks.FakeBatchBringerFactory{}
	batchBringerFactoryMock.CreateCalls(func(m map[types.ShardID]map[types.PartyID]types.BatchSequence, anc *config.AssemblerNodeConfig, l *flogging.FabricLogger) assembler.BatchBringer {
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
	consensusBringerFactoryMock.CreateCalls(func(channelID string, rb1 []config.RawBytes, rb2, rb3 config.RawBytes, s string, al node_ledger.AssemblerLedgerReaderWriter, l *flogging.FabricLogger) delivery.ConsensusBringer {
		return at.consensusBringerMock
	})
	at.consensusBringerMock.ReplicateCalls(func() <-chan *state.AvailableBatchOrdered {
		return at.consensusBAChan
	})

	at.assembler = assembler.NewDefaultAssembler(
		at.logger,
		&dummyAssemblerStopper{},
		at.nodeConfig,
		&orderer_config.Configuration{},
		at.genesisBlock,
		make(chan struct{}),
		&node_ledger.DefaultAssemblerLedgerFactory{},
		prefetchIndexerFactory,
		prefetcherFactoryMock,
		batchBringerFactoryMock,
		consensusBringerFactoryMock,
		&mocks.SignerSerializer{},
		at.synchronizerFactoryMock,
		&synchronizer_mocks.VerifierFactory{},
	)

	at.assembler.StartAssemblerService()
}

func (at *assemblerTest) WaitAssemblerRunning(t *testing.T) {
	require.Eventually(t, func() bool {
		status := at.assembler.GetStatus()
		return status.State == node_utils.StateRunning
	}, eventuallyTimeout, eventuallyTick)
	time.Sleep(100 * time.Millisecond) // wait for grpc sever to start, before attemting to close it in the test
}

func (at *assemblerTest) WaitAssemblerStopped(t *testing.T) {
	require.Eventually(t, func() bool {
		status := at.assembler.GetStatus()
		return status.State == node_utils.StateStopped
	}, eventuallyTimeout, eventuallyTick)
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
	test.WaitAssemblerRunning(t)

	test.StopAssembler()
	test.WaitAssemblerStopped(t)

	// Assert
	al, err := node_ledger.NewAssemblerLedger(test.logger, test.ledgerDir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), al.Ledger.Height())
	genesisBlock, err := al.Ledger.RetrieveBlockByNumber(0)
	require.NoError(t, err)
	require.True(t, protoutil.IsConfigBlock(genesisBlock))
	al.Close()
}

func TestAssembler_RestartWithoutAddingBatchesShouldWork(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// Act & Assert
	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	test.StopAssembler()
	test.WaitAssemblerStopped(t)

	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	test.StopAssembler()
}

func TestAssembler_StopCallsAllSubcomponents(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// Act
	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	test.StopAssembler()
	test.WaitAssemblerStopped(t)

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
	test.WaitAssemblerRunning(t)
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
	test.WaitAssemblerStopped(t)
	test.StartAssembler()
	test.WaitAssemblerRunning(t)

	// Assert
	require.Eventually(t, func() bool {
		return test.consensusBringerMock.ReplicateCallCount() == 2
	}, eventuallyTimeout, eventuallyTick)
	test.StopAssembler()
}

func TestAssemblerStatusSoftStop(t *testing.T) {
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	require.Equal(t, test.assembler.GetStatus().ConfigSequenceNumber, uint64(0))

	test.SoftStopAssembler()

	require.Eventually(t, func() bool {
		status := test.assembler.GetStatus()
		return status.State == node_utils.StateSoftStopped && status.ConfigSequenceNumber == 0
	}, eventuallyTimeout, eventuallyTick)

	test.assembler.Stop()
	test.WaitAssemblerStopped(t)
	require.Equal(t, test.assembler.GetStatus().ConfigSequenceNumber, uint64(0))
}

func TestAssemblerStatusStop(t *testing.T) {
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	require.Equal(t, test.assembler.GetStatus().ConfigSequenceNumber, uint64(0))

	test.StopAssembler()
	test.WaitAssemblerStopped(t)
	require.Equal(t, test.assembler.GetStatus().ConfigSequenceNumber, uint64(0))
}

// Scenario:
// 1. An assembler is started with a genesis config block (block number 0) and an empty ledger.
// 2. initLedger appends the genesis block directly to the ledger.
// 3. No synchronizer is created, and the ledger height is exactly 1.
func TestAssembler_InitLedgerGenesisBlockDoesNotSync(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// Act
	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	test.StopAssembler()
	test.WaitAssemblerStopped(t)

	// Assert: no synchronizer is created for the genesis path, and only the genesis block is written.
	require.Equal(t, 0, test.synchronizerFactoryMock.CreateSynchronizerCallCount())

	al, err := node_ledger.NewAssemblerLedger(test.logger, test.ledgerDir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), al.Ledger.Height())
	al.Close()
}

// Scenario:
// 1. An assembler is started with a config block whose number is ahead of the empty ledger's height.
// 2. initLedger creates a synchronizer with the expected arguments (self id, target height, config block, cluster config).
// 3. The synchronizer's Sync() commits the missing blocks (genesis and the config block) to the ledger.
// 4. The ledger is populated up to the target height.
func TestAssembler_InitLedgerSyncsWhenConfigBlockAheadOfLedger(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	var configBlockNumber uint64 = 1
	configBlock := tx.CreateConfigBlock(configBlockNumber, []byte("config block data"))
	test := setupAssemblerTest(t, shards, parties, parties[0], configBlock)

	// The fake synchronizer simulates pulling and committing the missing blocks (0..configBlockNumber)
	// to the ledger via the support adapter, so that after Sync() the ledger reaches the target height.
	var fakeSync *synchronizer_mocks.FakeSynchronizerWithStop
	test.synchronizerFactoryMock.CreateSynchronizerCalls(func(_ *flogging.FabricLogger, _ uint64, _ orderer_config.Cluster, support synchronizer.AssemblerSupport, _ bccsp.BCCSP, _ uint64, bootConfigBlock *common.Block, _ synchronizer.VerifierFactory) synchronizer.SynchronizerWithStop {
		fakeSync = &synchronizer_mocks.FakeSynchronizerWithStop{}
		fakeSync.SyncCalls(func() error {
			genesis := utils.EmptyGenesisBlock("arma")
			support.WriteConfigBlock(genesis)
			// chain the config block to the genesis block so the ledger accepts it.
			bootConfigBlock.Header.PreviousHash = protoutil.BlockHeaderHash(genesis.Header)
			support.WriteConfigBlock(bootConfigBlock)
			return nil
		})
		return fakeSync
	})

	// Act
	test.StartAssembler()
	test.WaitAssemblerRunning(t)

	// Assert: exactly one synchronizer was created with the expected parameters, and Sync() ran once.
	require.Equal(t, 1, test.synchronizerFactoryMock.CreateSynchronizerCallCount())
	_, selfID, cluster, support, _, targetHeight, passedConfigBlock, _ := test.synchronizerFactoryMock.CreateSynchronizerArgsForCall(0)
	require.Equal(t, uint64(test.party), selfID)
	require.Equal(t, configBlockNumber+1, targetHeight)
	require.Equal(t, configBlock, passedConfigBlock)
	require.NotNil(t, support)
	require.Equal(t, "assemblerSync", cluster.ReplicationPolicy)
	require.Equal(t, 100, cluster.SendBufferSize)
	require.Equal(t, []byte(test.nodeConfig.TLSCertificateFile), cluster.ClientCertificate)
	require.Equal(t, []byte(test.nodeConfig.TLSPrivateKeyFile), cluster.ClientPrivateKey)
	require.NotNil(t, fakeSync)
	require.Equal(t, 1, fakeSync.SyncCallCount())

	test.StopAssembler()
	test.WaitAssemblerStopped(t)

	// Assert: the ledger was populated by the synchronizer up to the target height.
	al, err := node_ledger.NewAssemblerLedger(test.logger, test.ledgerDir)
	require.NoError(t, err)
	require.Equal(t, configBlockNumber+1, al.Ledger.Height())
	al.Close()
}

// Scenario:
// 1. An assembler is started with a config block whose number is ahead of the empty ledger's height.
// 2. initLedger creates a synchronizer and runs Sync(), which returns an error.
// 3. Assembler initialization aborts with a panic.
func TestAssembler_InitLedgerPanicsWhenSyncFails(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	configBlock := tx.CreateConfigBlock(7, []byte("config block data"))
	test := setupAssemblerTest(t, shards, parties, parties[0], configBlock)

	fakeSync := &synchronizer_mocks.FakeSynchronizerWithStop{}
	fakeSync.SyncReturns(errors.New("failed to reach quorum"))
	test.synchronizerFactoryMock.CreateSynchronizerReturns(fakeSync)

	// Act & Assert
	require.Panics(t, func() { test.StartAssembler() })
	require.Equal(t, 1, test.synchronizerFactoryMock.CreateSynchronizerCallCount())
	require.Equal(t, 1, fakeSync.SyncCallCount())
}

// Scenario:
// 1. An assembler is started with a config block whose number is ahead of the empty ledger's height.
// 2. initLedger creates a synchronizer and runs Sync(), which returns successfully but writes no block.
// 3. initLedger detects the ledger is still empty and panics rather than starting with an empty ledger.
func TestAssembler_InitLedgerPanicsWhenLedgerEmptyAfterSync(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	configBlock := tx.CreateConfigBlock(7, []byte("config block data"))
	test := setupAssemblerTest(t, shards, parties, parties[0], configBlock)
	// The default fake synchronizer returns nil from Sync() without writing any block.

	// Act & Assert
	require.Panics(t, func() { test.StartAssembler() })
	require.Equal(t, 1, test.synchronizerFactoryMock.CreateSynchronizerCallCount())
}

// TestAssembler_InitLedgerDoesNotSyncWhenConfigBlockAlreadyInLedger
// Scenario:
// 1. An assembler is started with a genesis config block, writing it to the ledger (height becomes 1).
// 2. The assembler is stopped and then restarted with the same genesis config block, whose number is now below the ledger height.
// 3. initLedger takes neither the genesis nor the sync path, and the assembler restarts without creating a synchronizer.
func TestAssembler_InitLedgerDoesNotSyncWhenConfigBlockAlreadyInLedger(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0], utils.EmptyGenesisBlock("arma"))

	// First start writes the genesis block, bringing the ledger height to 1.
	test.StartAssembler()
	test.WaitAssemblerRunning(t)
	test.StopAssembler()
	test.WaitAssemblerStopped(t)

	// Act: restart with the same genesis config block (number 0), now below the ledger height (1).
	test.StartAssembler()
	test.WaitAssemblerRunning(t)

	// Assert: no synchronizer was created across either start.
	require.Equal(t, 0, test.synchronizerFactoryMock.CreateSynchronizerCallCount())

	test.StopAssembler()
	test.WaitAssemblerStopped(t)
}
