package assembler_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/assembler"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	"github.ibm.com/decentralized-trust-research/arma/node/delivery"
	node_ledger "github.ibm.com/decentralized-trust-research/arma/node/ledger"

	assembler_mocks "github.ibm.com/decentralized-trust-research/arma/node/assembler/mocks"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	delivery_mocks "github.ibm.com/decentralized-trust-research/arma/node/delivery/mocks"
	ledger_mocks "github.ibm.com/decentralized-trust-research/arma/node/ledger/mocks"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type assemblerTest struct {
	logger                         types.Logger
	shards                         []types.ShardID
	party                          types.PartyID
	ledgerDir                      string
	nodeConfig                     *config.AssemblerNodeConfig
	orderedBatchAttestationCreator *OrderedBatchAttestationCreator
	expecedLedgerBA                []core.OrderedBatchAttestation
	assembler                      *assembler.Assembler
	shardToBatcherChan             map[types.ShardID]chan core.Batch
	consensusBAChan                chan core.OrderedBatchAttestation
	batchBringerMock               *assembler_mocks.FakeBatchBringer
	ledgerMock                     *ledger_mocks.FakeAssemblerLedgerReaderWriter
	prefetcherMock                 *assembler_mocks.FakePrefetcherController
	prefetchIndexMock              *assembler_mocks.FakePrefetchIndexer
	consensusBringerMock           *delivery_mocks.FakeConsensusBringer
}

func generateRandomBytes(t *testing.T, size int) []byte {
	b := make([]byte, size)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func createLedgerMockWrappingRealLedger(logger types.Logger, ledgerPath string) (*ledger_mocks.FakeAssemblerLedgerReaderWriter, node_ledger.AssemblerLedgerReaderWriter, error) {
	mock := &ledger_mocks.FakeAssemblerLedgerReaderWriter{}
	ledger, err := node_ledger.NewAssemblerLedger(logger, ledgerPath)

	mock.AppendCalls(func(b core.Batch, i interface{}) {
		ledger.Append(b, i)
	})
	mock.AppendConfigCalls(func(b *common.Block, dn types.DecisionNum) {
		ledger.AppendConfig(b, dn)
	})
	mock.BatchFrontierCalls(func(si []types.ShardID, pi []types.PartyID, d time.Duration) (map[types.ShardID]map[types.PartyID]types.BatchSequence, error) {
		return ledger.BatchFrontier(si, pi, d)
	})
	mock.CloseCalls(func() {
		ledger.Close()
	})
	mock.GetTxCountCalls(func() uint64 {
		return ledger.GetTxCount()
	})
	mock.LastOrderingInfoCalls(func() (*state.OrderingInformation, error) {
		return ledger.LastOrderingInfo()
	})
	mock.LedgerReaderCalls(func() blockledger.Reader {
		return ledger.LedgerReader()
	})
	return mock, ledger, err
}

func setupAssemblerTest(t *testing.T, shards []types.ShardID, parties []types.PartyID, myParty types.PartyID) *assemblerTest {
	orderedBatchAttestationCreator, _ := NewOrderedBatchAttestationCreator()
	test := &assemblerTest{
		logger:                         testutil.CreateLoggerForModule(t, "assembler", zap.DebugLevel),
		shards:                         shards,
		party:                          myParty,
		ledgerDir:                      t.TempDir(),
		orderedBatchAttestationCreator: orderedBatchAttestationCreator,
		expecedLedgerBA:                []core.OrderedBatchAttestation{},
		batchBringerMock:               &assembler_mocks.FakeBatchBringer{},
		ledgerMock:                     &ledger_mocks.FakeAssemblerLedgerReaderWriter{},
		prefetcherMock:                 &assembler_mocks.FakePrefetcherController{},
		prefetchIndexMock:              &assembler_mocks.FakePrefetchIndexer{},
		consensusBringerMock:           &delivery_mocks.FakeConsensusBringer{},
	}
	assemblerEndpoint := "assembler"
	consenterEndpoint := "concenter"

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
		TLSPrivateKeyFile:  generateRandomBytes(t, 16),
		TLSCertificateFile: generateRandomBytes(t, 16),
		PartyId:            test.party,
		Directory:          test.ledgerDir,
		ListenAddress:      assemblerEndpoint,
		UseTLS:             true,
		Consenter: config.ConsenterInfo{
			PartyID:    myParty,
			Endpoint:   consenterEndpoint,
			PublicKey:  generateRandomBytes(t, 16),
			TLSCACerts: []config.RawBytes{generateRandomBytes(t, 16)},
		},
		Shards: shardsInfo,
	}

	return test
}

func (at *assemblerTest) SendBAToAssembler(oba core.OrderedBatchAttestation) {
	at.consensusBAChan <- oba
	at.expecedLedgerBA = append(at.expecedLedgerBA, oba)
}

func (at *assemblerTest) SendBatchToAssembler(batch core.Batch) {
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
	at.shardToBatcherChan = make(map[types.ShardID]chan core.Batch)
	at.consensusBAChan = make(chan core.OrderedBatchAttestation, 100_000)

	prefetchIndexerFactory := &assembler.DefaultPrefetchIndexerFactory{}

	prefetcherFactoryMock := &assembler_mocks.FakePrefetcherFactory{}
	prefetcherFactoryMock.CreateCalls(func(si []types.ShardID, pi1 []types.PartyID, pi2 assembler.PrefetchIndexer, bb assembler.BatchBringer, l types.Logger) assembler.PrefetcherController {
		return at.prefetcherMock
	})

	batchBringerFactoryMock := &assembler_mocks.FakeBatchBringerFactory{}
	batchBringerFactoryMock.CreateCalls(func(m map[types.ShardID]map[types.PartyID]types.BatchSequence, anc config.AssemblerNodeConfig, l types.Logger) assembler.BatchBringer {
		return at.batchBringerMock
	})
	for _, shardId := range at.shards {
		batchChan := make(chan core.Batch, 100_000)
		at.shardToBatcherChan[shardId] = batchChan
	}
	at.batchBringerMock.ReplicateCalls(func(si types.ShardID) <-chan core.Batch {
		return at.shardToBatcherChan[si]
	})

	consensusBringerFactoryMock := &delivery_mocks.FakeConsensusBringerFactory{}
	consensusBringerFactoryMock.CreateCalls(func(rb1 []config.RawBytes, rb2, rb3 config.RawBytes, s string, l types.Logger) delivery.ConsensusBringer {
		return at.consensusBringerMock
	})
	at.consensusBringerMock.ReplicateCalls(func(acp core.AssemblerConsensusPosition) <-chan core.OrderedBatchAttestation {
		return at.consensusBAChan
	})

	ledgerFactory := &ledger_mocks.FakeAssemblerLedgerFactory{}
	ledgerFactory.CreateCalls(func(l types.Logger, s string) (node_ledger.AssemblerLedgerReaderWriter, error) {
		mock, _, err := createLedgerMockWrappingRealLedger(l, s)
		at.ledgerMock = mock
		return mock, err
	})

	at.assembler = assembler.NewDefaultAssembler(
		at.logger,
		*at.nodeConfig,
		nil,
		ledgerFactory,
		prefetchIndexerFactory,
		prefetcherFactoryMock,
		batchBringerFactoryMock,
		consensusBringerFactoryMock,
	)
}

func TestAssembler_StartAndThenStopShouldOnlyWriteGenesisBlockToLedger(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0])

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

func TestAssembler_RestartWithoutAddingBatchesShouldWork(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0])

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
	test := setupAssemblerTest(t, shards, parties, parties[0])

	// Act
	test.StartAssembler()
	<-time.After(100 * time.Millisecond)
	test.StopAssembler()

	// Assert
	require.Equal(t, 1, test.consensusBringerMock.StopCallCount())
	require.Equal(t, 1, test.prefetcherMock.StopCallCount())
	require.Equal(t, 1, test.ledgerMock.CloseCallCount())
}

func TestAssembler_RecoveryWhenmPartialDecisionWrittenToLedger(t *testing.T) {
	// Arrange
	shards := []types.ShardID{1, 2}
	parties := []types.PartyID{1, 2, 3}
	test := setupAssemblerTest(t, shards, parties, parties[0])
	test.StartAssembler()
	batches := []core.Batch{
		testutil.CreateMockBatch(1, 1, 1, []int{1}),
		testutil.CreateMockBatch(1, 1, 2, []int{1}),
	}

	// Act
	test.SendBAToAssembler(test.orderedBatchAttestationCreator.Append(batches[0], 1, 0, 2))
	test.SendBatchToAssembler(batches[0])
	require.Eventually(t, func() bool {
		return test.ledgerMock.AppendCallCount() == 1
	}, eventuallyTimeout, eventuallyTick)
	test.StopAssembler()
	test.StartAssembler()

	// Assert
	require.Eventually(t, func() bool {
		return test.consensusBringerMock.ReplicateCallCount() == 2
	}, eventuallyTimeout, eventuallyTick)
	position := test.consensusBringerMock.ReplicateArgsForCall(1)
	require.Equal(t, types.DecisionNum(1), position.DecisionNum)
	require.Equal(t, 1, position.BatchIndex)
}
