/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/updatable_block_verifier.go --fake-name UpdatableBlockVerifier . updatableBlockVerifier
//lint:ignore U1000 used to generate mock
type updatableBlockVerifier interface {
	deliverclient.CloneableUpdatableBlockVerifier
}

//go:generate counterfeiter -o mocks/orderer_config.go --fake-name OrdererConfig . ordererConfig
//lint:ignore U1000 used to generate mock
type ordererConfig interface {
	channelconfig.Orderer
}

func TestBFTSynchronizer(t *testing.T) {
	flogging.ActivateSpec("debug")
	blockBytes, err := os.ReadFile("testdata/mychannel.block") // TODO produce block on-the-fly, this is from fabric v3
	require.NoError(t, err)

	goodConfigBlock := &cb.Block{}
	require.NoError(t, proto.Unmarshal(blockBytes, goodConfigBlock))

	b42 := makeConfigBlockWithMetadata(goodConfigBlock, 42, &smartbftprotos.ViewMetadata{ViewId: 1, LatestSequence: 8})
	b99 := makeBlockWithMetadata(99, 42, &smartbftprotos.ViewMetadata{ViewId: 1, LatestSequence: 12})
	b100 := makeBlockWithMetadata(100, 42, &smartbftprotos.ViewMetadata{ViewId: 1, LatestSequence: 13})
	b101 := makeConfigBlockWithMetadata(goodConfigBlock, 101, &smartbftprotos.ViewMetadata{ViewId: 2, LatestSequence: 1})
	b102 := makeBlockWithMetadata(102, 101, &smartbftprotos.ViewMetadata{ViewId: 2, LatestSequence: 3})

	blockNum2configSqn := map[uint64]uint64{
		99:  7,
		100: 7,
		101: 8,
		102: 8,
	}

	t.Run("no remote endpoints but myself", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:1": 100,
			},
			"example.com:1",
			nil,
		)

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightReturns(100)
		fakeCS.BlockReturns(b99)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b99 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)

		require.NotPanics(t, bftSynchronizer.Stop)
	})

	t.Run("no remote endpoints", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(map[string]uint64{}, "", nil)

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightReturns(100)
		fakeCS.BlockReturns(b99)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b99 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)

		require.NotPanics(t, bftSynchronizer.Stop)
	})

	t.Run("error creating block puller", func(t *testing.T) {
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(nil, errors.New("oops"))

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightReturns(100)
		fakeCS.BlockReturns(b99)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b99 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)
		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)
	})

	t.Run("no remote endpoints above my height", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:1": 100,
				"example.com:2": 100,
			},
			"example.com:1",
			nil,
		)

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightReturns(100)
		fakeCS.BlockReturns(b99)
		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b99 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)
	})

	t.Run("startHeight is 0, fetch genesis block and sync", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:2": 3,
				"example.com:3": 3,
				"example.com:4": 3,
			},
			"example.com:1",
			nil,
		)

		// Genesis block (block 0) that will be fetched
		genesisBlock := makeConfigBlockWithMetadata(goodConfigBlock, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 0})

		// Mock GenesisByEndpoints to return matching genesis blocks from F+1 endpoints
		bp.GenesisByEndpointsReturns(
			map[string]*cb.Block{
				"example.com:2": genesisBlock,
				"example.com:3": genesisBlock,
				"example.com:4": genesisBlock,
			},
			nil,
		)

		b1 := makeBlockWithMetadata(1, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 1})
		b2 := makeBlockWithMetadata(2, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 2})

		var ledger []*cb.Block
		// Start with empty ledger (height 0)

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})
		fakeCS.BlockCalls(func(u uint64) *cb.Block {
			if u >= uint64(len(ledger)) {
				return nil
			}
			return ledger[u]
		})
		fakeCS.WriteConfigBlockCalls(func(b *cb.Block, m []byte) {
			ledger = append(ledger, b)
		})
		fakeCS.WriteBlockSyncCalls(func(b *cb.Block, m []byte) {
			ledger = append(ledger, b)
		})
		fakeCS.LastConfigBlockCalls(func(b *cb.Block) (*cb.Block, error) {
			// get last config index
			rawLastConfig, err := protoutil.GetMetadataFromBlock(b, cb.BlockMetadataIndex_LAST_CONFIG)
			if err != nil {
				return nil, errors.Wrap(err, "failed getting proposed block metadata last config")
			}
			lastConf := &cb.LastConfig{}
			if err := proto.Unmarshal(rawLastConfig.Value, lastConf); err != nil {
				return nil, errors.Wrap(err, "failed unmarshaling proposed block metadata last config")
			}
			lastConfigBlock := ledger[lastConf.Index]

			return lastConfigBlock, nil
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeOrdererConfig.BatchSizeReturns(&orderer.BatchSize{
			MaxMessageCount:   100,
			AbsoluteMaxBytes:  1000000,
			PreferredMaxBytes: 500000,
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		fakeVerifierFactory := &mocks.VerifierFactory{}
		fakeVerifier := &mocks.UpdatableBlockVerifier{}
		fakeVerifierFactory.CreateBlockVerifierReturns(fakeVerifier, nil)

		fakeBFTDelivererFactory := &mocks.BFTDelivererFactory{}
		fakeBFTDeliverer := &mocks.BFTBlockDeliverer{}
		fakeBFTDelivererFactory.CreateBFTDelivererReturns(fakeBFTDeliverer)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{2, 2, 2, 2}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b2 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit: func(block *cb.Block) types.Reconfig {
				// Return the expected reconfig for the last block
				if block == b2 {
					return types.Reconfig{
						InLatestDecision: false,
						CurrentNodes:     []uint64{1, 2, 3, 4},
						CurrentConfig:    types.Configuration{SelfID: 1},
					}
				}
				return types.Reconfig{}
			},
			Support:             fakeCS,
			ClusterDialer:       &comm.PredicateDialer{Config: comm.ClientConfig{}},
			LocalConfigCluster:  config.Cluster{},
			BlockPullerFactory:  bpf,
			VerifierFactory:     fakeVerifierFactory,
			BFTDelivererFactory: fakeBFTDelivererFactory,
			Logger:              flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		wg := sync.WaitGroup{}
		wg.Add(1)
		stopDeliverCh := make(chan struct{})
		fakeBFTDeliverer.DeliverBlocksCalls(func() {
			b := bftSynchronizer.Buffer()
			require.NotNil(t, b)
			err := b.HandleBlock("mychannel", b1)
			require.NoError(t, err)
			err = b.HandleBlock("mychannel", b2)
			require.NoError(t, err)
			<-stopDeliverCh // the goroutine will block here
			wg.Done()
		})
		fakeBFTDeliverer.StopCalls(func() {
			close(stopDeliverCh)
		})

		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)

		// Verify genesis block was written
		require.Equal(t, 3, len(ledger), "ledger should contain genesis block + 2 synced blocks")
		require.Equal(t, uint64(0), ledger[0].Header.Number, "first block should be genesis")
		require.Equal(t, uint64(1), ledger[1].Header.Number)
		require.Equal(t, uint64(2), ledger[2].Header.Number)

		// Verify WriteConfigBlock was called for genesis block
		require.Equal(t, 1, fakeCS.WriteConfigBlockCallCount(), "genesis block should be written as config block")
		// Verify WriteBlockSync was called for the 2 regular blocks
		require.Equal(t, 2, fakeCS.WriteBlockSyncCallCount(), "2 regular blocks should be written")

		// Verify GenesisByEndpoints was called
		require.Equal(t, 1, bp.GenesisByEndpointsCallCount(), "should fetch genesis block")

		wg.Wait()

		require.NotPanics(t, bftSynchronizer.Stop)
	})

	t.Run("startHeight is 0, fetch genesis block fails - different genesis blocks", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:2": 3,
				"example.com:3": 3,
				"example.com:4": 3,
			},
			"example.com:1",
			nil,
		)

		// Create different genesis blocks for each endpoint
		genesisBlock1 := makeConfigBlockWithMetadata(goodConfigBlock, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 0})
		genesisBlock2 := makeConfigBlockWithMetadata(goodConfigBlock, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 1}) // Different metadata
		genesisBlock3 := makeConfigBlockWithMetadata(goodConfigBlock, 0, &smartbftprotos.ViewMetadata{ViewId: 1, LatestSequence: 0}) // Different ViewId

		// Mock GenesisByEndpoints to return different genesis blocks from each endpoint
		// With 4 nodes (F=1), we need at least 2 matching blocks, but here all 3 are different
		bp.GenesisByEndpointsReturns(
			map[string]*cb.Block{
				"example.com:2": genesisBlock1,
				"example.com:3": genesisBlock2,
				"example.com:4": genesisBlock3,
			},
			nil,
		)

		var ledger []*cb.Block
		// Start with empty ledger (height 0)

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})
		fakeCS.BlockCalls(func(u uint64) *cb.Block {
			if u >= uint64(len(ledger)) {
				return nil
			}
			return ledger[u]
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				return &decision.Latest
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		// The synchronizer should panic because it cannot find F+1 matching genesis blocks
		require.Panics(t, func() {
			bftSynchronizer.Sync()
		}, "Should panic when genesis blocks don't match")

		// Verify genesis block was NOT written
		require.Equal(t, 0, len(ledger), "ledger should remain empty when genesis fetch fails")
		require.Equal(t, 0, fakeCS.WriteConfigBlockCallCount(), "no blocks should be written")
		require.Equal(t, 0, fakeCS.WriteBlockSyncCallCount(), "no blocks should be written")

		// Verify GenesisByEndpoints was called
		require.Equal(t, 1, bp.GenesisByEndpointsCallCount(), "should attempt to fetch genesis block")
	})

	t.Run("startHeight is 0, fetch genesis block fails - single genesis block", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:2": 3,
			},
			"example.com:1",
			nil,
		)

		// Create a single genesis block
		genesisBlock := makeConfigBlockWithMetadata(goodConfigBlock, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 0})

		// With 4 nodes (F=1), we need at least F+1=2 matching blocks
		// Here we have only 1 genesis block returned from a single endpoint
		bp.GenesisByEndpointsReturns(
			map[string]*cb.Block{
				"example.com:2": genesisBlock,
			},
			nil,
		)

		var ledger []*cb.Block

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				return &decision.Latest
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		// The synchronizer should panic because it cannot find F+1 matching genesis blocks
		require.Panics(t, func() {
			bftSynchronizer.Sync()
		}, "Should panic when only a single genesis block is returned")

		// Verify genesis block was NOT written
		require.Equal(t, 0, len(ledger), "ledger should remain empty when genesis fetch fails")
		require.Equal(t, 0, fakeCS.WriteConfigBlockCallCount(), "no blocks should be written")

		// Verify GenesisByEndpoints was called
		require.Equal(t, 1, bp.GenesisByEndpointsCallCount(), "should attempt to fetch genesis block")
	})

	t.Run("startHeight is 0, fetch genesis block fails - error from GenesisByEndpoints", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:2": 3,
				"example.com:3": 3,
				"example.com:4": 3,
			},
			"example.com:1",
			nil,
		)

		// Mock GenesisByEndpoints to return an error
		bp.GenesisByEndpointsReturns(
			nil,
			errors.New("network error: unable to fetch genesis blocks"),
		)

		var ledger []*cb.Block

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				return &decision.Latest
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		// The synchronizer should panic because GenesisByEndpoints returned an error
		require.Panics(t, func() {
			bftSynchronizer.Sync()
		}, "Should panic when GenesisByEndpoints returns an error")

		// Verify genesis block was NOT written
		require.Equal(t, 0, len(ledger), "ledger should remain empty when genesis fetch fails")
		require.Equal(t, 0, fakeCS.WriteConfigBlockCallCount(), "no blocks should be written")

		// Verify GenesisByEndpoints was called
		require.Equal(t, 1, bp.GenesisByEndpointsCallCount(), "should attempt to fetch genesis block")
	})

	t.Run("startHeight is 0, fetch genesis block fails - nil blocks from endpoints", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:2": 3,
				"example.com:3": 3,
				"example.com:4": 3,
			},
			"example.com:1",
			nil,
		)

		genesisBlock := makeConfigBlockWithMetadata(goodConfigBlock, 0, &smartbftprotos.ViewMetadata{ViewId: 0, LatestSequence: 0})

		// Mock GenesisByEndpoints to return nil blocks from some endpoints
		// With 4 nodes (F=1), we need at least 2 matching blocks, but only 1 valid block is returned
		bp.GenesisByEndpointsReturns(
			map[string]*cb.Block{
				"example.com:2": genesisBlock,
				"example.com:3": nil, // Nil block
				"example.com:4": nil, // Nil block
			},
			nil,
		)

		var ledger []*cb.Block

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				return &decision.Latest
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		// The synchronizer should panic because only 1 valid block is returned (less than F+1)
		require.Panics(t, func() {
			bftSynchronizer.Sync()
		}, "Should panic when nil blocks prevent F+1 matches")

		// Verify genesis block was NOT written
		require.Equal(t, 0, len(ledger), "ledger should remain empty when genesis fetch fails")
		require.Equal(t, 0, fakeCS.WriteConfigBlockCallCount(), "no blocks should be written")

		// Verify GenesisByEndpoints was called
		require.Equal(t, 1, bp.GenesisByEndpointsCallCount(), "should attempt to fetch genesis block")
	})

	t.Run("remote endpoints above my height: 2 blocks", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:1": 100,
				"example.com:2": 101,
				"example.com:3": 102,
				"example.com:4": 103,
			},
			"example.com:1",
			nil,
		)

		var ledger []*cb.Block
		for i := uint64(0); i < 100; i++ {
			ledger = append(ledger, &cb.Block{Header: &cb.BlockHeader{Number: i}})
		}
		ledger[42] = b42
		ledger[99] = b99

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})
		fakeCS.BlockCalls(func(u uint64) *cb.Block {
			b := ledger[u]
			t.Logf("Block Calls: %d, %v", u, b)
			return ledger[u]
		})
		fakeCS.SequenceCalls(func() uint64 { return blockNum2configSqn[uint64(len(ledger))] })
		fakeCS.WriteConfigBlockCalls(func(b *cb.Block, m []byte) {
			ledger = append(ledger, b)
		})
		fakeCS.WriteBlockSyncCalls(func(b *cb.Block, m []byte) {
			ledger = append(ledger, b)
		})
		fakeCS.LastConfigBlockCalls(func(b *cb.Block) (*cb.Block, error) {
			// get last config index
			rawLastConfig, err := protoutil.GetMetadataFromBlock(b, cb.BlockMetadataIndex_LAST_CONFIG)
			if err != nil {
				return nil, errors.Wrap(err, "failed getting proposed block metadata last config")
			}
			lastConf := &cb.LastConfig{}
			if err := proto.Unmarshal(rawLastConfig.Value, lastConf); err != nil {
				return nil, errors.Wrap(err, "failed unmarshaling proposed block metadata last config")
			}
			lastConfigBlock := ledger[lastConf.Index]

			return lastConfigBlock, nil
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeOrdererConfig.BatchSizeReturns(&orderer.BatchSize{
			MaxMessageCount:   100,
			AbsoluteMaxBytes:  1000000,
			PreferredMaxBytes: 500000,
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		fakeVerifierFactory := &mocks.VerifierFactory{}
		fakeVerifier := &mocks.UpdatableBlockVerifier{}
		fakeVerifierFactory.CreateBlockVerifierReturns(fakeVerifier, nil)

		fakeBFTDelivererFactory := &mocks.BFTDelivererFactory{}
		fakeBFTDeliverer := &mocks.BFTBlockDeliverer{}
		fakeBFTDelivererFactory.CreateBFTDelivererReturns(fakeBFTDeliverer)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: true,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b101 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit: func(block *cb.Block) types.Reconfig {
				if block == b101 {
					return types.Reconfig{
						InLatestDecision: true,
						CurrentNodes:     []uint64{1, 2, 3, 4},
						CurrentConfig:    types.Configuration{SelfID: 1},
					}
				}
				return types.Reconfig{}
			},
			Support:             fakeCS,
			ClusterDialer:       &comm.PredicateDialer{Config: comm.ClientConfig{}},
			LocalConfigCluster:  config.Cluster{},
			BlockPullerFactory:  bpf,
			VerifierFactory:     fakeVerifierFactory,
			BFTDelivererFactory: fakeBFTDelivererFactory,
			Logger:              flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)

		wg := sync.WaitGroup{}
		wg.Add(1)
		stopDeliverCh := make(chan struct{})
		fakeBFTDeliverer.DeliverBlocksCalls(func() {
			b := bftSynchronizer.Buffer()
			require.NotNil(t, b)
			err := b.HandleBlock("mychannel", b100)
			require.NoError(t, err)
			err = b.HandleBlock("mychannel", b101)
			require.NoError(t, err)
			<-stopDeliverCh // the goroutine will block here
			wg.Done()
		})
		fakeBFTDeliverer.StopCalls(func() {
			close(stopDeliverCh)
		})

		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)
		require.Equal(t, 102, len(ledger))
		require.Equal(t, 1, fakeCS.WriteBlockSyncCallCount())
		require.Equal(t, 1, fakeCS.WriteConfigBlockCallCount())
		wg.Wait()

		require.NotPanics(t, bftSynchronizer.Stop)
	})

	t.Run("remote endpoints above my height: 3 blocks", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:1": 100,
				"example.com:2": 103,
				"example.com:3": 103,
				"example.com:4": 200,
			},
			"example.com:1",
			nil,
		)

		var ledger []*cb.Block
		for i := uint64(0); i < 100; i++ {
			ledger = append(ledger, &cb.Block{Header: &cb.BlockHeader{Number: i}})
		}
		ledger[42] = b42
		ledger[99] = b99

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			return uint64(len(ledger))
		})
		fakeCS.BlockCalls(func(u uint64) *cb.Block {
			b := ledger[u]
			t.Logf("Block Calls: %d, %v", u, b)
			return ledger[u]
		})
		fakeCS.SequenceCalls(func() uint64 { return blockNum2configSqn[uint64(len(ledger))] })
		fakeCS.WriteConfigBlockCalls(func(b *cb.Block, m []byte) {
			ledger = append(ledger, b)
		})
		fakeCS.WriteBlockSyncCalls(func(b *cb.Block, m []byte) {
			ledger = append(ledger, b)
		})
		fakeCS.LastConfigBlockCalls(func(b *cb.Block) (*cb.Block, error) {
			// get last config index
			rawLastConfig, err := protoutil.GetMetadataFromBlock(b, cb.BlockMetadataIndex_LAST_CONFIG)
			if err != nil {
				return nil, errors.Wrap(err, "failed getting proposed block metadata last config")
			}
			lastConf := &cb.LastConfig{}
			if err := proto.Unmarshal(rawLastConfig.Value, lastConf); err != nil {
				return nil, errors.Wrap(err, "failed unmarshaling proposed block metadata last config")
			}
			lastConfigBlock := ledger[lastConf.Index]

			return lastConfigBlock, nil
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeOrdererConfig.BatchSizeReturns(&orderer.BatchSize{
			MaxMessageCount:   100,
			AbsoluteMaxBytes:  1000000,
			PreferredMaxBytes: 500000,
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		fakeVerifierFactory := &mocks.VerifierFactory{}
		fakeVerifier := &mocks.UpdatableBlockVerifier{}
		fakeVerifierFactory.CreateBlockVerifierReturns(fakeVerifier, nil)

		fakeBFTDelivererFactory := &mocks.BFTDelivererFactory{}
		fakeBFTDeliverer := &mocks.BFTBlockDeliverer{}
		fakeBFTDelivererFactory.CreateBFTDelivererReturns(fakeBFTDeliverer)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: true,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b102 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit: func(block *cb.Block) types.Reconfig {
				if block == b101 {
					return types.Reconfig{
						InLatestDecision: true,
						CurrentNodes:     []uint64{1, 2, 3, 4},
						CurrentConfig:    types.Configuration{SelfID: 1},
					}
				} else if block == b102 {
					return types.Reconfig{
						InLatestDecision: false,
						CurrentNodes:     []uint64{1, 2, 3, 4},
						CurrentConfig:    types.Configuration{SelfID: 1},
					}
				}
				return types.Reconfig{}
			},
			Support:             fakeCS,
			ClusterDialer:       &comm.PredicateDialer{Config: comm.ClientConfig{}},
			LocalConfigCluster:  config.Cluster{},
			BlockPullerFactory:  bpf,
			VerifierFactory:     fakeVerifierFactory,
			BFTDelivererFactory: fakeBFTDelivererFactory,
			Logger:              flogging.MustGetLogger("test.smartbft"),
		}
		require.NotNil(t, bftSynchronizer)

		wg := sync.WaitGroup{}
		wg.Add(1)
		stopDeliverCh := make(chan struct{})
		fakeBFTDeliverer.DeliverBlocksCalls(func() {
			b := bftSynchronizer.Buffer()
			require.NotNil(t, b)
			err := b.HandleBlock("mychannel", b100)
			require.NoError(t, err)
			err = b.HandleBlock("mychannel", b101)
			require.NoError(t, err)
			err = b.HandleBlock("mychannel", b102)
			require.NoError(t, err)

			<-stopDeliverCh // the goroutine will block here
			wg.Done()
		})
		fakeBFTDeliverer.StopCalls(func() {
			close(stopDeliverCh)
		})

		resp := bftSynchronizer.Sync()
		require.NotNil(t, resp)
		require.Equal(t, *decision, resp)
		require.Equal(t, 103, len(ledger))
		require.Equal(t, 2, fakeCS.WriteBlockSyncCallCount())
		require.Equal(t, 1, fakeCS.WriteConfigBlockCallCount())
		wg.Wait()

		require.Eventually(t, func() bool { return fakeBFTDeliverer.StopCallCount() == 1 }, 10*time.Second, time.Millisecond)
	})

	t.Run("synchronizer can be stopped", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		// everyone is above my height, but I will stop the synchronizer after I get just one block, so it should not pull all blocks.
		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:1": 100,
				"example.com:2": 100000,
				"example.com:3": 100000,
				"example.com:4": 100000,
			},
			"example.com:1",
			nil,
		)

		var ledgerLock sync.Mutex
		var ledger []*cb.Block
		for i := uint64(0); i < 100; i++ {
			ledger = append(ledger, &cb.Block{Header: &cb.BlockHeader{Number: i}})
		}
		ledger[42] = b42
		ledger[99] = b99

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightCalls(func() uint64 {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			return uint64(len(ledger))
		})
		fakeCS.BlockCalls(func(u uint64) *cb.Block {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			return ledger[u]
		})
		fakeCS.SequenceCalls(func() uint64 {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			return blockNum2configSqn[uint64(len(ledger))]
		})
		fakeCS.WriteConfigBlockCalls(func(b *cb.Block, m []byte) {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			ledger = append(ledger, b)
		})
		fakeCS.WriteBlockSyncCalls(func(b *cb.Block, m []byte) {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			ledger = append(ledger, b)
		})
		fakeCS.LastConfigBlockCalls(func(b *cb.Block) (*cb.Block, error) {
			// get last config index
			rawLastConfig, err := protoutil.GetMetadataFromBlock(b, cb.BlockMetadataIndex_LAST_CONFIG)
			if err != nil {
				return nil, errors.Wrap(err, "failed getting proposed block metadata last config")
			}
			lastConf := &cb.LastConfig{}
			if err := proto.Unmarshal(rawLastConfig.Value, lastConf); err != nil {
				return nil, errors.Wrap(err, "failed unmarshaling proposed block metadata last config")
			}

			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			lastConfigBlock := ledger[lastConf.Index]

			return lastConfigBlock, nil
		})

		fakeOrdererConfig := &mocks.OrdererConfig{}
		fakeOrdererConfig.ConsentersReturns([]*cb.Consenter{
			{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4},
		})
		fakeOrdererConfig.BatchSizeReturns(&orderer.BatchSize{
			MaxMessageCount:   100,
			AbsoluteMaxBytes:  1000000,
			PreferredMaxBytes: 500000,
		})
		fakeCS.SharedConfigReturns(fakeOrdererConfig)

		fakeVerifierFactory := &mocks.VerifierFactory{}
		fakeVerifier := &mocks.UpdatableBlockVerifier{}
		fakeVerifierFactory.CreateBlockVerifierReturns(fakeVerifier, nil)

		fakeBFTDelivererFactory := &mocks.BFTDelivererFactory{}
		fakeBFTDeliverer := &mocks.BFTBlockDeliverer{}
		fakeBFTDelivererFactory.CreateBFTDelivererReturns(fakeBFTDeliverer)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 2}, {ID: 3}, {ID: 4}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				return &decision.Latest
			},
			OnCommit: func(block *cb.Block) types.Reconfig {
				return types.Reconfig{
					InLatestDecision: false,
					CurrentNodes:     []uint64{1, 2, 3, 4},
					CurrentConfig:    types.Configuration{SelfID: 1},
				}
			},
			Support:             fakeCS,
			ClusterDialer:       &comm.PredicateDialer{Config: comm.ClientConfig{}},
			LocalConfigCluster:  config.Cluster{},
			BlockPullerFactory:  bpf,
			VerifierFactory:     fakeVerifierFactory,
			BFTDelivererFactory: fakeBFTDelivererFactory,
			Logger:              flogging.MustGetLogger("test.smartbft"),
		}
		require.NotNil(t, bftSynchronizer)

		wg := sync.WaitGroup{}
		wg.Add(1)
		pauseDeliverCh := make(chan struct{})
		doneDeliverCh := make(chan struct{})

		fakeBFTDeliverer.DeliverBlocksCalls(func() {
			b := bftSynchronizer.Buffer()
			require.NotNil(t, b)
			err := b.HandleBlock("mychannel", b100)
			require.NoError(t, err)

			wg.Done()
			<-pauseDeliverCh // the goroutine will block here

			num := uint64(101)
			for {
				time.Sleep(time.Microsecond)
				// keep trying to deliver blocks until the synchronizer is stopped, which will cause the buffer to be closed and HandleBlock to return an error
				blk := makeBlockWithMetadata(num, 42, &smartbftprotos.ViewMetadata{ViewId: 1, LatestSequence: 13})
				err := b.HandleBlock("mychannel", blk)
				if err != nil {
					close(doneDeliverCh)
					return
				}
			}
		})

		go func() {
			resp := bftSynchronizer.Sync()
			require.NotNil(t, resp)
			require.Equal(t, *decision, resp)
		}()

		// wait until the first block is delivered and processed
		wg.Wait()

		// wait until the synchronizer pulls at least one block (it should pull block 100)
		require.Eventually(t, func() bool {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			return len(ledger) > 100
		}, 10*time.Second, 10*time.Millisecond)

		bftSynchronizer.Stop()
		close(pauseDeliverCh)
		<-doneDeliverCh

		require.Eventually(t, func() bool {
			ledgerLock.Lock()
			defer ledgerLock.Unlock()

			return len(ledger) >= 101
		}, 10*time.Second, 10*time.Millisecond)

		require.Equal(t, len(ledger)-100, fakeCS.WriteBlockSyncCallCount())
		require.Equal(t, 0, fakeCS.WriteConfigBlockCallCount())
		require.Eventually(t, func() bool { return fakeBFTDeliverer.StopCallCount() == 1 }, 10*time.Second, time.Millisecond)
	})
}

func TestBFTSynchronizer_Stop(t *testing.T) {
	blockBytes, err := os.ReadFile("testdata/mychannel.block") // TODO produce block on-the-fly, this is from fabric v3
	require.NoError(t, err)

	goodConfigBlock := &cb.Block{}
	require.NoError(t, proto.Unmarshal(blockBytes, goodConfigBlock))

	b99 := makeBlockWithMetadata(99, 42, &smartbftprotos.ViewMetadata{ViewId: 1, LatestSequence: 12})

	t.Run("stop before Synch", func(t *testing.T) {
		bp := &mocks.FakeHeightDetector{}
		bpf := &mocks.FakeHeightDetectorFactory{}
		bpf.CreateHeightDetectorReturns(bp, nil)

		bp.HeightsByEndpointsReturns(
			map[string]uint64{
				"example.com:1": 100,
			},
			"example.com:1",
			nil,
		)

		fakeCS := &mocks.FakeConsenterSupport{}
		fakeCS.HeightReturns(100)
		fakeCS.BlockReturns(b99)

		decision := &types.SyncResponse{
			Latest: types.Decision{
				Proposal:   types.Proposal{Header: []byte{1, 1, 1, 1}},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{
				InReplicatedDecisions: false,
				CurrentNodes:          []uint64{1, 2, 3, 4},
				CurrentConfig:         types.Configuration{SelfID: 1},
			},
		}

		bftSynchronizer := &synchronizer.BFTSynchronizer{
			LatestConfig: func() (types.Configuration, []uint64) {
				return types.Configuration{
					SelfID: 1,
				}, []uint64{1, 2, 3, 4}
			},
			BlockToDecision: func(block *cb.Block) *types.Decision {
				if block == b99 {
					return &decision.Latest
				}
				return nil
			},
			OnCommit:           noopUpdateLastHash,
			Support:            fakeCS,
			LocalConfigCluster: config.Cluster{},
			BlockPullerFactory: bpf,
			Logger:             flogging.MustGetLogger("test.smartbft"),
		}

		require.NotNil(t, bftSynchronizer)
		require.NotPanics(t, bftSynchronizer.Stop)
	})
}
