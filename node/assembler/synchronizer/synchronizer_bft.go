/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// AssemblerBFTSynchronizer brings the local assembler ledger up to TargetHeight by pulling
// committed blocks from the assemblers. It is used when the assembler falls behind (or
// starts fresh) and needs to catch up before it can resume normal operation.
//
// Synchronization proceeds in up to two phases:
//
//  1. Genesis bootstrap (only when the local ledger is empty, i.e. height 0). The genesis block
//     is fetched from every assembler endpoint and the block agreed upon by at least f+1 peers is
//     accepted, tolerating up to f Byzantine faults. The accepted block is written to the ledger.
//
//  2. BFT delivery. A BFT block deliverer streams blocks starting from the current height into an
//     internal SyncBuffer, from which they are drained, verified, and written to the ledger (via
//     WriteConfigBlock for config blocks, WriteBlockSync otherwise) up to and including
//     TargetHeight-1.
//
// Lifecycle: call Sync() once to run synchronization to completion (or until an error/timeout).
// Stop() may be called concurrently from another goroutine to abort an in-progress Sync(); it
// signals the SyncBuffer to stop, which unblocks the pulling loop.
//
// All fields other than the unexported mutex/syncBuffer are expected to be set before Sync() is
// called and are treated as immutable thereafter.
type AssemblerBFTSynchronizer struct {
	Logger      *flogging.FabricLogger
	SelfPartyID uint64
	// TargetHeight is the ledger height to reach (exclusive block number); must be > 0. Sync
	// pulls blocks until the ledger reaches this height, i.e. through block TargetHeight-1.
	TargetHeight uint64
	// Support provides ledger access (height, blocks, config lookup, writes), the channel/shared
	// config, and this node's signing identity.
	Support            AssemblerSupport
	CryptoProvider     bccsp.BCCSP
	ClusterDialer      *comm.PredicateDialer
	LocalConfigCluster config.Cluster
	// BlockPullerFactory creates the genesis fetcher used during the genesis bootstrap phase.
	BlockPullerFactory GenesisFetcherFactory
	// VerifierFactory creates the block verifier used to validate delivered blocks.
	VerifierFactory VerifierFactory
	// BFTDelivererFactory creates the BFT block deliverer used during the delivery phase.
	BFTDelivererFactory BFTDelivererFactory
	// JoinConfigBlock is the bootstrap config block supplied by the assembler. It provides the
	// live orderer endpoint list used to seed the BFT deliverer and may be distinct from the
	// ledger's last config block, which is used for block verification.
	JoinConfigBlock *common.Block

	mutex      sync.Mutex
	syncBuffer *SyncBuffer
}

func (a *AssemblerBFTSynchronizer) Sync() error {
	a.Logger.Debugf("Starting Assembler Synchronizer")
	return a.synchronize()
}

func (a *AssemblerBFTSynchronizer) Stop() {
	a.Logger.Infof("Stopping Assembler Synchronizer")
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.syncBuffer != nil {
		a.syncBuffer.Stop()
	}
}

// Buffer return the internal SyncBuffer for testability.
func (s *AssemblerBFTSynchronizer) Buffer() *SyncBuffer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.syncBuffer
}

func (a *AssemblerBFTSynchronizer) synchronize() error {
	startHeight := a.Support.Height()
	if a.TargetHeight == 0 {
		return fmt.Errorf("error synchronizing assembler: targetHeight must be > 0")
	}

	if startHeight == a.TargetHeight {
		a.Logger.Infof("Assembler already at target height %d", a.TargetHeight)
		return nil
	}

	if startHeight > a.TargetHeight {
		a.Logger.Warnf("Assembler is ahead of target height: startHeight %d is greater than targetHeight %d", startHeight, a.TargetHeight)
		return nil
	}

	if startHeight == 0 {
		genesisBlock, err := a.fetchGenesisBlock()
		if err != nil {
			a.Logger.Panicf("Cannot join the cluster: %s", errors.Wrap(err, "failed to fetch genesis block"))
		}
		a.Support.WriteConfigBlock(genesisBlock)
		startHeight = a.Support.Height()
		a.Logger.Infof("Fetched and wrote genesis block, new height: %d, party: %d", startHeight, a.SelfPartyID)
	}

	if startHeight == a.TargetHeight {
		a.Logger.Infof("Assembler reached target height %d", a.TargetHeight)
		return nil
	}

	capacityBlocks := uint(100)
	a.mutex.Lock()
	a.syncBuffer = NewSyncBuffer(capacityBlocks)
	a.mutex.Unlock()

	// Create the BFT block deliverer
	bftDeliverer, err := a.createBFTDeliverer(startHeight, arma_types.PartyID(a.SelfPartyID))
	if err != nil {
		return errors.Wrapf(err, "cannot create BFT block deliverer")
	}

	// Start a go-routine that fetches block and inserts them into the syncBuffer.
	go bftDeliverer.DeliverBlocks()
	defer bftDeliverer.Stop()

	_, err = a.getBlocksFromSyncBuffer(startHeight, a.TargetHeight)
	if err != nil {
		return errors.Wrap(err, "failed to get any blocks from SyncBuffer")
	}

	return nil
}

func (a *AssemblerBFTSynchronizer) getBlocksFromSyncBuffer(startHeight, targetHeight uint64) (*common.Block, error) {
	targetSeq := targetHeight - 1
	seq := startHeight
	var blocksFetched int
	a.Logger.Debugf("Will fetch sequences [%d-%d]", seq, targetSeq)

	var lastPulledBlock *common.Block
	for seq <= targetSeq {
		block := a.syncBuffer.PullBlock(seq)
		if block == nil {
			a.Logger.Debugf("Failed to fetch block [%d] from cluster", seq)
			break
		}
		if protoutil.IsConfigBlock(block) {
			a.Support.WriteConfigBlock(block)
			a.Logger.Debugf("Fetched and committed config block [%d] from cluster", seq)
		} else {
			a.Support.WriteBlockSync(block)
			a.Logger.Debugf("Fetched and committed block [%d] from cluster", seq)
		}
		lastPulledBlock = block

		seq++
		blocksFetched++
	}

	a.syncBuffer.Stop()

	if lastPulledBlock == nil {
		return nil, errors.Errorf("failed pulling block %d", seq)
	}

	a.Logger.Infof("Finished synchronizing with cluster, fetched %d blocks, starting from block [%d], up until and including block [%d]",
		blocksFetched, startHeight, lastPulledBlock.Header.Number)

	return lastPulledBlock, nil
}

// createBFTDeliverer creates and initializes the BFT block deliverer.
func (a *AssemblerBFTSynchronizer) createBFTDeliverer(startHeight uint64, myParty arma_types.PartyID) (BFTBlockDeliverer, error) {
	if startHeight == 0 {
		return nil, errors.New("cannot create BFT deliverer from empty ledger (startHeight=0)")
	}

	lastBlock := a.Support.Block(startHeight - 1)
	if lastBlock == nil || lastBlock.Header == nil {
		return nil, errors.Errorf("failed to retrieve last block %d from ledger", startHeight-1)
	}

	ledgerLastConfigBlock, err := a.Support.LastConfigBlock(lastBlock)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve last config block from ledger")
	}
	if ledgerLastConfigBlock == nil || ledgerLastConfigBlock.Header == nil {
		return nil, errors.New("last config block from ledger is nil")
	}

	joinConfigBlock := a.JoinConfigBlock
	if joinConfigBlock == nil || joinConfigBlock.Header == nil {
		return nil, errors.New("join config block is nil")
	}

	a.Logger.Infof("Creating BFTDeliverer, last Block in ledger: %d, last config block in ledger: %d, join config block: %d", lastBlock.Header.Number, ledgerLastConfigBlock.Header.Number, joinConfigBlock.Header.Number)

	blockOps := &utils.CommonConfigBlockOperations{}
	lastConfigEnv, err := blockOps.ConfigFromBlock(joinConfigBlock)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve last config envelope")
	}

	var updatableVerifier deliverclient.CloneableUpdatableBlockVerifier
	updatableVerifier, err = a.VerifierFactory.CreateBlockVerifier(ledgerLastConfigBlock, lastBlock, a.CryptoProvider, a.Logger)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create BlockVerificationAssistant")
	}

	clientConfig := a.ClusterDialer.Config // The cluster and block puller use slightly different options
	clientConfig.AsyncConnect = false
	clientConfig.SecOpts.VerifyCertificate = nil

	block, _ := pem.Decode(clientConfig.SecOpts.Certificate)
	if block == nil {
		return nil, errors.Errorf("failed to decode TLS certificate: %v", string(clientConfig.SecOpts.Certificate))
	}
	tlsCertHash := util.ComputeSHA256(block.Bytes)

	// The maximal amount of time to wait before retrying to connect.
	maxRetryInterval := 10 * time.Second // TODO s.LocalConfigCluster.ReplicationRetryTimeout
	// The minimal amount of time to wait before retrying. The retry interval doubles after every unsuccessful attempt.
	minRetryInterval := maxRetryInterval / 50
	// The maximal duration of a Sync. After this time Sync returns with whatever it had pulled until that point.
	maxRetryDuration := time.Minute // TODO s.LocalConfigCluster.ReplicationPullTimeout * time.Duration(s.LocalConfigCluster.ReplicationMaxRetries)
	// If a remote orderer does not deliver blocks for this amount of time, even though it can do so, it is replaced as the block deliverer.
	blockCensorshipTimeOut := maxRetryDuration / 3

	bftDeliverer := a.BFTDelivererFactory.CreateBFTDeliverer(
		a.Support.ChannelID(),
		a.syncBuffer,
		&ledgerInfoAdapter{a.Support},
		updatableVerifier,
		blocksprovider.DialerAdapter{ClientConfig: clientConfig},
		&orderers.ConnectionSourceFactory{}, // no overrides in the orderer
		a.CryptoProvider,
		make(chan struct{}),
		a.Support,
		blocksprovider.DeliverAdapter{},
		&blocksprovider.BFTCensorshipMonitorFactory{},
		&AssemblerEndpointsExtractor{},
		flogging.MustGetLogger("orderer.blocksprovider").With("channel", a.Support.ChannelID()),
		minRetryInterval,
		maxRetryInterval,
		blockCensorshipTimeOut,
		maxRetryDuration,
		func() (stopRetries bool) {
			a.syncBuffer.Stop()
			return true // In the orderer we must limit the time we try to do Synch()
		},
		tlsCertHash,
	)

	a.Logger.Infof("Created a BFTDeliverer ")
	bftDeliverer.Initialize(lastConfigEnv.GetConfig(), myParty)

	return bftDeliverer, nil
}

// fetchGenesisBlock fetches the genesis block from remote orderers.
// TODO make this method stoppable, currently it can take a long time if remote endpoints are not responsive, and we have no way to interrupt it.
func (a *AssemblerBFTSynchronizer) fetchGenesisBlock() (*common.Block, error) {
	a.Logger.Infof("Fetching genesis block, party: %d", a.SelfPartyID)
	blockPuller, err := a.BlockPullerFactory.CreateGenesisFetcher(arma_types.PartyID(a.SelfPartyID), a.Support, a.ClusterDialer, a.LocalConfigCluster, a.CryptoProvider, a.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create GenesisFetcher")
	}
	defer blockPuller.Close()

	genesisByEndpoint, err := blockPuller.GenesisByEndpoints()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get GenesisByEndpoints")
	}

	a.Logger.Infof("Received genesis blocks from %d endpoints: %v", len(genesisByEndpoint), slices.Collect(maps.Keys(genesisByEndpoint)))

	// Calculate required matches
	clusterSize := len(a.Support.SharedConfig().Consenters())
	f, requiredMatches, _ := utils.ComputeFTQ(uint16(clusterSize))
	a.Logger.Infof("Cluster size: %d, F: %d, required matches: %d", clusterSize, f, requiredMatches)

	// Count occurrences of each genesis block by hash
	blockCounts := make(map[string]int)
	blockByHash := make(map[string]*common.Block)
	endpointToHash := []string{}
	for endpoint, block := range genesisByEndpoint {
		if block == nil {
			a.Logger.Warnf("Nil genesis block from endpoint: %s", endpoint)
			continue
		}

		blockBytes, err := proto.Marshal(block)
		if err != nil {
			a.Logger.Warnf("Cannot marshal genesis block from endpoint: %s; err: %s", endpoint, err)
			continue
		}

		blockHash := sha256.Sum256(blockBytes)
		blockHashStr := hex.EncodeToString(blockHash[:])

		blockCounts[blockHashStr]++
		blockByHash[blockHashStr] = block
		endpointToHash = append(endpointToHash, fmt.Sprintf("[EP: %s, H: %s]", endpoint, blockHashStr))
	}

	// Find a block that appears at least F+1 times
	for blockHash, count := range blockCounts {
		if count >= int(requiredMatches) {
			genesisBlock := blockByHash[blockHash]
			a.Logger.Infof("Found genesis block with %d matching copies (required: %d)", count, requiredMatches)
			return genesisBlock, nil
		}
	}

	return nil, errors.Errorf("could not find genesis block with at least %d matching copies: %+v", requiredMatches, endpointToHash)
}
