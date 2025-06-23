/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"context"
	"sync"
	"time"

	arma_types "github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	"github.ibm.com/decentralized-trust-research/arma/node/delivery"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
)

type synchronizer struct {
	deliver                  func(proposal smartbft_types.Proposal, signatures []smartbft_types.Signature)
	pruneRequestsFromMemPool func([]byte)
	getBlock                 func(seq uint64) *common.Block
	getHeight                func() uint64
	CurrentNodes             []uint64
	BFTConfig                smartbft_types.Configuration
	logger                   arma_types.Logger
	endpoint                 func() string
	cc                       comm.ClientConfig
	nextSeq                  func() uint64
	lock                     sync.Mutex
	memStore                 map[uint64]*common.Block
	latestCommittedBlock     uint64
	stopSync                 func()
}

func (s *synchronizer) OnAppend(block *common.Block) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.latestCommittedBlock = block.Header.Number
	delete(s.memStore, block.Header.Number)
}

func (s *synchronizer) stop() {
	if s.stopSync != nil {
		s.stopSync()
	}
}

func (s *synchronizer) run() {
	var stopCtx context.Context
	stopCtx, s.stopSync = context.WithCancel(context.Background())

	requestEnvelopeFactoryFunc := func() *common.Envelope {
		requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
			common.HeaderType_DELIVER_SEEK_INFO,
			"consensus",
			nil,
			delivery.NextSeekInfo(s.nextSeq()),
			int32(0),
			uint64(0),
			nil,
		)
		if err != nil {
			s.logger.Panicf("Failed creating signed envelope: %v", err)
		}

		return requestEnvelope
	}

	blockHandlerFunc := func(block *common.Block) {
		for s.memStoreTooBig() {
			time.Sleep(time.Second)
			s.logger.Infof("Mem store is too big, waiting for BFT to catch up")
		}

		s.lock.Lock()
		defer s.lock.Unlock()

		if s.latestCommittedBlock >= block.Header.Number {
			return
		}

		s.memStore[block.Header.Number] = block
	}

	go delivery.Pull(stopCtx, "consensus-synchronizer", s.logger, s.endpoint, requestEnvelopeFactoryFunc, s.cc, blockHandlerFunc, nil)
}

func (s *synchronizer) memStoreTooBig() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.memStore) > 100
}

func (s *synchronizer) Sync() smartbft_types.SyncResponse {
	height := s.currentHeight()

	lastSeqInLedger := height - 1
	latestBlock := s.getBlock(lastSeqInLedger)

	// Iterate over blocks retrieved from pulling asynchronously and commit them.

	nextSeqToCommit := lastSeqInLedger + 1
	for {
		s.lock.Lock()
		retrievedBlock, exists := s.memStore[nextSeqToCommit]
		s.lock.Unlock()

		if !exists {
			break
		}

		latestBlock = retrievedBlock
		nextSeqToCommit++

		proposal, signatures, err := state.BytesToDecision(latestBlock.Data.Data[0])
		if err != nil {
			s.logger.Panicf("Failed parsing block we pulled: %v", err)
		}

		var batch arma_types.BatchedRequests
		if err := batch.Deserialize(proposal.Payload); err != nil {
			s.logger.Panicf("Failed deserializing proposal payload: %v", err)
		}

		for _, req := range batch {
			s.pruneRequestsFromMemPool(req)
		}

		s.deliver(proposal, signatures)
	}

	proposal, signatures, err := state.BytesToDecision(latestBlock.Data.Data[0])
	if err != nil {
		s.logger.Panicf("Failed parsing block we pulled: %v", err)
	}

	return smartbft_types.SyncResponse{
		Reconfig: smartbft_types.ReconfigSync{
			CurrentConfig: s.BFTConfig,
			CurrentNodes:  s.CurrentNodes,
		},
		Latest: smartbft_types.Decision{Proposal: proposal, Signatures: signatures},
	}
}

func (s *synchronizer) currentHeight() uint64 {
	height := s.getHeight()
	for height == 0 {
		time.Sleep(time.Second)
		height = s.getHeight()
	}
	return height
}
