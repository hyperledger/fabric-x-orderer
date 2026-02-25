/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"context"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"

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
	logger                   *flogging.FabricLogger
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

func (s *synchronizer) Stop() {
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

		if s.latestCommittedBlock >= block.Header.Number && s.latestCommittedBlock != 0 {
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
	height := s.getHeight()

	var lastSeqInLedger uint64
	var latestBlock *common.Block
	var nextSeqToCommit uint64
	if height > 0 {
		lastSeqInLedger = height - 1
		latestBlock = s.getBlock(lastSeqInLedger)
		nextSeqToCommit = lastSeqInLedger + 1
	}

	// Iterate over blocks retrieved from pulling asynchronously and commit them.
	for {
		s.lock.Lock()
		retrievedBlock, exists := s.memStore[nextSeqToCommit]
		s.lock.Unlock()

		if !exists {
			if nextSeqToCommit == 0 {
				continue
			}
			break
		}

		latestBlock = retrievedBlock
		nextSeqToCommit++

		proposal, signatures, err := state.BytesToDecision(latestBlock.Data.Data[0])
		if err != nil {
			s.logger.Panicf("Failed parsing block we pulled: %v", err)
		}

		hdr := &state.Header{}
		if err := hdr.Deserialize(proposal.Header); err != nil {
			s.logger.Panicf("Failed deserializing header: %v", err)
		}

		if hdr.DecisionNumOfLastConfigBlock != hdr.Num { // not a config block
			var batch arma_types.BatchedRequests
			if err := batch.Deserialize(proposal.Payload); err != nil {
				s.logger.Panicf("Failed deserializing proposal payload: %v", err)
			}

			for _, req := range batch {
				s.pruneRequestsFromMemPool(req)
			}
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
