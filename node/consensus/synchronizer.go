package consensus

import (
	"context"
	"sync"
	"time"

	arma_types "arma/common/types"
	"arma/node/comm"
	"arma/node/delivery"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/protoutil"
)

type synchronizer struct {
	deliver                  func(proposal types.Proposal, signatures []types.Signature)
	pruneRequestsFromMemPool func([]byte)
	getBlock                 func(seq uint64) *common.Block
	getHeight                func() uint64
	CurrentNodes             []uint64
	BFTConfig                types.Configuration
	logger                   arma_types.Logger
	endpoint                 func() string
	cc                       comm.ClientConfig
	nextSeq                  func() uint64
	lock                     sync.Mutex
	memStore                 map[uint64]*common.Block
	latestCommittedBlock     uint64
}

func (s *synchronizer) OnAppend(block *common.Block) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.latestCommittedBlock = block.Header.Number

	delete(s.memStore, block.Header.Number)
}

func (s *synchronizer) run() {
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

	go delivery.Pull(context.Background(), "consensus", s.logger, s.endpoint, requestEnvelope, s.cc, func(block *common.Block) {
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
	})
}

func (s *synchronizer) memStoreTooBig() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.memStore) > 100
}

func (s *synchronizer) Sync() types.SyncResponse {
	height := s.currentHeight()

	lastSeqInLedger := height - 1
	latestBlock := s.getBlock(lastSeqInLedger)

	s.lock.Lock()
	defer s.lock.Unlock()

	// Iterate over blocks retrieved from pulling asynchronously from the leader,
	// and commit them.

	nextSeqToCommit := lastSeqInLedger + 1
	for {
		retrievedBlock, exists := s.memStore[nextSeqToCommit]
		if !exists {
			break
		}

		latestBlock = retrievedBlock
		nextSeqToCommit++

		proposal, signatures, err := bytesToDecision(latestBlock.Data.Data[0])
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

	proposal, signatures, err := bytesToDecision(latestBlock.Data.Data[0])
	if err != nil {
		s.logger.Panicf("Failed parsing block we pulled: %v", err)
	}

	return types.SyncResponse{
		Reconfig: types.ReconfigSync{
			CurrentConfig: s.BFTConfig,
			CurrentNodes:  s.CurrentNodes,
		},
		Latest: types.Decision{Proposal: proposal, Signatures: signatures},
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
