/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/request_inspector.go . RequestInspector
type RequestInspector interface {
	RequestID(req []byte) string
}

//go:generate counterfeiter -o mocks/mem_pool.go . MemPool
type MemPool interface {
	NextRequests(ctx context.Context) [][]byte
	RemoveRequests(requests ...string)
	Submit(request []byte) error
	Restart(bool)
	RequestCount() int64
	Close()
}

//go:generate counterfeiter -o mocks/state_provider.go . StateProvider
type StateProvider interface {
	GetLatestStateChan() <-chan *state.State
}

//go:generate counterfeiter -o mocks/config_sequence_getter.go . ConfigSequenceGetter
type ConfigSequenceGetter interface {
	ConfigSequence() types.ConfigSequence
}

//go:generate counterfeiter -o mocks/complainer.go . Complainer
type Complainer interface {
	Complain(string)
}

//go:generate counterfeiter -o mocks/batched_requests_verifier.go . BatchedRequestsVerifier

// BatchedRequestsVerifier verifies batched requests
type BatchedRequestsVerifier interface {
	VerifyBatchedRequests(types.BatchedRequests) error
}

//go:generate counterfeiter -o mocks/batch_acker.go . BatchAcker

// BatchAcker sends an ack over a specific batch
type BatchAcker interface {
	Ack(seq types.BatchSequence, to types.PartyID)
}

//go:generate counterfeiter -o mocks/baf_sender.go . BAFSender

// BAFSender sends the baf to the consenters
type BAFSender interface {
	SendBAF(baf types.BatchAttestationFragment)
}

//go:generate counterfeiter -o mocks/baf_creator.go . BAFCreator

// BAFCreator creates a baf
type BAFCreator interface {
	CreateBAF(seq types.BatchSequence, primary types.PartyID, shard types.ShardID, digest []byte) types.BatchAttestationFragment
}

type BatchLedgerWriter interface {
	Append(partyID types.PartyID, batchSeq types.BatchSequence, configSeq types.ConfigSequence, batchedRequests types.BatchedRequests)
}

type BatchLedgeReader interface {
	Height(partyID types.PartyID) uint64
	RetrieveBatchByNumber(partyID types.PartyID, seq uint64) types.Batch
}

//go:generate counterfeiter -o mocks/batch_ledger.go . BatchLedger
type BatchLedger interface {
	BatchLedgerWriter
	BatchLedgeReader
}

type BatcherRole struct {
	Batchers                []types.PartyID
	BatchTimeout            time.Duration
	RequestInspector        RequestInspector
	ID                      types.PartyID
	Shard                   types.ShardID
	Threshold               int
	N                       uint16
	Logger                  types.Logger
	Ledger                  BatchLedger
	BatchPuller             BatchesPuller
	StateProvider           StateProvider
	ConfigSequenceGetter    ConfigSequenceGetter
	BAFCreator              BAFCreator
	BAFSender               BAFSender
	BatchAcker              BatchAcker
	Complainer              Complainer
	BatchedRequestsVerifier BatchedRequestsVerifier
	MemPool                 MemPool
	BatchSequenceGap        types.BatchSequence
	running                 sync.WaitGroup
	stopChan                chan struct{}
	stopOnce                sync.Once
	stopCtx                 context.Context
	cancelBatch             func()
	primary                 types.PartyID
	seq                     types.BatchSequence
	term                    uint64
	termChan                chan uint64
	ackerLock               sync.RWMutex
	acker                   SeqAcker
	Metrics                 *BatcherMetrics
}

func (b *BatcherRole) Start() {
	b.stopChan = make(chan struct{})
	b.stopCtx, b.cancelBatch = context.WithCancel(context.Background())
	b.termChan = make(chan uint64, 1)

	b.running.Add(2)
	go b.getTermAndNotifyChange()
	go b.run()
}

func (b *BatcherRole) run() {
	defer b.running.Done()
	for {
		select {
		case <-b.stopChan:
			return
		default:
		}

		term := atomic.LoadUint64(&b.term)
		b.primary = b.getPrimaryID(term)
		b.seq = types.BatchSequence(b.Ledger.Height(b.primary))
		b.Logger.Infof("ID: %d, shard: %d, primary id: %d, term: %d, seq: %d", b.ID, b.Shard, b.primary, term, b.seq)

		if b.primary == b.ID {
			b.runPrimary()
		} else {
			b.runSecondary()
		}
	}
}

func (b *BatcherRole) getPrimaryID(term uint64) types.PartyID {
	primaryIndex := b.getPrimaryIndex(term)
	return b.Batchers[primaryIndex]
}

func (b *BatcherRole) getPrimaryIndex(term uint64) types.PartyID {
	primaryIndex := types.PartyID((uint64(b.Shard) + term) % uint64(b.N))

	return primaryIndex
}

func (b *BatcherRole) Stop() {
	b.Logger.Infof("Stopping batcher rol")
	b.stopOnce.Do(func() { close(b.stopChan) })
	b.cancelBatch()
	b.MemPool.Close()
	for len(b.termChan) > 0 {
		<-b.termChan // drain term channel
	}
	b.running.Wait()
}

func (b *BatcherRole) getTerm(state *state.State) uint64 {
	term := uint64(math.MaxUint64)
	for _, shard := range state.Shards {
		if shard.Shard == b.Shard {
			term = shard.Term
		}
	}
	if term == math.MaxUint64 {
		b.Logger.Panicf("Could not find our shard (%d) within the shards: %v", b.Shard, state.Shards)
	}
	return term
}

func (b *BatcherRole) getTermAndNotifyChange() {
	defer b.running.Done()
	stateChan := b.StateProvider.GetLatestStateChan()
	for {
		select {
		case <-b.stopChan:
			return
		case state := <-stateChan:
			newTerm := b.getTerm(state)
			currentTerm := atomic.LoadUint64(&b.term)
			if currentTerm != newTerm {
				atomic.StoreUint64(&b.term, newTerm)
				b.termChan <- newTerm
				b.resubmitPendingBAFs(state, b.getPrimaryID(currentTerm))
				b.Metrics.memPoolSize.Set(float64(b.MemPool.RequestCount()))
			}
		}
	}
}

// when the term is changed there might be a case where
// a batch with a certain tx was attested by not enough batchers (less than f+1 BAFs)
// and that tx is in the pool of other batchers but again not enough (less than f+1 batchers)
// to prevent a case where such a tx falls through the cracks, after a term change
// all batchers resubmit to their pools txs in batches with their BAFs still in pending state
func (b *BatcherRole) resubmitPendingBAFs(state *state.State, prevPrimary types.PartyID) {
	for _, baf := range state.Pending {
		if baf.Shard() == b.Shard && baf.Signer() == b.ID && baf.Primary() == prevPrimary {
			b.Logger.Debugf("found pending BAF signed by me (id: %d) from prev primary: %d ; %s", b.ID, prevPrimary, baf.String())
			batch := b.Ledger.RetrieveBatchByNumber(baf.Primary(), uint64(baf.Seq()))
			if batch == nil {
				b.Logger.Panicf("Error: No such batch; pending BAF signed by me (id: %d) from prev primary: %d ; %s", b.ID, prevPrimary, baf.String())
			}
			for _, req := range batch.Requests() {
				if err := b.MemPool.Submit(req); err != nil {
					if strings.Contains(err.Error(), "already inserted") {
						b.Logger.Debugf("Failed submitting request to pool; err: %v", err)
						continue
					}
					b.Logger.Errorf("Failed submitting request to pool; err: %v", err)
				}
			}
		}
	}
}

func (b *BatcherRole) Submit(request []byte) error {
	if err := b.MemPool.Submit(request); err != nil {
		return err
	}

	b.Metrics.memPoolSize.Set(float64(b.MemPool.RequestCount()))
	return nil
}

func (b *BatcherRole) HandleAck(seq types.BatchSequence, from types.PartyID) {
	b.ackerLock.RLock()
	defer b.ackerLock.RUnlock()
	if b.acker != nil {
		b.acker.HandleAck(seq, from)
	}
}

func (b *BatcherRole) runPrimary() {
	b.Logger.Infof("Batcher %d acting as primary (shard %d)", b.ID, b.Shard)
	b.Metrics.currentRole.Set(1)

	defer func() {
		b.Logger.Infof("Batcher %d stopped acting as primary (shard %d)", b.ID, b.Shard)
		b.ackerLock.RLock()
		b.acker.Stop()
		b.ackerLock.RUnlock()
	}()

	b.ackerLock.Lock()
	b.acker = NewAcker(b.seq, b.BatchSequenceGap, b.N, uint16(b.Threshold), b.Logger)
	b.ackerLock.Unlock()
	b.MemPool.Restart(true)

	var currentBatch types.BatchedRequests
	var digest []byte

	for {
		for {
			b.ackerLock.RLock()
			ch := b.acker.WaitForSecondaries(b.seq)
			b.ackerLock.RUnlock()
			select {
			case newTerm := <-b.termChan:
				b.Logger.Infof("Primary batcher %d (shard %d) term change to term %d", b.ID, b.Shard, newTerm)
				return
			case <-b.stopChan:
				return
			case <-ch:
			}
			ctx, cancel := context.WithTimeout(b.stopCtx, b.BatchTimeout)
			currentBatch = b.MemPool.NextRequests(ctx)
			if len(currentBatch) == 0 {
				cancel()
				continue
			}
			b.Logger.Infof("Batcher batched a total of %d requests for sequence %d", len(currentBatch), b.seq)
			digest = currentBatch.Digest()
			cancel()
			break
		}

		baf := b.BAFCreator.CreateBAF(b.seq, b.ID, b.Shard, digest)

		// After the batch is appended to the ledger the batcher sends the BAF to the consenters
		// (this BAF is a declaration that the batch is stored in the ledger)
		// Once the BAF reached the consenters it is considered safe to remove the requests from the mem pool

		b.Ledger.Append(b.ID, b.seq, b.ConfigSequenceGetter.ConfigSequence(), currentBatch)

		// TODO: Check that the batcher doesn’t get stuck here if quorum isn’t reached and the batcher is restarted or a term change occurs
		b.BAFSender.SendBAF(baf)
		b.Metrics.batchedTxsTotal.Add(float64(len(currentBatch)))

		b.ackerLock.RLock()
		b.acker.HandleAck(b.seq, b.ID)
		b.ackerLock.RUnlock()

		b.seq++

		b.removeRequests(currentBatch)

		// TODO find out from the state if old batches need to be resubmitted (not enough BAFs collected)
	}
}

func (b *BatcherRole) removeRequests(batch types.BatchedRequests) {
	reqInfos := make([]string, 0, len(batch))
	for _, req := range batch {
		reqInfos = append(reqInfos, b.RequestInspector.RequestID(req))
	}
	b.MemPool.RemoveRequests(reqInfos...)
	b.Metrics.memPoolSize.Set(float64(b.MemPool.RequestCount()))
}

func (b *BatcherRole) runSecondary() {
	b.Logger.Infof("Batcher %d acting as secondary (shard %d; primary %d)", b.ID, b.Shard, b.primary)
	b.MemPool.Restart(false)
	b.Metrics.currentRole.Set(2)

	for {
		out := b.BatchPuller.PullBatches(b.primary)
		for {
			var batch types.Batch
			select {
			case batch = <-out:
			case newTerm := <-b.termChan:
				b.Logger.Infof("Secondary batcher %d (shard %d) term change to term %d", b.ID, b.Shard, newTerm)
				b.BatchPuller.Stop()
				return
			case <-b.stopChan:
				b.Logger.Infof("Batcher %d stopped acting as secondary (shard %d; primary %d)", b.ID, b.Shard, b.primary)
				b.BatchPuller.Stop()
				return
			}
			if err := b.verifyBatch(batch); err != nil {
				b.Logger.Warnf("Secondary batcher %d (shard %d) sending a complaint (primary %d); verify batch err: %v", b.ID, b.Shard, b.primary, err)
				// TODO: Check that the batcher doesn’t get stuck here if quorum isn’t reached and the batcher is restarted or a term change occurs
				b.Complainer.Complain(fmt.Sprintf("batcher %d (shard %d) complaining; primary %d; term %d; verify batch err: %v", b.ID, b.Shard, b.primary, atomic.LoadUint64(&b.term), err))
				b.BatchPuller.Stop()
				break // TODO maybe add backoff
			}
			b.Metrics.batchesPulledTotal.Add(1)
			requests := batch.Requests()

			// After the batch is appended to the ledger the batcher sends the BAF to the consenters
			// (this BAF is a declaration that the batch is stored in the ledger)
			// Once the BAF reached the consenters it is considered safe to remove the requests from the mem pool

			b.Logger.Infof("Secondary batcher %d (shard %d; current primary %d) appending to ledger batch with seq %d and %d requests", b.ID, b.Shard, b.primary, b.seq, len(requests))
			b.Ledger.Append(b.primary, b.seq, b.ConfigSequenceGetter.ConfigSequence(), requests)
			baf := b.BAFCreator.CreateBAF(b.seq, b.primary, b.Shard, requests.Digest())
			// TODO: Check that the batcher doesn’t get stuck here if quorum isn’t reached and the batcher is restarted or a term change occurs
			b.BAFSender.SendBAF(baf)
			b.Metrics.batchedTxsTotal.Add(float64(len(requests)))
			b.removeRequests(requests)
			b.BatchAcker.Ack(baf.Seq(), b.primary)
			b.seq++
		}
	}
}

func (b *BatcherRole) verifyBatch(batch types.Batch) error {
	if batch.ConfigSequence() != b.ConfigSequenceGetter.ConfigSequence() {
		b.Logger.Warnf("Batch config seq (%d) does not match batcher's current config seq (%d)", batch.ConfigSequence(), b.ConfigSequenceGetter.ConfigSequence())
	}
	if batch.Primary() != b.primary {
		return errors.Errorf("batch primary (%d) not equal to expected primary (%d)", batch.Primary(), b.primary)
	}
	if batch.Shard() != b.Shard {
		return errors.Errorf("batch shard (%d) not equal to expected shard (%d)", batch.Shard(), b.Shard)
	}
	if batch.Seq() != b.seq {
		return errors.Errorf("batch seq (%d) not equal to expected seq (%d)", batch.Seq(), b.seq)
	}
	if len(batch.Requests()) == 0 {
		return errors.Errorf("empty batch")
	}
	br := batch.Requests()
	if !slices.Equal(batch.Digest(), br.Digest()) {
		return errors.Errorf("batch digest (%v) is not equal to calculated digest (%v)", batch.Digest(), br.Digest())
	}
	if err := b.BatchedRequestsVerifier.VerifyBatchedRequests(batch.Requests()); err != nil {
		return errors.Errorf("failed verifying requests for batch seq %d; err: %v", b.seq, err)
	}
	return nil
}
