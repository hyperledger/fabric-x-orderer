package core

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"

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
	Close()
}

//go:generate counterfeiter -o mocks/batch.go . Batch
type Batch interface {
	types.BatchID
	Requests() types.BatchedRequests
}

//go:generate counterfeiter -o mocks/batch_puller.go . BatchPuller
type BatchPuller interface {
	PullBatches(from types.PartyID) <-chan Batch
	Stop()
}

//go:generate counterfeiter -o mocks/state_provider.go . StateProvider
type StateProvider interface {
	GetLatestStateChan() <-chan *State
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
	SendBAF(baf BatchAttestationFragment)
}

//go:generate counterfeiter -o mocks/baf_creator.go . BAFCreator

// BAFCreator creates a baf
type BAFCreator interface {
	CreateBAF(seq types.BatchSequence, primary types.PartyID, shard types.ShardID, digest []byte) BatchAttestationFragment
}

type BatchLedgerWriter interface {
	Append(partyID types.PartyID, batchSeq types.BatchSequence, batchedRequests types.BatchedRequests)
}

type BatchLedgeReader interface {
	Height(partyID types.PartyID) uint64
	RetrieveBatchByNumber(partyID types.PartyID, seq uint64) Batch
}

//go:generate counterfeiter -o mocks/batch_ledger.go . BatchLedger
type BatchLedger interface {
	BatchLedgerWriter
	BatchLedgeReader
}

var (
	gap                 = types.BatchSequence(10)
	DefaultBatchTimeout = time.Millisecond * 500
)

type Batcher struct {
	Batchers                []types.PartyID
	BatchTimeout            time.Duration
	RequestInspector        RequestInspector
	ID                      types.PartyID
	Shard                   types.ShardID
	Threshold               int
	N                       uint16
	Logger                  types.Logger
	Ledger                  BatchLedger
	BatchPuller             BatchPuller
	StateProvider           StateProvider
	BAFCreator              BAFCreator
	BAFSender               BAFSender
	BatchAcker              BatchAcker
	Complainer              Complainer
	BatchedRequestsVerifier BatchedRequestsVerifier
	MemPool                 MemPool
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
}

func (b *Batcher) Start() {
	if b.BatchTimeout == 0 {
		b.BatchTimeout = DefaultBatchTimeout
	}

	b.stopChan = make(chan struct{})
	b.stopCtx, b.cancelBatch = context.WithCancel(context.Background())
	b.termChan = make(chan uint64, 1)

	b.running.Add(2)
	go b.getTermAndNotifyChange()
	go b.run()
}

func (b *Batcher) run() {
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

func (b *Batcher) getPrimaryID(term uint64) types.PartyID {
	primaryIndex := b.getPrimaryIndex(term)
	return b.Batchers[primaryIndex]
}

func (b *Batcher) getPrimaryIndex(term uint64) types.PartyID {
	primaryIndex := types.PartyID((uint64(b.Shard) + term) % uint64(b.N))

	return primaryIndex
}

func (b *Batcher) Stop() {
	b.Logger.Infof("Stopping batcher core")
	b.stopOnce.Do(func() { close(b.stopChan) })
	b.cancelBatch()
	b.MemPool.Close()
	for len(b.termChan) > 0 {
		<-b.termChan // drain term channel
	}
	b.running.Wait()
}

func (b *Batcher) getTerm(state *State) uint64 {
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

func (b *Batcher) getTermAndNotifyChange() {
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
			}
		}
	}
}

func (b *Batcher) Submit(request []byte) error {
	return b.MemPool.Submit(request)
}

func (b *Batcher) HandleAck(seq types.BatchSequence, from types.PartyID) {
	b.ackerLock.RLock()
	defer b.ackerLock.RUnlock()
	if b.acker != nil {
		b.acker.HandleAck(seq, from)
	}
}

func (b *Batcher) runPrimary() {
	b.Logger.Infof("Batcher %d acting as primary (shard %d)", b.ID, b.Shard)

	defer func() {
		b.Logger.Infof("Batcher %d stopped acting as primary (shard %d)", b.ID, b.Shard)
		b.ackerLock.RLock()
		b.acker.Stop()
		b.ackerLock.RUnlock()
	}()

	b.ackerLock.Lock()
	b.acker = NewAcker(b.seq, gap, b.N, uint16(b.Threshold), b.Logger)
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

		b.Ledger.Append(b.ID, b.seq, currentBatch)

		b.BAFSender.SendBAF(baf)

		b.ackerLock.RLock()
		b.acker.HandleAck(b.seq, b.ID)
		b.ackerLock.RUnlock()

		b.seq++

		b.removeRequests(currentBatch)

		// TODO find out from the state if old batches need to be resubmitted (not enough BAFs collected)
	}
}

func (b *Batcher) removeRequests(batch types.BatchedRequests) {
	reqInfos := make([]string, 0, len(batch))
	for _, req := range batch {
		reqInfos = append(reqInfos, b.RequestInspector.RequestID(req))
	}
	b.MemPool.RemoveRequests(reqInfos...)
}

func (b *Batcher) runSecondary() {
	b.Logger.Infof("Batcher %d acting as secondary (shard %d; primary %d)", b.ID, b.Shard, b.primary)
	b.MemPool.Restart(false)
	for {
		out := b.BatchPuller.PullBatches(b.primary)
		for {
			var batch Batch
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
				b.Complainer.Complain(fmt.Sprintf("batcher %d (shard %d) complaining; primary %d; term %d; verify batch err: %v", b.ID, b.Shard, b.primary, atomic.LoadUint64(&b.term), err))
				b.BatchPuller.Stop()
				break // TODO maybe add backoff
			}
			requests := batch.Requests()
			b.Logger.Infof("Secondary batcher %d (shard %d; current primary %d) appending to ledger batch with seq %d and %d requests", b.ID, b.Shard, b.primary, b.seq, len(requests))
			b.Ledger.Append(b.primary, b.seq, requests)
			b.removeRequests(requests)
			baf := b.BAFCreator.CreateBAF(b.seq, b.primary, b.Shard, requests.Digest())
			b.BAFSender.SendBAF(baf)
			b.BatchAcker.Ack(baf.Seq(), b.primary)
			b.seq++
		}
	}
}

func (b *Batcher) verifyBatch(batch Batch) error {
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
