package arma

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"
)

type RequestInspector interface {
	RequestID(req []byte) string
}

type MemPool interface {
	NextRequests(ctx context.Context) [][]byte
	RemoveRequests(requests ...string)
	Submit(request []byte) error
	Restart(bool)
	Close()
}

type Batch interface {
	Digest() []byte
	Requests() BatchedRequests
	Party() PartyID
}

type BatchPuller interface {
	PullBatches(from PartyID) <-chan Batch
}

type BatchedRequests [][]byte

func (batch BatchedRequests) ToBytes() []byte {
	if len(batch) == 0 {
		return nil
	}

	var reqSize int
	for _, req := range batch {
		reqSize += len(req)
	}

	reqSize += 4 * len(batch)

	buff := make([]byte, reqSize)

	var pos int
	for _, req := range batch {
		sizeBuff := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuff, uint32(len(req)))
		copy(buff[pos:], sizeBuff)
		pos += 4
		copy(buff[pos:], req)
		pos += len(req)
	}

	return buff
}

func BatchFromRaw(raw []byte) BatchedRequests {
	var batch BatchedRequests
	for len(raw) > 0 {
		size := binary.BigEndian.Uint32(raw[0:4])
		raw = raw[4:]
		req := raw[0:size]
		batch = append(batch, req)
		raw = raw[size:]
	}

	return batch
}

type BatchLedgerWriter interface {
	Append(PartyID, uint64, []byte)
}

type BatchLedgeReader interface {
	Height(partyID PartyID) uint64
	RetrieveBatchByNumber(partyID PartyID, seq uint64) Batch
}

type BatchLedger interface {
	BatchLedgerWriter
	BatchLedgeReader
}

var gap = uint64(10)

type Batcher struct {
	Batchers             []PartyID
	BatchTimeout         time.Duration
	Digest               func([][]byte) []byte
	RequestInspector     RequestInspector
	ID                   PartyID
	State                State
	Shard                ShardID
	Threshold            int
	Seq                  uint64
	Logger               Logger
	Ledger               BatchLedger
	BatchPuller          BatchPuller
	AttestBatch          func(seq uint64, primary PartyID, shard ShardID, digest []byte) BatchAttestationFragment
	AttestationFromBytes func([]byte) (BatchAttestationFragment, error)
	TotalOrderBAF        func(BatchAttestationFragment)
	AckBAF               func(seq uint64, to PartyID)
	MemPool              MemPool
	confirmedSeq         uint64
	running              sync.WaitGroup
	stopChan             chan struct{}
	stopCtx              context.Context
	cancelBatch          func()
	primary              PartyID
	lock                 sync.Mutex
	signal               sync.Cond
	confirmedSequences   map[uint64]map[PartyID]struct{}
}

func (b *Batcher) Run() {
	b.running.Add(1)
	b.stopChan = make(chan struct{})
	b.stopCtx, b.cancelBatch = context.WithCancel(context.Background())
	b.signal = sync.Cond{L: &b.lock}
	b.confirmedSequences = make(map[uint64]map[PartyID]struct{})

	primaryIndex := b.getPrimaryIndex()

	b.Logger.Infof("ID: %d, batcher for our shard: %v, primary index: %d", b.ID, b.Batchers, primaryIndex)

	b.primary = b.Batchers[primaryIndex]

	b.confirmedSeq = b.Seq

	if b.primary == b.ID {
		go b.runPrimary()
		return
	}

	go b.runSecondary()
}

func (b *Batcher) getPrimaryIndex() PartyID {
	term := uint64(math.MaxUint64)
	for _, shard := range b.State.Shards {
		if shard.Shard == b.Shard {
			term = shard.Term
		}
	}

	if term == math.MaxUint64 {
		b.Logger.Panicf("Could not find our shard (%d) within the shards: %v", b.Shard, b.State.Shards)
	}

	primaryIndex := PartyID((uint64(b.Shard) + term) % uint64(b.State.N))

	return primaryIndex
}

func (b *Batcher) Stop() {
	close(b.stopChan)
	b.cancelBatch()
	b.MemPool.Close()
	b.running.Wait()
}

func (b *Batcher) Submit(request []byte) error {
	return b.MemPool.Submit(request)
}

func (b *Batcher) HandleAck(seq uint64, from PartyID) {
	// Only the primary performs the remaining code
	if b.primary != b.ID {
		b.Logger.Warnf("Batcher %d called handle ack on sequence %d from %d but it is not the primary (%d)", b.ID, seq, from, b.primary)
		return
	}

	b.Logger.Infof("Called handle ack on sequence %d from %d", seq, from)

	b.lock.Lock()
	defer b.lock.Unlock()

	if seq < b.confirmedSeq {
		b.Logger.Debugf("Received message on sequence %d but we expect sequence %d to %d", seq, b.confirmedSeq, b.confirmedSeq+gap)
		return
	}

	if seq-b.confirmedSeq > gap {
		b.Logger.Warnf("Received message on sequence %d but our confirmed sequence is only at %d", seq, b.confirmedSeq)
		return
	}

	_, exists := b.confirmedSequences[seq]
	if !exists {
		b.confirmedSequences[seq] = make(map[PartyID]struct{}, len(b.Batchers))
	}

	if _, exists := b.confirmedSequences[seq][from]; exists {
		b.Logger.Warnf("Already received conformation on %d from %d", seq, from)
		return
	}

	b.confirmedSequences[seq][from] = struct{}{}

	signatureCollectCount := len(b.confirmedSequences[b.confirmedSeq])
	if signatureCollectCount >= b.Threshold {
		b.Logger.Infof("Removing %d digest mapping from memory as we received enough (%d) conformations", b.confirmedSeq, signatureCollectCount)
		delete(b.confirmedSequences, b.confirmedSeq)
		b.confirmedSeq++
		b.signal.Broadcast()
	} else {
		b.Logger.Infof("Collected %d out of %d conformations on sequence %d", signatureCollectCount, b.Threshold, seq)
	}
}

func (b *Batcher) secondariesKeepUpWithMe() bool {
	b.Logger.Debugf("Current sequence: %d, confirmed sequence: %d", b.Seq, b.confirmedSeq)
	return b.Seq-b.confirmedSeq < gap
}

func (b *Batcher) runPrimary() {
	defer b.running.Done()
	b.Logger.Infof("%d Acting as primary (shard %d)", b.ID, b.Shard)
	b.MemPool.Restart(true)

	if b.BatchTimeout == 0 {
		b.BatchTimeout = time.Millisecond * 500
	}

	var currentBatch BatchedRequests
	var digest []byte

	for {
		var serializedBatch []byte
		for {
			ctx, cancel := context.WithTimeout(b.stopCtx, b.BatchTimeout)
			currentBatch = b.MemPool.NextRequests(ctx)
			select {
			case <-b.stopChan:
				cancel()
				return
			default:
			}
			if len(currentBatch) == 0 {
				continue
			}
			b.Logger.Infof("Batcher batched a total of %d requests for sequence %d", len(currentBatch), b.Seq)
			digest = b.Digest(currentBatch)
			serializedBatch = currentBatch.ToBytes()
			cancel()
			break
		}

		baf := b.AttestBatch(b.Seq, b.ID, b.Shard, digest)

		b.Ledger.Append(b.ID, b.Seq, serializedBatch)

		b.sendBAF(baf)
		b.HandleAck(b.Seq, b.ID)

		b.Seq++

		b.removeRequests(currentBatch)
		b.waitForSecondaries()
	}
}

func (b *Batcher) removeRequests(batch BatchedRequests) {
	reqInfos := make([]string, 0, len(batch))
	for _, req := range batch {
		reqInfos = append(reqInfos, b.RequestInspector.RequestID(req))
	}
	b.MemPool.RemoveRequests(reqInfos...)
}

func (b *Batcher) waitForSecondaries() {
	t1 := time.Now()
	defer func() {
		b.Logger.Infof("Waiting for secondaries to keep up with me took %v", time.Since(t1))
	}()
	b.lock.Lock()
	for !b.secondariesKeepUpWithMe() {
		b.signal.Wait()
	}
	b.lock.Unlock()
}

func (b *Batcher) sendBAF(baf BatchAttestationFragment) {
	if len(baf.Digest()) != 32 {
		panic(fmt.Sprintf("programming error: tried to send BAF with digest of size %d", len(baf.Digest())))
	}

	b.Logger.Infof("Sending batch attestation fragment for seq %d with digest %x", baf.Seq(), baf.Digest())

	b.TotalOrderBAF(baf)
}

func (b *Batcher) runSecondary() {
	defer b.running.Done()
	b.Logger.Infof("Batcher %d acting as secondary (shard %d)", b.ID, b.Shard)
	primary := b.primary
	out := b.BatchPuller.PullBatches(primary)
	for {
		var batchedRequests Batch
		select {
		case batchedRequests = <-out:
		case <-b.stopChan:
			return
		}

		requests := batchedRequests.Requests()
		if len(requests) == 0 {
			panic("programming error: replicated an empty batch")
		}
		batch := requests.ToBytes()
		b.Ledger.Append(primary, b.Seq, batch)
		b.removeRequests(requests)
		baf := b.AttestBatch(b.Seq, primary, b.Shard, b.Digest(requests))
		b.sendBAF(baf)
		b.AckBAF(baf.Seq(), primary)
		b.Seq++
	}
}
