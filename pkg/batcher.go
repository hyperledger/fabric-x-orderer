package arma

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

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
	Party() uint16
}

type BatchPuller interface {
	PullBatches(from uint16, startSeq uint64) <-chan Batch
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

type BatchLedger interface {
	Append(uint16, uint64, []byte)
}

type Batcher struct {
	BatchTimeout         time.Duration
	Digest               func([][]byte) []byte
	RequestInspector     RequestInspector
	Primary              uint16
	ID                   uint16
	Shard                uint16
	Threshold            int
	Logger               Logger
	Ledger               BatchLedger
	Seq                  uint64
	ConfirmedSeq         uint64
	BatchPuller          BatchPuller
	AttestBatch          func(seq uint64, primary uint16, shard uint16, digest []byte) BatchAttestationFragment
	AttestationFromBytes func([]byte) (BatchAttestationFragment, error)
	TotalOrderBAF        func(BatchAttestationFragment)
	AckBAF               func(seq uint64, to uint16)
	MemPool              MemPool
	running              sync.WaitGroup
	stopChan             chan struct{}
	stopCtx              context.Context
	cancelBatch          func()

	lock               sync.Mutex
	signal             sync.Cond
	confirmedSequences map[uint64]map[uint16]struct{}
}

func (b *Batcher) Run() {
	b.running.Add(1)
	b.stopChan = make(chan struct{})
	b.stopCtx, b.cancelBatch = context.WithCancel(context.Background())
	b.signal = sync.Cond{L: &b.lock}
	b.confirmedSequences = make(map[uint64]map[uint16]struct{})
	if b.Primary == b.ID {
		go b.runPrimary()
		return
	}

	go b.runSecondary()
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

func (b *Batcher) HandleAck(seq uint64, from uint16) {
	// Only the primary performs the remaining code
	if b.Primary != b.ID {
		b.Logger.Warnf("Received ack on sequence %d from %d but we are not the primary (%d)", seq, from, b.Primary)
		return
	}

	b.Logger.Infof("Received ack on sequence %d from %d", seq, from)

	b.lock.Lock()
	defer b.lock.Unlock()

	confirmedSequence := atomic.LoadUint64(&b.ConfirmedSeq)

	if seq < confirmedSequence {
		b.Logger.Debugf("Received message on sequence %d but we expect sequence %d to %d", seq, confirmedSequence, confirmedSequence+10)
		return
	}

	if seq-confirmedSequence > 10 {
		b.Logger.Warnf("Received message on sequence %d but our confirmed sequence is only at %d", seq, confirmedSequence)
		return
	}

	_, exists := b.confirmedSequences[seq]
	if !exists {
		b.confirmedSequences[seq] = make(map[uint16]struct{}, 16)
	}

	if _, exists := b.confirmedSequences[seq][from]; exists {
		b.Logger.Warnf("Already received signature on %d from %d", seq, from)
		return
	}

	b.confirmedSequences[seq][from] = struct{}{}

	signatureCollectCount := len(b.confirmedSequences[seq])
	if signatureCollectCount >= b.Threshold {
		atomic.AddUint64(&b.ConfirmedSeq, 1)
		b.Logger.Infof("Removing %d digest mapping from memory as we received enough (%d) signatures", seq, signatureCollectCount)
		delete(b.confirmedSequences, seq)
		b.signal.Broadcast()
	} else {
		b.Logger.Infof("Collected %d out of %d signatures on sequence %d", signatureCollectCount, b.Threshold, seq)
	}

}

func (b *Batcher) secondariesKeepUpWithMe() bool {
	confirmedSeq := atomic.LoadUint64(&b.ConfirmedSeq)
	b.Logger.Debugf("Current sequence: %d, confirmed sequence: %d", b.Seq, confirmedSeq)
	return b.Seq-confirmedSeq < 10
}

func (b *Batcher) runPrimary() {
	defer b.running.Done()
	b.Logger.Infof("Acting as primary")
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
				return
			default:

			}
			if len(currentBatch) == 0 {
				continue
			}
			b.Logger.Infof("Batcher ordered a total of %d requests for sequence %d", len(currentBatch), b.Seq)
			digest = b.Digest(currentBatch)
			serializedBatch = currentBatch.ToBytes()
			cancel()
			break
		}

		baf := b.AttestBatch(b.Seq, b.Primary, b.Shard, digest)

		b.Ledger.Append(b.ID, b.Seq, serializedBatch)

		b.sendBAF(baf)
		b.HandleAck(b.Seq, b.ID)

		b.Seq++

		b.removeRequests(currentBatch)
		b.waitForSecondaries()
	}
}

func (b *Batcher) removeRequests(batch BatchedRequests) {

	workerNum := runtime.NumCPU()

	var wg sync.WaitGroup
	wg.Add(workerNum)

	for workerID := 0; workerID < workerNum; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			reqInfos := make([]string, 0, len(batch))
			for i, req := range batch {
				if i%workerNum != workerID {
					continue
				}
				reqInfos = append(reqInfos, b.RequestInspector.RequestID(req))
			}

			b.MemPool.RemoveRequests(reqInfos...)

		}(workerID)
	}

	wg.Wait()
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

	b.Logger.Infof("Sending batch attestation fragment on %d with digest %x", baf.Seq(), baf.Digest())

	b.TotalOrderBAF(baf)
}

func (b *Batcher) runSecondary() {
	defer b.running.Done()
	b.Logger.Infof("Acting as secondary")
	out := b.BatchPuller.PullBatches(b.Primary, b.Seq)
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
		b.Ledger.Append(b.Primary, b.Seq, batch)
		b.removeRequests(requests)
		baf := b.AttestBatch(b.Seq, b.Primary, b.Shard, b.Digest(requests))
		b.sendBAF(baf)
		b.AckBAF(baf.Seq(), b.Primary)
		b.Seq++
	}
}
