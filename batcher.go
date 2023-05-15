package arma

import (
	"arma/request"
	"context"
	"encoding/binary"
	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"runtime"
	"sync"
	"time"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

type Batch interface {
	Digest() []byte
	Requests() BatchedRequests
	Party() uint16
}

type BatchReplicator interface {
	Replicate(uint16, uint64) <-chan Batch
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
	Digest                func([][]byte) []byte
	OnCollectAttestations func(uint642 uint64, digest []byte, m map[uint16][]byte)
	RequestInspector      api.RequestInspector
	Primary               uint16
	ID                    uint16
	Quorum                int
	Logger                Logger
	Ledger                BatchLedger
	Seq                   uint64
	ConfirmedSeq          uint64
	Replicator            BatchReplicator
	Sign                  func(uint64, []byte) []byte
	Send                  func(uint16, []byte)
	memPool               *request.Pool

	lock               sync.Mutex
	signal             sync.Cond
	confirmedSequences map[uint64]map[uint16][]byte
	seq2digest         map[uint64][]byte
}

func (b *Batcher) run() {
	if b.Primary == b.ID {
		go b.runPrimary()
		return
	}

	go b.runSecondary()
}

func (b *Batcher) Submit(request []byte) error {
	return b.memPool.Submit(request)
}

func (b *Batcher) HandleMessage(msg []byte, from uint16) {
	seq := binary.BigEndian.Uint64(msg[0:8])
	signature := msg[8:]

	b.lock.Lock()
	defer b.lock.Unlock()

	_, exists := b.confirmedSequences[seq]
	if !exists {
		b.confirmedSequences[seq] = make(map[uint16][]byte, 16)
	}

	if _, exists := b.confirmedSequences[seq][from]; exists {
		b.Logger.Warnf("Already received signature on %d from %d", seq, from)
		return
	}

	b.confirmedSequences[seq][from] = signature

	if len(b.confirmedSequences[seq]) >= b.Quorum && seq == b.ConfirmedSeq+1 {
		b.ConfirmedSeq++
		b.notifyBatchAttestation(seq, b.seq2digest[seq], b.confirmedSequences[seq])
		delete(b.confirmedSequences, seq)
	}

	b.signal.Broadcast()
}

func (b *Batcher) secondariesKeepUpWithMe() bool {
	return b.Seq-b.ConfirmedSeq < 10
}

func (b *Batcher) notifyBatchAttestation(seq uint64, digest []byte, m map[uint16][]byte) {
	b.Logger.Infof("Collected %d signatures on %d", len(m), seq)
	b.OnCollectAttestations(seq, digest, m)
}

func (b *Batcher) runPrimary() {
	b.memPool.SetBatching(true)

	var currentBatch BatchedRequests
	var digest []byte
	for {
		var serializedBatch []byte
		for len(serializedBatch) == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			currentBatch = b.memPool.NextRequests(ctx)
			digest = b.Digest(currentBatch)
			serializedBatch = currentBatch.ToBytes()
			cancel()
		}

		σ := b.Sign(b.Seq, digest)

		b.lock.Lock()
		b.seq2digest[b.Seq] = digest
		b.lock.Unlock()

		b.send(b.Primary, b.Seq, σ)

		b.Ledger.Append(b.ID, b.Seq, serializedBatch)
		b.Seq++

		b.waitForSecondaries()
		b.removeRequests(currentBatch)
	}
}

func (b *Batcher) removeRequests(batch BatchedRequests) {

	workerNum := runtime.NumCPU()

	var wg sync.WaitGroup
	wg.Add(workerNum)

	for workerID := 0; workerID < workerNum; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			reqInfos := make([]types.RequestInfo, 0, len(batch))
			for i, req := range batch {
				if i%workerNum != workerID {
					continue
				}
				reqInfos = append(reqInfos, b.RequestInspector.RequestID(req))
			}

			b.memPool.RemoveRequests(reqInfos...)

		}(workerID)
	}

	wg.Wait()
}

func (b *Batcher) waitForSecondaries() {
	t1 := time.Now()
	defer func() {
		b.Logger.Debugf("Waiting for secondaries to keep up with me took %v", time.Since(t1))
	}()
	b.lock.Lock()
	for !b.secondariesKeepUpWithMe() {
		b.signal.Wait()
	}
	b.lock.Unlock()
}

func (b *Batcher) send(to uint16, seq uint64, msg []byte) {
	rawMsg := make([]byte, 8+len(msg))
	binary.BigEndian.PutUint64(rawMsg[0:8], seq)
	copy(rawMsg[8:], msg)

	if to != b.ID {
		b.Send(to, rawMsg)
		return
	}

	if b.Primary != b.ID {
		panic("should not send to yourself if you're not a primary")
	}

	b.HandleMessage(rawMsg, b.ID)
}

func (b *Batcher) runSecondary() {
	out := b.Replicator.Replicate(b.Primary, b.Seq)
	for {
		batchedRequests := <-out
		batch := batchedRequests.Requests().ToBytes()
		b.Ledger.Append(b.Primary, b.Seq, batch)
		b.removeRequests(batchedRequests.Requests())
		σ := b.Sign(b.Seq, batch)
		b.send(b.Primary, b.Seq, σ)
		b.Seq++
	}
}
