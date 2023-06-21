package arma

import (
	"arma/request"
	"bytes"
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
	RemoveRequests(requests ...string)
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
	BatchTimeout          time.Duration
	Digest                func([][]byte) []byte
	OnCollectAttestations func(uint642 uint64, digest []byte, m map[uint16][]byte)
	RequestInspector      RequestInspector
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
	MemPool               *request.Pool

	lock               sync.Mutex
	signal             sync.Cond
	confirmedSequences map[uint64]map[uint16][]byte
	seq2digest         map[uint64][]byte
}

func (b *Batcher) Run() {
	b.signal = sync.Cond{L: &b.lock}
	b.seq2digest = make(map[uint64][]byte)
	b.confirmedSequences = make(map[uint64]map[uint16][]byte)
	if b.Primary == b.ID {
		go b.runPrimary()
		return
	}

	go b.runSecondary()
}

func (b *Batcher) Submit(request []byte) error {
	return b.MemPool.Submit(request)
}

func (b *Batcher) HandleMessage(msg []byte, from uint16) {
	seq := binary.BigEndian.Uint64(msg[0:8])
	digest := msg[8 : 8+32]
	signature := msg[8+32:]

	b.Logger.Infof("Received signature on sequence %d and digest %x from %d", seq, digest, from)

	b.lock.Lock()
	defer b.lock.Unlock()

	confirmedSequence := atomic.LoadUint64(&b.ConfirmedSeq)

	if seq < confirmedSequence {
		b.Logger.Warnf("Received message on sequence %d but we expect sequence %d to %d", seq, confirmedSequence, confirmedSequence+10)
		return
	}

	if seq-confirmedSequence > 10 {
		b.Logger.Warnf("Received message on sequence %d but our confirmed sequence is only at %d", seq, confirmedSequence)
		return
	}

	_, exists := b.confirmedSequences[seq]
	if !exists {
		b.confirmedSequences[seq] = make(map[uint16][]byte, 16)
	}

	if _, exists := b.confirmedSequences[seq][from]; exists {
		b.Logger.Warnf("Already received signature on %d from %d", seq, from)
		return
	}

	b.confirmedSequences[seq][from] = signature

	if storedDigest, exists := b.seq2digest[seq]; !exists {
		panic(fmt.Sprintf("no digest found for sequence %d, current sequence is %d", seq, b.Seq))
	} else if !bytes.Equal(storedDigest, digest) {
		panic(fmt.Sprintf("stored digest %v but received digest %v for batch %d", storedDigest, digest, seq))
	}

	signatureCollectCount := len(b.confirmedSequences[seq])
	if signatureCollectCount >= b.Quorum {
		atomic.AddUint64(&b.ConfirmedSeq, 1)
		b.notifyBatchAttestation(seq, b.seq2digest[seq], b.confirmedSequences[seq])
		b.Logger.Infof("Removing %d digest mapping from memory as we received enough (%d) signatures", seq, signatureCollectCount)
		delete(b.seq2digest, seq)
		delete(b.confirmedSequences, seq)
		b.signal.Broadcast()
	} else {
		b.Logger.Infof("Collected %d out of %d signatures on sequence %d", signatureCollectCount, b.Quorum, seq)
	}

}

func (b *Batcher) secondariesKeepUpWithMe() bool {
	confirmedSeq := atomic.LoadUint64(&b.ConfirmedSeq)
	b.Logger.Debugf("Current sequence: %d, confirmed sequence: %d", b.Seq, confirmedSeq)
	return b.Seq-confirmedSeq < 10
}

func (b *Batcher) notifyBatchAttestation(seq uint64, digest []byte, m map[uint16][]byte) {
	b.Logger.Infof("Collected %d signatures on %d", len(m), seq)
	b.OnCollectAttestations(seq, digest, m)
}

func (b *Batcher) runPrimary() {
	b.Logger.Infof("Acting as primary")
	b.MemPool.SetBatching(true)

	if b.BatchTimeout == 0 {
		b.BatchTimeout = time.Millisecond * 500
	}

	var currentBatch BatchedRequests
	var digest []byte
	for {
		var serializedBatch []byte
		for {
			ctx, cancel := context.WithTimeout(context.Background(), b.BatchTimeout)
			currentBatch = b.MemPool.NextRequests(ctx)
			if len(currentBatch) == 0 {
				continue
			}
			b.Logger.Infof("Batcher ordered a total of %d requests for sequence %d", len(currentBatch), b.Seq)
			digest = b.Digest(currentBatch)
			serializedBatch = currentBatch.ToBytes()
			cancel()
			break
		}

		σ := b.Sign(b.Seq, digest)

		b.lock.Lock()
		b.seq2digest[b.Seq] = digest
		b.lock.Unlock()

		b.send(b.Primary, b.Seq, σ, digest)

		b.Ledger.Append(b.ID, b.Seq, serializedBatch)
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

func (b *Batcher) send(to uint16, seq uint64, msg []byte, digest []byte) {
	if len(digest) != 32 {
		panic(fmt.Sprintf("programming error: tried to send digest of size %d", len(digest)))
	}
	rawMsg := make([]byte, 8+len(msg)+len(digest))
	binary.BigEndian.PutUint64(rawMsg[0:8], seq)
	copy(rawMsg[8:], digest)
	copy(rawMsg[8+32:], msg)

	b.Logger.Infof("Sending to %d signature on %d with digest %x", to, seq, digest)

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
	b.Logger.Infof("Acting as secondary")
	out := b.Replicator.Replicate(b.Primary, b.Seq)
	for {
		batchedRequests := <-out
		requests := batchedRequests.Requests()
		if len(requests) == 0 {
			panic("programming error: replicated an empty batch")
		}
		batch := requests.ToBytes()
		b.Ledger.Append(b.Primary, b.Seq, batch)
		b.removeRequests(requests)
		σ := b.Sign(b.Seq, batch)
		b.send(b.Primary, b.Seq, σ, b.Digest(requests))
		b.Seq++
	}
}
