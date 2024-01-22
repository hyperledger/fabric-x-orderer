package ledger

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

var (
	Ledgers []*CheapLedger
)

type CheapLedger struct {
	file     *os.File
	filePath string
	nextSeq  uint64
	lock     sync.Mutex
	cond     sync.Cond
	size     int64
	index    sync.Map
}

type Entry struct {
	Data []byte
	Id   []byte
	Seq  uint64
}

func NewCheapLedger(filePath string) *CheapLedger {
	cl := &CheapLedger{
		filePath: filePath,
	}
	write, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	cl.cond = sync.Cond{L: &cl.lock}
	cl.file = write

	return cl
}

func (cl *CheapLedger) Height() uint64 {
	return atomic.LoadUint64(&cl.nextSeq)
}

func (cl *CheapLedger) Load(seq uint64) Entry {
	val, exists := cl.index.Load(seq)
	if !exists {
		panic(fmt.Sprintf("sequence %d is not found in index", seq))
	}

	pos := val.(int64)

	f, err := os.Open(cl.filePath)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	_, err = f.Seek(pos, 0)
	if err != nil {
		panic(err)
	}

	prefix := make([]byte, 8+32+4)

	if _, err := f.Read(prefix); err != nil {
		panic(err)
	}

	actualSeq := binary.BigEndian.Uint64(prefix[:8])
	if seq != actualSeq {
		panic(fmt.Sprintf("requested sequence %d but actual sequence is %d", seq, actualSeq))
	}

	id := prefix[8 : 8+32]

	length := binary.BigEndian.Uint32(prefix[8+32:])

	b := make([]byte, length)
	if _, err := f.Read(b); err != nil {
		panic(err)
	}

	return Entry{Seq: seq, Id: id, Data: b}
}

func (cl *CheapLedger) Write(b []byte, id []byte) error {
	if len(id) != 32 {
		panic(fmt.Sprintf("id should be of size 32, but it's of size %d", len(id)))
	}

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(b)))
	seqBuff := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBuff, cl.nextSeq)

	seq := cl.nextSeq
	size := cl.size

	cl.lock.Lock()
	defer cl.lock.Unlock()

	atomic.AddUint64(&cl.nextSeq, 1)
	atomic.AddInt64(&cl.size, int64(len(b))+4+8+32)

	buff := make([]byte, len(id)+len(b)+len(length)+len(seqBuff))
	copy(buff, seqBuff)
	copy(buff[8:], id)
	copy(buff[8+32:], length)
	copy(buff[8+32+4:], b)

	if _, err := cl.file.Write(buff); err != nil {
		return err
	}

	cl.file.Sync()

	cl.index.Store(seq, size)

	cl.cond.Broadcast()

	return nil
}

func (cl *CheapLedger) Read(startSeq uint64, ctx context.Context) <-chan Entry {
	cl.lock.Lock()
	oldSize := cl.size
	cl.lock.Unlock()

	f, err := os.Open(cl.filePath)
	if err != nil {
		panic(err)
	}

	var pos int64
	val, exists := cl.index.Load(startSeq)
	if exists {
		pos = val.(int64)
	} else {
		pos = oldSize
	}

	_, err = f.Seek(pos, 0)
	if err != nil {
		panic(err)
	}

	c := make(chan Entry)

	go func() {
		defer f.Close()

		for {

			select {
			case <-ctx.Done():
				return
			default:

			}

			cl.lock.Lock()
			for atomic.LoadInt64(&cl.size) == pos {
				cl.cond.Wait()
			}
			cl.lock.Unlock()

			select {
			case <-ctx.Done():
				return
			default:

			}

			newSize := atomic.LoadInt64(&cl.size)

			remainingBytesToRead := newSize - pos

			cl.readEntries(remainingBytesToRead, f, c, ctx)

			pos = newSize
		}
	}()

	return c
}

func (cl *CheapLedger) readEntries(remainingBytesToRead int64, f *os.File, c chan Entry, ctx context.Context) {
	for remainingBytesToRead > 0 {

		prefix := make([]byte, 8+32+4)

		if _, err := f.Read(prefix); err != nil {
			panic(err)
		}

		seq := binary.BigEndian.Uint64(prefix[:8])
		id := prefix[8 : 8+32]

		length := binary.BigEndian.Uint32(prefix[8+32:])

		b := make([]byte, length)

		if _, err := f.Read(b); err != nil {
			panic(err)
		}

		select {
		case c <- Entry{Seq: seq, Id: id, Data: b}:
		case <-ctx.Done():
			return
		}

		remainingBytesToRead -= int64(length)
		remainingBytesToRead -= 44 // 8 + 4 + 32
	}
}
