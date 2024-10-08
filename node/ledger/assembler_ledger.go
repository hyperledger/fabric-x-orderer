package ledger

import (
	"sync/atomic"
	"time"

	"arma/common/types"
	"arma/core"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
)

type AssemblerLedger struct {
	Logger types.Logger
	Ledger blockledger.ReadWriter

	// TODO We need to recover these values when the assembler restarts
	prevHash         []byte
	transactionCount uint64
	nextSeq          uint64
}

func (l *AssemblerLedger) TrackThroughput() {
	// TODO we need to be able to stop this routine
	firstProbe := true
	lastTxCount := uint64(0)
	for {
		txCount := atomic.LoadUint64(&l.transactionCount)
		if !firstProbe {
			l.Logger.Infof("Tx Count: %d, Commit throughput: %.2f", txCount, float64(txCount-lastTxCount)/10.0)
		}
		lastTxCount = txCount
		firstProbe = false
		time.Sleep(time.Second * 10)
	}
}

func (l *AssemblerLedger) GetTxCount() uint64 {
	c := atomic.LoadUint64(&l.transactionCount)
	return c
}

func (l *AssemblerLedger) Append(seq uint64, batch core.Batch, ba core.BatchAttestation) {
	// TODO input arguments will change in next commit
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended block of %d requests to ledger in %v", len(batch.Requests()), time.Since(t1))
	}()
	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       l.nextSeq,
			DataHash:     ba.Digest(),
			PreviousHash: l.prevHash,
		},
		Data: &common.BlockData{
			Data: batch.Requests(),
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}},
		},
	}

	l.nextSeq++
	l.prevHash = protoutil.BlockHeaderHash(block.Header)

	if err := l.Ledger.Append(block); err != nil {
		panic(err)
	}

	atomic.AddUint64(&l.transactionCount, uint64(len(batch.Requests())))
}
