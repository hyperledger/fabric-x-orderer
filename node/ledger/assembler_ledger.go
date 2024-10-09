package ledger

import (
	"sync/atomic"
	"time"

	"arma/node/consensus/state"

	"arma/common/types"
	"arma/core"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
)

type AssemblerLedger struct {
	Logger types.Logger
	Ledger blockledger.ReadWriter

	// TODO We need to recover this value when the assembler restarts
	transactionCount uint64
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

func (l *AssemblerLedger) Append(batch core.Batch, orderingInfo interface{}) {
	ordInfo := orderingInfo.(*state.OrderingInformation)
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended block %d of %d requests to ledger in %v",
			ordInfo.BlockHeader.Number, len(batch.Requests()), time.Since(t1))
	}()

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       ordInfo.Number, // TODO make sure we start from 0
			DataHash:     ordInfo.Digest,
			PreviousHash: ordInfo.PrevHash,
		},
		Data: &common.BlockData{
			Data: batch.Requests(),
		},
	}

	var metadataContents [][]byte
	for i := 0; i < len(common.BlockMetadataIndex_name); i++ {
		metadataContents = append(metadataContents, []byte{})
	}
	block.Metadata = &common.BlockMetadata{Metadata: metadataContents}

	var sigs []*common.MetadataSignature
	var signers []uint64

	for _, s := range ordInfo.Signatures {
		sigs = append(sigs, &common.MetadataSignature{
			Signature: s.Value,
			IdentifierHeader: protoutil.MarshalOrPanic(&common.IdentifierHeader{
				Identifier: uint32(s.ID),
				Nonce:      []byte{},
			}),
		})

		signers = append(signers, s.ID)
	}

	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
		Signatures: sigs,
	})

	// TODO carry the last config somewhere, we do it the old fabric way
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: 0}),
	})

	//===
	// TODO Ordering metadata  marshal orderingInfo and batchID
	ordererBlockMetadata, err := AssemblerBlockMetadataToBytes(batch, ordInfo)
	if err != nil {
		l.Logger.Panicf("failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = ordererBlockMetadata

	l.Logger.Debugf("Block: H: %+v; D: %d TXs; M: <primary=%d, shard=%d, seq=%d> <dec=%d, index=%d, count=%d> <signers: %+v>",
		block.Header,                                // Header
		len(block.GetData().GetData()),              // Data
		batch.Primary(), batch.Shard(), batch.Seq(), // Metadata batchID
		ordInfo.DecisionNum, ordInfo.BatchIndex, ordInfo.BatchCount, // Metadata ordering
		signers, // Metadata signers
	)

	if err := l.Ledger.Append(block); err != nil {
		panic(err)
	}

	atomic.AddUint64(&l.transactionCount, uint64(len(batch.Requests())))
}
