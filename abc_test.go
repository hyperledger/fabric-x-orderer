package arma

import (
	"fmt"
	"testing"
	"time"
)

type naiveConsensusLedger chan []byte

func (n naiveConsensusLedger) Append(_ uint64, blockHeader []byte) {
	n <- blockHeader
}

type naiveTotalOrder chan [][]byte

func (n naiveTotalOrder) SubmitRequest(req []byte) error {
	n <- [][]byte{req}
	return nil
}

func (n naiveTotalOrder) Deliver() [][]byte {
	batch := <-n
	return batch
}

type naiveblock struct {
	seq         uint64
	batch       Batch
	attestation BatchAttestation
}

type naiveBlockLedger chan naiveblock

func (n naiveBlockLedger) Append(seq uint64, batch Batch, attestation BatchAttestation) {
	n <- naiveblock{
		seq:         seq,
		batch:       batch,
		attestation: attestation,
	}
}

type shardCommitter struct {
	sr      *shardReplicator
	shardID uint16
}

func (s *shardCommitter) Append(party uint16, _ uint64, rawBatch []byte) {
	fmt.Println("Appended block of", len(rawBatch), "bytes")
	nb := &naiveBatch{
		requests: BatchFromRaw(rawBatch),
		node:     party,
	}

	s.sr.subscribers[s.shardID] <- nb
}

type shardReplicator struct {
	subscribers []chan Batch
}

func (s *shardReplicator) Replicate(shard uint16, _ uint64) <-chan Batch {
	return s.subscribers[shard]
}

func TestBatcherAssembler(t *testing.T) {
	shardCount := 10

	_, _, baReplicator, assembler := createAssembler(t, shardCount)
	blockLedger := make(naiveBlockLedger)
	assembler.Ledger = blockLedger

	replicator := &shardReplicator{}
	for i := 0; i < shardCount; i++ {
		replicator.subscribers = append(replicator.subscribers, make(chan Batch))
	}
	assembler.Replicator = replicator

	consenterLedger := make(naiveConsensusLedger)

	totalOrder := make(naiveTotalOrder, 1)

	consenter := &Consenter{
		BatchAttestationFromBytes: func(bytes []byte) BatchAttestation {
			nba := &naiveBatchAttestation{}
			nba.Deserialize(bytes)
			return nba
		},
		ConsensusLedger: consenterLedger,
		Logger:          createLogger(t, 0),
		TotalOrder:      totalOrder,
	}

	go func() {
		for rawBytes := range consenterLedger {
			fmt.Println("Got attestation from consenter ledger of size", len(rawBytes), "bytes")
			ba := &naiveBatchAttestation{}
			ba.Deserialize(rawBytes)
			fmt.Println(">>>", ba.digest)
			baReplicator <- ba
		}
	}()

	var batchers []*Batcher

	for i := 0; i < shardCount; i++ {
		batcher := createBatcher(t, i)
		batcher.OnCollectAttestations = func(seq uint64, digest []byte, m map[uint16][]byte) {
			ba := &naiveBatchAttestation{
				digest: digest,
				seq:    seq,
			}
			consenter.Submit(ba)
		}
		batcher.Quorum = 1
		batchers = append(batchers, batcher)
	}

	for i := 0; i < shardCount; i++ {
		sc := &shardCommitter{
			shardID: uint16(i),
			sr:      replicator,
		}
		batchers[i].Ledger = sc
		batchers[i].Replicator = nil
		batchers[i].Primary = uint16(i)
		batchers[i].run()
	}

	time.Sleep(time.Second)

	assembler.run()
	consenter.run()

	batchers[0].Submit([]byte{1, 2, 3})

	<-blockLedger
}
