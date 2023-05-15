package arma

import "testing"

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

type shardReplicator struct {
	subscribers []chan Batch
}

func (s *shardReplicator) Append(party uint16, _ uint64, rawBatch []byte) {
	nb := &naiveBatch{
		requests: BatchFromRaw(rawBatch),
		node:     party,
	}

	for _, sub := range s.subscribers {
		sub <- nb
	}
}

func (s *shardReplicator) Replicate(shard uint16, seq uint64) <-chan Batch {
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

	consenterLedger := make(naiveConsensusLedger)

	consenter := &Consenter{
		BatchAttestationFromBytes: func(bytes []byte) BatchAttestation {
			nba := &naiveBatchAttestation{}
			nba.Deserialize(bytes)
			return nba
		},
		ConsensusLedger: consenterLedger,
		Logger:          createLogger(t, 0),
		TotalOrder:      make(naiveTotalOrder),
	}

	go func() {
		for rawBytes := range consenterLedger {
			ba := &naiveBatchAttestation{}
			ba.Deserialize(rawBytes)
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
		batchers[i].Ledger = replicator
		batchers[i].Replicator = nil
	}

}
