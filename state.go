package arma

import (
	"bytes"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
)

type Rule func(*State, Logger, ...ControlEvent)

var Rules = []Rule{
	CollectAndDeduplicateBatchAttestations,
	DetectEquivocation,
	PrimaryRotateDueToComplaints,
}

type batchAttestationVote struct {
	seq     uint64
	shard   uint16
	primary uint16
	signer  uint16
}

type State struct {
	N          uint16
	Quorum     uint16
	Threshold  uint16
	ShardCount uint16
	Shards     []ShardTerm
	Pending    []BatchAttestationFragment
	Aborted    []BatchAttestationFragment
}

func (s *State) Serialize() []byte {

	if s.ShardCount != uint16(len(s.Shards)) {
		panic(fmt.Sprintf("shard count is %d but detected %d shards", s.ShardCount, len(s.Shards)))
	}

	fragmentBuffBytes := fragmentsToBytes(s.Pending)
	abortedBuffBytes := fragmentsToBytes(s.Aborted)

	buff := make([]byte, len(abortedBuffBytes)+len(fragmentBuffBytes)+2+2+2+2+len(s.Shards)*(2+8))
	binary.BigEndian.PutUint16(buff, s.N)
	binary.BigEndian.PutUint16(buff[2:], s.Quorum)
	binary.BigEndian.PutUint16(buff[4:], s.Threshold)
	binary.BigEndian.PutUint16(buff[6:], s.ShardCount)
	pos := 8
	for _, shard := range s.Shards {
		binary.BigEndian.PutUint16(buff[pos:], shard.Shard)
		pos += 2
		binary.BigEndian.PutUint64(buff[pos:], shard.Term)
		pos += 8
	}

	copy(buff[pos:], fragmentBuffBytes)
	pos = len(fragmentBuffBytes)

	copy(buff[pos:], abortedBuffBytes)

	return buff
}

func fragmentsToBytes(fragments []BatchAttestationFragment) []byte {
	fragmentBuff := bytes.Buffer{}
	for _, baf := range fragments {
		bafBytes := baf.Serialize()
		bafByteLenBuff := make([]byte, 4)
		binary.BigEndian.PutUint32(bafByteLenBuff, uint32(len(bafBytes)))
		fragmentBuff.Write(bafByteLenBuff)
		fragmentBuff.Write(bafBytes)
	}

	fragmentBuffBytes := fragmentBuff.Bytes()
	return fragmentBuffBytes
}

func (s *State) DeSerialize(buff []byte, fragmentFromBytes func([]byte) BatchAttestationFragment) {
	s.Pending = nil
	s.Shards = nil

	s.N = binary.BigEndian.Uint16(buff[0:2])
	s.Quorum = binary.BigEndian.Uint16(buff[2:4])
	s.Threshold = binary.BigEndian.Uint16(buff[4:6])
	s.ShardCount = binary.BigEndian.Uint16(buff[6:8])

	pos := 8
	shards := make([]ShardTerm, int(s.ShardCount))
	for i := 0; i < int(s.ShardCount); i++ {
		shards[i] = ShardTerm{
			Shard: binary.BigEndian.Uint16(buff[pos:]),
			Term:  binary.BigEndian.Uint64(buff[pos+2:]),
		}
		pos += 10
	}

	s.Shards = shards

	var fragments []BatchAttestationFragment

	for pos < len(buff) {
		lengthOfBAF := binary.BigEndian.Uint32(buff[pos:])
		pos += 4
		bafBytes := make([]byte, lengthOfBAF)
		copy(bafBytes, buff[pos:])
		pos += int(lengthOfBAF)
		fragments = append(fragments, fragmentFromBytes(bafBytes))
	}

	s.Pending = fragments
}

type ShardTerm struct {
	Shard uint16
	Term  uint64
}

type AntiBatchAttestationFragment struct {
	Seq     uint64
	Primary uint16
	Signer  uint16
	Shard   uint16
	Digest  string
}

type ControlEvent struct {
	BAF       BatchAttestationFragment
	AntiBAF   *AntiBatchAttestationFragment
	Complaint *ShardTerm
}

func (s *State) Process(l Logger, ces ...ControlEvent) ([]byte, []BatchAttestationFragment) {

	for _, rule := range Rules {
		rule(s, l, ces...)
	}

	// After applying rules, extract all batch attestations for which enough fragments have been collected.
	extracted := ExtractBatchAttestationsFromPending(s, l)

	return s.Serialize(), extracted
}

func (s *State) Init(rawBytes []byte) error {
	rest, err := asn1.Unmarshal(rawBytes, s)
	if len(rest) == 0 {
		return fmt.Errorf("found trailing bytes (%x)", rest)
	}

	if err != nil {
		return fmt.Errorf("failed parsing raw state (%x): %v", rawBytes, err)
	}

	return nil
}

func PrimaryRotateDueToComplaints(s *State, l Logger, ces ...ControlEvent) {
	for _, ce := range ces {
		if ce.Complaint == nil {
			return
		}

		if len(s.Shards) <= int(ce.Complaint.Shard) {
			l.Errorf("Got complaint for shard %d but only have %d shards, ignoring complaint", ce.Complaint.Shard, len(s.Shards))
			continue
		}

		term := s.Shards[ce.Complaint.Shard].Term
		if term != ce.Complaint.Term {
			l.Infof("Got complaint for shard %d in term %d but shard is at term %d", ce.Complaint.Shard, ce.Complaint.Term, term)
			continue
		}

		oldTerm := s.Shards[ce.Complaint.Shard].Term
		oldPrimary := uint16(oldTerm % uint64(s.N))

		s.Shards[ce.Complaint.Shard].Term++

		newTerm := s.Shards[ce.Complaint.Shard].Term
		newPrimary := uint16(newTerm % uint64(s.N))

		l.Infof("Shard %d advanced from term %d to term %d, and the primary switched from %d to %d",
			ce.Complaint.Shard, oldTerm, newTerm, oldPrimary, newPrimary)
	}
}

func CollectAndDeduplicateBatchAttestations(s *State, l Logger, ces ...ControlEvent) {
	shardsAndSequences := make(map[batchAttestationVote]struct{}, len(s.Pending))

	for _, baf := range s.Pending {
		shardsAndSequences[batchAttestationVote{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary(), signer: baf.Signer()}] = struct{}{}
	}

	for _, ce := range ces {
		if ce.BAF == nil {
			continue
		}

		shard := ce.BAF.Shard()
		if len(s.Shards) <= int(shard) {
			l.Warnf("Got Batch Attestation Fragment for shard %d but only have %d shards, ignoring it", ce.Complaint.Shard, len(s.Shards))
			continue
		}

		if _, exists := shardsAndSequences[batchAttestationVote{seq: ce.BAF.Seq(), shard: ce.BAF.Shard(), primary: ce.BAF.Primary(), signer: ce.BAF.Signer()}]; exists {
			l.Warnf("Node %d already signed Batch Attestation Fragment for sequence %d from primary %d in shard %d",
				ce.BAF.Signer(), ce.BAF.Seq(), ce.BAF.Primary(), ce.BAF.Shard())
			continue
		}

		s.Pending = append(s.Pending, ce.BAF)
	}
}

func DetectEquivocation(s *State, l Logger, _ ...ControlEvent) {
	// We have a total of N parties per shard.
	// We collect a quorum of signatures and then wait for f+1 identical ones.
	// If we can't collect such, it means the primary equivocated.

	// <seq, shard, primary> --> { digest -->  signer }
	m := batchAttestationVotesByDigests(s)

	// For each <seq, shard, primary> check if it has a digest with a quorum of votes.

	for batchAttestation, digest2signers := range m {

		var foundThreshold bool

		var totalSigners int

		for _, signers := range digest2signers {
			totalSigners += len(signers)
			if len(signers) >= int(s.Threshold) {
				foundThreshold = true
				break
			}
		}

		if totalSigners >= int(s.Quorum) && !foundThreshold {
			l.Warnf("batch attestation sequence %d in shard %d of primary %d"+
				" has more than %d distinct signers but no threshold of signers signed on the same digest (%v)",
				batchAttestation.seq, batchAttestation.shard, batchAttestation.primary, totalSigners, digest2signers)

			for _, shard := range s.Shards {
				term := shard.Term
				currentPrimary := uint16(term % uint64(s.N))
				if currentPrimary == batchAttestation.primary {
					l.Warnf("Rotating primary %d (term %d -> %d) in shard %d due to equivocation for sequence %d in shard %d",
						batchAttestation.primary, shard.Term, shard.Term+1, shard.Shard, batchAttestation.seq, batchAttestation.shard)
					shard.Term++
				}
			} // for all shards
		} // equivocation detected
	} // for all <seq, shard, primary>
}

func batchAttestationVotesByDigests(s *State) map[batchAttestationVote]map[string][]uint16 {
	m := make(map[batchAttestationVote]map[string][]uint16)

	for _, baf := range s.Pending {
		currentVote := batchAttestationVote{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary()}

		digests2signers, exists := m[currentVote]
		if !exists {
			digests2signers = make(map[string][]uint16)
			m[currentVote] = digests2signers
		}

		digests2signers[string(baf.Digest())] = append(digests2signers[string(baf.Digest())], baf.Signer())
	}
	return m
}

func ExtractBatchAttestationsFromPending(s *State, l Logger) []BatchAttestationFragment {
	// <seq, shard, primary> --> { digest -->  signer }
	m := batchAttestationVotesByDigests(s)

	batchAttestationsWithThreshold := make(map[batchAttestationVote]struct{})

	for batchAttestation, digest2signers := range m {
		var foundThreshold bool

		var totalSigners int

		for _, signers := range digest2signers {
			totalSigners += len(signers)
			if len(signers) >= int(s.Threshold) {
				foundThreshold = true
				l.Infof("Found threshold (%d > %d) of batch attestation fragments for shard %d, seq %d", len(signers), s.Threshold, batchAttestation.shard, batchAttestation.seq)
				break
			}
		}

		if !foundThreshold {
			l.Infof("Could not find a threshold of batch attestation fragments for shard %d, seq %d", batchAttestation.shard, batchAttestation.seq)
			continue
		}

		batchAttestationsWithThreshold[batchAttestation] = struct{}{}

	} // for all <seq, shard, primary>

	var extracted []BatchAttestationFragment

	newPending := make([]BatchAttestationFragment, 0, len(s.Pending))

	// We iterate over the pending because we need deterministic processing
	for _, baf := range s.Pending {
		if _, exists := batchAttestationsWithThreshold[batchAttestationVote{
			seq:     baf.Seq(),
			shard:   baf.Shard(),
			primary: baf.Primary(),
		}]; !exists {
			newPending = append(newPending, baf)
		} else {
			extracted = append(extracted, baf)
		}
	}

	s.Pending = newPending

	return extracted
}
