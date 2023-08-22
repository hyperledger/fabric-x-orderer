package arma

import (
	"bytes"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
)

type Rule func(*State, Logger, ...ControlEvent)

var Rules = []Rule{
	CollectAndDeduplicateEvents,
	//DetectEquivocation, // TODO: false positive, lets find out why
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
	Complaints []Complaint
}

type RawState struct {
	Config     []byte
	Shards     []byte
	Pending    []byte
	Complaints []byte
}

func (s *State) Serialize() []byte {

	if s.ShardCount != uint16(len(s.Shards)) {
		panic(fmt.Sprintf("shard count is %d but detected %d shards", s.ShardCount, len(s.Shards)))
	}

	rawState := RawState{
		Complaints: complaintsToBytes(s.Complaints),
		Pending:    fragmentsToBytes(s.Pending),
		Shards:     shardsToBytes(s.Shards),
		Config:     s.configToBytes(),
	}

	buff, err := asn1.Marshal(rawState)
	if err != nil {
		panic(err)
	}

	return buff
}

func (s *State) configToBytes() []byte {
	buff := make([]byte, 2*4)
	binary.BigEndian.PutUint16(buff, s.N)
	binary.BigEndian.PutUint16(buff[2:], s.Quorum)
	binary.BigEndian.PutUint16(buff[4:], s.Threshold)
	binary.BigEndian.PutUint16(buff[6:], s.ShardCount)
	return buff
}

func shardsToBytes(shards []ShardTerm) []byte {
	buff := make([]byte, len(shards)*(2+8))

	var pos int
	for _, shard := range shards {
		binary.BigEndian.PutUint16(buff[pos:], shard.Shard)
		pos += 2
		binary.BigEndian.PutUint64(buff[pos:], shard.Term)
		pos += 8
	}
	return buff
}

func complaintsToBytes(complaints []Complaint) []byte {
	cBuff := bytes.Buffer{}
	for _, c := range complaints {
		cBytes := c.Bytes()
		cByteLenBuff := make([]byte, 4)
		binary.BigEndian.PutUint32(cByteLenBuff, uint32(len(cBytes)))
		cBuff.Write(cByteLenBuff)
		cBuff.Write(cBytes)
	}

	cBuffBytes := cBuff.Bytes()
	return cBuffBytes
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

func (s *State) DeSerialize(rawBytes []byte, fragmentFromBytes func([]byte) BatchAttestationFragment) error {
	s.Pending = nil
	s.Shards = nil
	s.Complaints = nil

	var rs RawState
	if _, err := asn1.Unmarshal(rawBytes, &rs); err != nil {
		return err
	}

	s.loadConfig(rs.Config)
	s.loadShards(rs.Shards, int(s.ShardCount))
	s.loadPending(rs.Pending, fragmentFromBytes)
	if err := s.loadComplaints(rs.Complaints); err != nil {
		return fmt.Errorf("failed loading complaints: %v", err)
	}

	return nil
}

func (s *State) loadPending(buff []byte, fragmentFromBytes func([]byte) BatchAttestationFragment) {
	var pending []BatchAttestationFragment

	var pos int
	for pos < len(buff) {
		lengthOfBAF := binary.BigEndian.Uint32(buff[pos:])
		pos += 4
		bafBytes := make([]byte, lengthOfBAF)
		copy(bafBytes, buff[pos:])
		pos += int(lengthOfBAF)
		pending = append(pending, fragmentFromBytes(bafBytes))
	}

	s.Pending = pending
}

func (s *State) loadComplaints(buff []byte) error {
	var complaints []Complaint

	var pos int
	for pos < len(buff) {
		lengthOfComplaint := binary.BigEndian.Uint32(buff[pos:])
		pos += 4
		rawComplaint := make([]byte, lengthOfComplaint)
		copy(rawComplaint, buff[pos:])
		pos += int(lengthOfComplaint)

		var c Complaint
		if err := c.FromBytes(rawComplaint); err != nil {
			return err
		}

		complaints = append(complaints, c)
	}

	s.Complaints = complaints
	return nil
}

func (s *State) loadShards(rawBytes []byte, count int) {
	var pos int
	shards := make([]ShardTerm, int(s.ShardCount))
	for i := 0; i < count; i++ {
		shards[i] = ShardTerm{
			Shard: binary.BigEndian.Uint16(rawBytes[pos:]),
			Term:  binary.BigEndian.Uint64(rawBytes[pos+2:]),
		}
		pos += 10
	}

	s.Shards = shards
}

func (s *State) loadConfig(buff []byte) {
	s.N = binary.BigEndian.Uint16(buff[0:2])
	s.Quorum = binary.BigEndian.Uint16(buff[2:4])
	s.Threshold = binary.BigEndian.Uint16(buff[4:6])
	s.ShardCount = binary.BigEndian.Uint16(buff[6:8])
}

type ShardTerm struct {
	Shard uint16
	Term  uint64
}

type Complaint struct {
	ShardTerm
	Signer    uint16
	Signature []byte
}

func (c *Complaint) String() string {
	return fmt.Sprintf("{Shard: %d, Term: %d}", c.Shard, c.Term)
}

func (c *Complaint) Bytes() []byte {
	buff := make([]byte, 12+len(c.Signature))
	var pos int
	binary.BigEndian.PutUint16(buff, c.Shard)
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], c.Term)
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], c.Signer)
	pos += 2
	copy(buff[pos:], c.Signature)
	return buff
}

func (c *Complaint) FromBytes(bytes []byte) error {
	if len(bytes) <= 12 {
		return fmt.Errorf("input too small (%d < 12)", len(bytes))
	}

	c.Shard = binary.BigEndian.Uint16(bytes)
	c.Term = binary.BigEndian.Uint64(bytes[2:])
	c.Signer = binary.BigEndian.Uint16(bytes[10:])
	c.Signature = bytes[12:]
	return nil
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
	Complaint *Complaint
}

func (ce *ControlEvent) Bytes() []byte {
	var bytes []byte
	switch {
	case ce.BAF != nil:
		rawBAF := ce.BAF.Serialize()
		bytes = make([]byte, len(rawBAF)+1)
		bytes[0] = 1
		copy(bytes[1:], rawBAF)
	case ce.AntiBAF != nil:
		panic("not supported yet")
	case ce.Complaint != nil:
		rawComplaint := ce.Complaint.Bytes()
		bytes = make([]byte, len(rawComplaint)+1)
		bytes[0] = 2
		copy(bytes[1:], rawComplaint)
	default:
		panic("empty control event")
	}

	return bytes
}

func (ce *ControlEvent) FromBytes(bytes []byte, fragmentFromBytes func([]byte) (BatchAttestationFragment, error)) error {
	var err error
	switch b := bytes[0]; b {
	case 1:
		ce.BAF, err = fragmentFromBytes(bytes[1:])
		return err
	case 2:
		ce.Complaint = &Complaint{}
		return ce.Complaint.FromBytes(bytes[1:])
	}

	return fmt.Errorf("unknown prefix (%d)", bytes[0])
}

func (s *State) Process(l Logger, ces ...ControlEvent) (State, []BatchAttestationFragment) {

	s2 := s.Clone()

	for _, rule := range Rules {
		rule(&s2, l, ces...)
	}

	// After applying rules, extract all batch attestations for which enough fragments have been collected.
	extracted := ExtractBatchAttestationsFromPending(&s2, l)

	return s2, extracted
}

func (s *State) Clone() State {
	var s2 State
	s2 = *s
	s2.Shards = make([]ShardTerm, len(s.Shards))
	s2.Pending = make([]BatchAttestationFragment, len(s.Pending))
	s2.Complaints = make([]Complaint, len(s.Complaints))
	copy(s2.Shards, s.Shards)
	copy(s2.Pending, s.Pending)
	copy(s2.Complaints, s.Complaints)
	return s2
}

func CleanupOldEComplaints(s *State, l Logger, _ ...ControlEvent) {
	var newComplaints []Complaint
	for _, c := range s.Complaints {
		term := s.Shards[int(c.Shard-1)].Term
		if c.Term < term {
			l.Infof("Cleaning complaint of shard %d for term %d as the current term is %d", c.Shard, c.Term, term)
			continue
		}
		newComplaints = append(newComplaints, c)
	}
}

func PrimaryRotateDueToComplaints(s *State, l Logger, _ ...ControlEvent) {
	complaintsToNum := make(map[ShardTerm]int)

	for _, complaint := range s.Complaints {

		if len(s.Shards) < int(complaint.Shard) {
			l.Errorf("Got complaint for shard %d but only have %d shards, ignoring complaint", complaint.Shard, len(s.Shards))
			continue
		}

		term := s.Shards[complaint.Shard].Term
		if term != complaint.Term {
			l.Infof("Got complaint for shard %d in term %d but shard is at term %d", complaint.Shard, complaint.Term, term)
			continue
		}

		complaintsToNum[complaint.ShardTerm]++

	}

	var newComplaints []Complaint

	for _, complaint := range s.Complaints {

		if complaintsToNum[complaint.ShardTerm] >= int(s.Quorum) {

			term := s.Shards[int(complaint.Shard-1)].Term
			if term != complaint.Term {
				l.Infof("Got complaint for shard %d in term %d but shard is at term %d", complaint.Shard, complaint.Term, term)
				continue
			}

			complaintNum := complaintsToNum[complaint.ShardTerm]
			oldTerm := s.Shards[complaint.Shard-1].Term
			oldPrimary := uint16((oldTerm + uint64(complaint.Shard)) % uint64(s.N))

			s.Shards[complaint.Shard-1].Term++

			newTerm := s.Shards[complaint.Shard-1].Term
			newPrimary := uint16((newTerm + uint64(complaint.Shard)) % uint64(s.N))

			l.Infof("Shard %d advanced from term %d to term %d due to %d complaintsToNum (quorum is %d), and the primary switched from %d to %d",
				complaint.Shard, oldTerm, newTerm, complaintNum, s.Quorum, oldPrimary, newPrimary)
		} else {
			newComplaints = append(newComplaints, complaint)
		}
	}

	s.Complaints = newComplaints

}

func CollectAndDeduplicateEvents(s *State, l Logger, ces ...ControlEvent) {
	shardsAndSequences := make(map[batchAttestationVote]struct{}, len(s.Pending))
	complaints := make(map[ShardTerm]map[uint16]struct{})

	for _, baf := range s.Pending {
		shardsAndSequences[batchAttestationVote{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary(), signer: baf.Signer()}] = struct{}{}
	}

	for _, complaint := range s.Complaints {
		if _, exists := complaints[complaint.ShardTerm]; !exists {
			complaints[complaint.ShardTerm] = make(map[uint16]struct{})
		}
		complaints[complaint.ShardTerm][complaint.Signer] = struct{}{}
	}

	for _, ce := range ces {
		if ce.BAF == nil && ce.Complaint == nil {
			continue
		}

		if ce.BAF != nil {
			shard := ce.BAF.Shard()
			if len(s.Shards) < int(shard) {
				l.Warnf("Got Batch Attestation Fragment for shard %d but only have %d shards, ignoring it", ce.BAF.Shard(), len(s.Shards))
				continue
			}

			if _, exists := shardsAndSequences[batchAttestationVote{seq: ce.BAF.Seq(), shard: ce.BAF.Shard(), primary: ce.BAF.Primary(), signer: ce.BAF.Signer()}]; exists {
				l.Warnf("Node %d already signed Batch Attestation Fragment for sequence %d from primary %d in shard %d",
					ce.BAF.Signer(), ce.BAF.Seq(), ce.BAF.Primary(), ce.BAF.Shard())
				continue
			}

			s.Pending = append(s.Pending, ce.BAF)
		}

		if ce.Complaint != nil {
			shard := ce.Complaint.Shard
			if len(s.Shards) <= int(shard) {
				l.Warnf("Got complaint for shard %d but only have %d shards, ignoring it", ce.Complaint.Shard, len(s.Shards))
				continue
			}

			if complainers, exists := complaints[ce.Complaint.ShardTerm]; exists {
				if _, exists := complainers[ce.Complaint.Signer]; exists {
					l.Warnf("Node %d already signed complaint for shard %d and term %d", ce.Complaint.Shard, ce.Complaint.Term)
					continue
				}
			}
			s.Complaints = append(s.Complaints, *ce.Complaint)
		}
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

		l.Debugf("A total of %d digests where found for seq %d in shard %d with primary %d", len(digest2signers), batchAttestation.seq, batchAttestation.shard, batchAttestation.primary)

		for _, signers := range digest2signers {
			if len(signers) >= int(s.Threshold) {
				foundThreshold = true
				l.Infof("Found threshold (%d > %d) of batch attestation fragments for shard %d, seq %d", len(signers), s.Threshold-1, batchAttestation.shard, batchAttestation.seq)
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
