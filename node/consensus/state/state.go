/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric/protoutil"
)

type Rule func(*State, types.Logger, ...ControlEvent)

var Rules = []Rule{
	CollectAndDeduplicateEvents,
	// DetectEquivocation, // TODO: false positive, lets find out why
	PrimaryRotateDueToComplaints,
	CleanupOldComplaints,
	// CleanupOldAttestations, // TODO: fully test for byzantine failures
}

type batchAttestationVote struct {
	seq     types.BatchSequence
	shard   types.ShardID
	primary types.PartyID
	signer  types.PartyID
}

type State struct {
	N          uint16
	Quorum     uint16
	Threshold  uint16
	ShardCount uint16
	Shards     []ShardTerm
	Pending    []types.BatchAttestationFragment
	Complaints []Complaint
	AppContext []byte
}

type RawState struct {
	Config     []byte
	Shards     []byte
	Pending    []byte
	Complaints []byte
	AppContext []byte
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
		AppContext: s.AppContext,
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
	if len(shards) == 0 {
		return nil
	}
	buff := make([]byte, len(shards)*(2+8))

	var pos int
	for _, shard := range shards {
		binary.BigEndian.PutUint16(buff[pos:], uint16(shard.Shard))
		pos += 2
		binary.BigEndian.PutUint64(buff[pos:], shard.Term)
		pos += 8
	}
	return buff
}

func complaintsToBytes(complaints []Complaint) []byte {
	if len(complaints) == 0 {
		return nil
	}
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

func fragmentsToBytes(fragments []types.BatchAttestationFragment) []byte {
	if len(fragments) == 0 {
		return nil
	}
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

func (s *State) Deserialize(rawBytes []byte, bafd BAFDeserializer) error {
	s.Pending = nil
	s.Shards = nil
	s.Complaints = nil

	var rs RawState
	if _, err := asn1.Unmarshal(rawBytes, &rs); err != nil {
		return err
	}

	s.loadConfig(rs.Config)
	s.loadShards(rs.Shards, int(s.ShardCount))
	if err := s.loadPending(rs.Pending, bafd); err != nil {
		return fmt.Errorf("failed loading batch attestation fragments: %v", err)
	}
	if err := s.loadComplaints(rs.Complaints); err != nil {
		return fmt.Errorf("failed loading complaints: %v", err)
	}

	s.AppContext = rs.AppContext

	return nil
}

func (s *State) loadPending(buff []byte, bafd BAFDeserializer) error {
	var pending []types.BatchAttestationFragment

	var pos int
	for pos < len(buff) {
		lengthOfBAF := binary.BigEndian.Uint32(buff[pos:])
		pos += 4
		bafBytes := make([]byte, lengthOfBAF)
		copy(bafBytes, buff[pos:])
		pos += int(lengthOfBAF)
		baf, err := bafd.Deserialize(bafBytes)
		if err != nil {
			return err
		}
		pending = append(pending, baf)
	}

	if len(pending) == 0 {
		s.Pending = nil
		return nil
	}

	s.Pending = pending

	return nil
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

	if len(complaints) == 0 {
		s.Complaints = nil
		return nil
	}

	s.Complaints = complaints
	return nil
}

func (s *State) loadShards(rawBytes []byte, count int) {
	if count == 0 {
		s.Shards = nil
		return
	}
	var pos int
	shards := make([]ShardTerm, int(s.ShardCount))
	for i := 0; i < count; i++ {
		shards[i] = ShardTerm{
			Shard: types.ShardID(binary.BigEndian.Uint16(rawBytes[pos:])),
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
	Shard types.ShardID
	Term  uint64
}

type Complaint struct {
	ShardTerm
	Signer    types.PartyID
	Signature []byte
	Reason    string
}

func (c *Complaint) Bytes() []byte {
	reasonLen := len([]byte(c.Reason))
	if reasonLen > math.MaxUint16 {
		reasonLen = math.MaxUint16
	}
	buff := make([]byte, 16+len(c.Signature)+reasonLen)
	var pos int
	binary.BigEndian.PutUint16(buff, uint16(c.Shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], c.Term)
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(c.Signer))
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(len(c.Signature)))
	pos += 2
	copy(buff[pos:pos+len(c.Signature)], c.Signature)
	pos += len(c.Signature)
	binary.BigEndian.PutUint16(buff[pos:], uint16(reasonLen))
	pos += 2
	copy(buff[pos:pos+reasonLen], []byte(c.Reason))
	return buff
}

func (c *Complaint) FromBytes(bytes []byte) error {
	if len(bytes) <= 16 {
		return fmt.Errorf("input too small (%d <= 16)", len(bytes))
	}
	c.Shard = types.ShardID(binary.BigEndian.Uint16(bytes))
	c.Term = binary.BigEndian.Uint64(bytes[2:10])
	c.Signer = types.PartyID(binary.BigEndian.Uint16(bytes[10:12]))
	sigSize := binary.BigEndian.Uint16(bytes[12:14])
	c.Signature = bytes[14 : 14+sigSize]
	rSize := binary.BigEndian.Uint16(bytes[14+sigSize : 14+sigSize+2])
	c.Reason = string(bytes[14+int(sigSize)+2 : 14+int(sigSize)+2+int(rSize)])
	return nil
}

func (c *Complaint) ToBeSigned() []byte {
	toBeSignedComplaint := Complaint{
		ShardTerm: c.ShardTerm,
		Signer:    c.Signer,
		Signature: nil,
		Reason:    c.Reason,
	}
	return toBeSignedComplaint.Bytes()
}

func (c *Complaint) String() string {
	return fmt.Sprintf("Complaint: Signer: %d; Shard: %d; Term %d; Reason: %s", c.Signer, c.Shard, c.Term, c.Reason)
}

type ConfigRequest struct {
	Envelope *common.Envelope
}

func (c *ConfigRequest) Bytes() []byte {
	bytes, err := protoutil.Marshal(c.Envelope)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal envelope: %v", err))
	}

	return bytes
}

func (c *ConfigRequest) FromBytes(bytes []byte) error {
	envelope, err := protoutil.UnmarshalEnvelope(bytes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal envelope: %v", err)
	}

	c.Envelope = envelope
	return nil
}

func (c *ConfigRequest) String() string {
	// TODO: add more info to this string, at least the config sequence
	return "Config Request"
}

type ControlEvent struct {
	BAF           types.BatchAttestationFragment
	Complaint     *Complaint
	ConfigRequest *ConfigRequest
}

func (ce *ControlEvent) String() string {
	if ce.Complaint != nil {
		return ce.Complaint.String()
	} else if ce.BAF != nil {
		return ce.BAF.String()
	} else if ce.ConfigRequest != nil {
		return ce.ConfigRequest.String()
	}
	return "empty control event"
}

// ID returns a string representing the specific control event
func (ce *ControlEvent) ID() string {
	var payloadToHash []byte
	switch {
	case ce.BAF != nil:
		payloadToHash = make([]byte, 22+32) // seq and config sequence are uint64, signer, primary and shard are uint16, and digest is 32 bytes
		binary.BigEndian.PutUint64(payloadToHash, uint64(ce.BAF.Seq()))
		binary.BigEndian.PutUint64(payloadToHash[8:], uint64(ce.BAF.ConfigSequence()))
		binary.BigEndian.PutUint16(payloadToHash[16:], uint16(ce.BAF.Signer()))
		binary.BigEndian.PutUint16(payloadToHash[18:], uint16(ce.BAF.Primary()))
		binary.BigEndian.PutUint16(payloadToHash[20:], uint16(ce.BAF.Shard()))
		copy(payloadToHash[22:], ce.BAF.Digest())
	case ce.Complaint != nil:
		complaintWithNoSig := &Complaint{
			ShardTerm: ce.Complaint.ShardTerm,
			Signer:    ce.Complaint.Signer,
			Reason:    ce.Complaint.Reason,
		}
		payloadToHash = complaintWithNoSig.Bytes()
	case ce.ConfigRequest != nil:
		// TODO: maybe use a different ID for ConfigRequest
		payloadToHash = ce.ConfigRequest.Bytes()
	default:
		return ""
	}
	dig := sha256.Sum256(payloadToHash)
	return hex.EncodeToString(dig[:])
}

// SignerID returns a string representing the signer of the specific control event
func (ce *ControlEvent) SignerID() string {
	switch {
	case ce.BAF != nil:
		return fmt.Sprintf("%d", ce.BAF.Signer())
	case ce.Complaint != nil:
		return fmt.Sprintf("%d", ce.Complaint.Signer)
	case ce.ConfigRequest != nil:
		// TODO: add ConfigRequest SignerID
		return ""
	default:
		return ""
	}
}

func (ce *ControlEvent) Bytes() []byte {
	var bytes []byte
	switch {
	case ce.BAF != nil:
		rawBAF := ce.BAF.Serialize()
		bytes = make([]byte, len(rawBAF)+1)
		bytes[0] = 1
		copy(bytes[1:], rawBAF)
	case ce.Complaint != nil:
		rawComplaint := ce.Complaint.Bytes()
		bytes = make([]byte, len(rawComplaint)+1)
		bytes[0] = 2
		copy(bytes[1:], rawComplaint)
	case ce.ConfigRequest != nil:
		rawConfig := ce.ConfigRequest.Bytes()
		bytes = make([]byte, len(rawConfig)+1)
		bytes[0] = 3
		copy(bytes[1:], rawConfig)
	default:
		panic("empty control event")
	}

	return bytes
}

func (ce *ControlEvent) FromBytes(bytes []byte, fragmentFromBytes func([]byte) (types.BatchAttestationFragment, error)) error {
	var err error
	switch b := bytes[0]; b {
	case 1:
		ce.BAF, err = fragmentFromBytes(bytes[1:])
		return err
	case 2:
		ce.Complaint = &Complaint{}
		return ce.Complaint.FromBytes(bytes[1:])
	case 3:
		ce.ConfigRequest = &ConfigRequest{}
		return ce.ConfigRequest.FromBytes(bytes[1:])
	}

	return fmt.Errorf("unknown prefix (%d)", bytes[0])
}

func (s *State) Process(l types.Logger, ces ...ControlEvent) (*State, []types.BatchAttestationFragment, []*ConfigRequest) {
	nextState := s.Clone()

	for _, rule := range Rules {
		rule(nextState, l, ces...)
	}

	// After applying rules, extract all batch attestations for which enough fragments have been collected.
	extracted := ExtractBatchAttestationsFromPending(nextState, l)
	configRequests := ExtractConfigRequests(ces)
	return nextState, extracted, configRequests
}

func (s *State) Clone() *State {
	s2 := *s
	s2.Shards = make([]ShardTerm, len(s.Shards))
	s2.Pending = make([]types.BatchAttestationFragment, len(s.Pending))
	s2.Complaints = make([]Complaint, len(s.Complaints))
	copy(s2.Shards, s.Shards)
	copy(s2.Pending, s.Pending)
	copy(s2.Complaints, s.Complaints)
	s2.AppContext = nil
	if s.AppContext != nil {
		s2.AppContext = make([]byte, 0, len(s.AppContext))
		s2.AppContext = append(s2.AppContext, s.AppContext...)
	}
	return &s2
}

func CleanupOldComplaints(s *State, l types.Logger, _ ...ControlEvent) {
	newComplaints := make([]Complaint, 0, len(s.Complaints))
	for _, c := range s.Complaints {
		shardIndex, _ := shardExists(c.Shard, s.Shards)
		term := s.Shards[shardIndex].Term
		if c.Term < term {
			l.Infof("Cleaning complaint of shard %d for term %d as the current term is %d", c.Shard, c.Term, term)
			continue
		}
		newComplaints = append(newComplaints, c)
	}

	s.Complaints = newComplaints
}

func PrimaryRotateDueToComplaints(s *State, l types.Logger, _ ...ControlEvent) {
	complaintsToNum := make(map[ShardTerm]int)

	for _, complaint := range s.Complaints {
		shardIndex, exsits := shardExists(complaint.Shard, s.Shards)
		if !exsits {
			l.Errorf("Got complaint for shard %d but it was not found in the shards: %v, ignoring complaint", complaint.Shard, s.Shards)
			continue
		}

		term := s.Shards[shardIndex].Term
		if term != complaint.Term {
			l.Infof("Got complaint for shard %d in term %d but shard is at term %d", complaint.Shard, complaint.Term, term)
			continue
		}

		complaintsToNum[complaint.ShardTerm]++

	}

	var newComplaints []Complaint

	for _, complaint := range s.Complaints {
		if complaintsToNum[complaint.ShardTerm] >= int(s.Threshold) {

			shardIndex, _ := shardExists(complaint.Shard, s.Shards)
			term := s.Shards[shardIndex].Term
			if term != complaint.Term {
				l.Infof("Got complaint for shard %d in term %d but shard is at term %d", complaint.Shard, complaint.Term, term)
				continue
			}

			complaintNum := complaintsToNum[complaint.ShardTerm]
			oldTerm := s.Shards[shardIndex].Term
			oldPrimary := uint16((oldTerm + uint64(complaint.Shard)) % uint64(s.N))

			s.Shards[shardIndex].Term++

			newTerm := s.Shards[shardIndex].Term
			newPrimary := uint16((newTerm + uint64(complaint.Shard)) % uint64(s.N))

			l.Infof("Shard %d advanced from term %d to term %d due to %d complaints (threshold is %d), and the primary switched from %d to %d",
				complaint.Shard, oldTerm, newTerm, complaintNum, s.Threshold, oldPrimary, newPrimary)
		} else {
			newComplaints = append(newComplaints, complaint)
		}
	}

	s.Complaints = newComplaints
}

func CollectAndDeduplicateEvents(s *State, l types.Logger, ces ...ControlEvent) {
	shardsAndSequences := make(map[batchAttestationVote]struct{}, len(s.Pending))
	complaints := make(map[ShardTerm]map[types.PartyID]struct{})

	for _, baf := range s.Pending {
		shardsAndSequences[batchAttestationVote{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary(), signer: baf.Signer()}] = struct{}{}
	}

	for _, complaint := range s.Complaints {
		if _, exists := complaints[complaint.ShardTerm]; !exists {
			complaints[complaint.ShardTerm] = make(map[types.PartyID]struct{})
		}
		complaints[complaint.ShardTerm][complaint.Signer] = struct{}{}
	}

	for _, ce := range ces {
		if ce.BAF == nil && ce.Complaint == nil {
			continue
		}

		if ce.BAF != nil {
			shard := ce.BAF.Shard()
			_, exists := shardExists(shard, s.Shards)
			if !exists {
				l.Warnf("Got Batch Attestation Fragment for shard %d but it was not found in the shards: %v, ignoring it", ce.BAF.Shard(), s.Shards)
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
			st := ce.Complaint.ShardTerm
			if !slices.Contains(s.Shards, st) {
				l.Warnf("Got complaint for shard %d in term %d but it was not found in the shards: %v, ignoring it", ce.Complaint.Shard, ce.Complaint.Term, s.Shards)
				continue
			}

			if complainers, exists := complaints[st]; exists {
				if _, exists := complainers[ce.Complaint.Signer]; exists {
					l.Warnf("Node %d already signed complaint for shard %d and term %d", ce.Complaint.Shard, ce.Complaint.Term)
					continue
				}
			} else {
				complaints[st] = make(map[types.PartyID]struct{})
			}
			complaints[st][ce.Complaint.Signer] = struct{}{}
			s.Complaints = append(s.Complaints, *ce.Complaint)
		}
	}
}

func DetectEquivocation(s *State, l types.Logger, _ ...ControlEvent) {
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
				currentPrimary := types.PartyID(term % uint64(s.N))
				if currentPrimary == batchAttestation.primary {
					l.Warnf("Rotating primary %d (term %d -> %d) in shard %d due to equivocation for sequence %d in shard %d",
						batchAttestation.primary, shard.Term, shard.Term+1, shard.Shard, batchAttestation.seq, batchAttestation.shard)
					shard.Term++
				}
			} // for all shards
		} // equivocation detected
	} // for all <seq, shard, primary>
}

func batchAttestationVotesByDigests(s *State) map[batchAttestationVote]map[string][]types.PartyID {
	m := make(map[batchAttestationVote]map[string][]types.PartyID)

	for _, baf := range s.Pending {
		currentVote := batchAttestationVote{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary()}

		digests2signers, exists := m[currentVote]
		if !exists {
			digests2signers = make(map[string][]types.PartyID)
			m[currentVote] = digests2signers
		}

		digests2signers[string(baf.Digest())] = append(digests2signers[string(baf.Digest())], baf.Signer())
	}
	return m
}

func ExtractBatchAttestationsFromPending(s *State, l types.Logger) []types.BatchAttestationFragment {
	// <seq, shard, primary> --> { digest -->  signer }
	m := batchAttestationVotesByDigests(s)

	batchAttestationsWithThreshold := make(map[batchAttestationVote]struct{})

	for batchAttestation, digest2signers := range m {
		var foundThreshold bool

		l.Debugf("A total of %d digests where found for seq %d in shard %d with primary %d", len(digest2signers), batchAttestation.seq, batchAttestation.shard, batchAttestation.primary)

		for _, signers := range digest2signers {
			if len(signers) >= int(s.Threshold) {
				foundThreshold = true
				l.Debugf("Found threshold (%d > %d) of batch attestation fragments for shard %d, seq %d", len(signers), s.Threshold-1, batchAttestation.shard, batchAttestation.seq)
				break
			}
		}

		if !foundThreshold {
			l.Debugf("Could not find a threshold of batch attestation fragments for shard %d, seq %d", batchAttestation.shard, batchAttestation.seq)
			continue
		}

		batchAttestationsWithThreshold[batchAttestation] = struct{}{}

	} // for all <seq, shard, primary>

	var extracted []types.BatchAttestationFragment

	newPending := make([]types.BatchAttestationFragment, 0, len(s.Pending))

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

	oldPendingCount := len(s.Pending)
	newPendingCount := len(newPending)

	l.Debugf("Pending attestations count changed from %d to %d", oldPendingCount, newPendingCount)
	s.Pending = newPending

	return extracted
}

func shardExists(shard types.ShardID, shardTerms []ShardTerm) (int, bool) {
	for index, st := range shardTerms {
		if st.Shard == shard {
			return index, true
		}
	}
	return -1, false
}

func ExtractConfigRequests(ces []ControlEvent) []*ConfigRequest {
	// TODO: decide how to handle multiple config requests
	var reqs []*ConfigRequest
	for _, ce := range ces {
		if ce.ConfigRequest != nil {
			reqs = append(reqs, ce.ConfigRequest)
		}
	}
	return reqs
}
