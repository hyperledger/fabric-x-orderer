/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"google.golang.org/protobuf/proto"
)

type Rule func(*State, types.ConfigSequence, *flogging.FabricLogger, ...ControlEvent)

var Rules = []Rule{
	FilterPendingEventsWithDiffConfigSeq,
	CollectAndDeduplicateEvents,
	DetectEquivocation,
	PrimaryRotateDueToComplaints,
	CleanupOldComplaints,
	// CleanupOldAttestations, // TODO: fully test for byzantine failures
}

// batchKey identifies a batch by <shard, primary, seq>: the batch-identity portion of
// types.BatchID with the digest excluded. It is the unit of equivocation detection and
// extraction — a byzantine primary produces several digests under a single batchKey.
type batchKey struct {
	seq     types.BatchSequence
	shard   types.ShardID
	primary types.PartyID
}

// attestationKey identifies one signer's attestation of a specific digest of a batch: a batchKey
// plus the signer and the hex-encoded digest. It is the deduplication unit — a signer may attest
// a given (batch, digest) at most once, but the same signer reporting two different digests for
// one batch is NOT a duplicate: those two BAFs are the evidence that the primary equivocated
// (each carries the primary's verified signature over its digest), and both must be retained so
// DetectEquivocation can see them.
type attestationKey struct {
	batch  batchKey
	signer types.PartyID
	digest string
}

// thresholdDigestKey identifies a specific digest of a batch that reached the signature
// threshold: a batchKey plus the hex-encoded digest (matching the inner key produced by
// batchAttestationVotesByDigests). When a primary equivocates, one batchKey can have several
// threshold-reaching thresholdDigestKeys.
type thresholdDigestKey struct {
	batch  batchKey
	digest string
}

func batchKeyOf(baf types.BatchAttestationFragment) batchKey {
	return batchKey{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary()}
}

func attestationKeyOf(baf types.BatchAttestationFragment) attestationKey {
	return attestationKey{batch: batchKeyOf(baf), signer: baf.Signer(), digest: hex.EncodeToString(baf.Digest())}
}

type State struct {
	N          uint16
	Quorum     uint16
	Threshold  uint16
	Shards     []ShardTerm
	Pending    []types.BatchAttestationFragment
	Complaints []Complaint
	AppContext []byte
}

func (s *State) String() string {
	var pendingStr string
	if len(s.Pending) == 0 {
		pendingStr = "none"
	} else {
		pendingStr = fmt.Sprintf("%d BAFs:", len(s.Pending))
		for i, baf := range s.Pending {
			if i < 5 { // Limit to first 5 for brevity
				pendingStr += fmt.Sprintf("\n    - %s", baf.String())
			} else if i == 5 {
				pendingStr += fmt.Sprintf("\n    ... and %d more", len(s.Pending)-5)
				break
			}
		}
	}

	var complaintsStr string
	if len(s.Complaints) == 0 {
		complaintsStr = "none"
	} else {
		complaintsStr = fmt.Sprintf("%d complaints:", len(s.Complaints))
		for i, c := range s.Complaints {
			if i < 5 { // Limit to first 5 for brevity
				complaintsStr += fmt.Sprintf("\n    - %s", c.String())
			} else if i == 5 {
				complaintsStr += fmt.Sprintf("\n    ... and %d more", len(s.Complaints)-5)
				break
			}
		}
	}

	return fmt.Sprintf("State{N: %d, Quorum: %d, Threshold: %d, ShardCount: %d, \nPending: %s, \nComplaints: %s}",
		s.N, s.Quorum, s.Threshold, len(s.Shards), pendingStr, complaintsStr)
}

func (s *State) Serialize() []byte {
	// Convert State to proto stateprotos.State
	protoShards := make([]*stateprotos.ShardTerm, len(s.Shards))
	for i, shard := range s.Shards {
		protoShards[i] = &stateprotos.ShardTerm{
			Shard: uint32(shard.Shard),
			Term:  shard.Term,
		}
	}

	protoComplaints := make([]*stateprotos.Complaint, len(s.Complaints))
	for i, c := range s.Complaints {
		protoComplaints[i] = &stateprotos.Complaint{
			ConfigSeq: uint64(c.ConfigSeq),
			Shard:     uint32(c.Shard),
			Term:      c.Term,
			Signer:    uint32(c.Signer),
			Signature: c.Signature,
			Reason:    c.Reason,
		}
	}

	protoPending := make([]*stateprotos.BatchAttestationFragment, len(s.Pending))
	for i, baf := range s.Pending {
		simpleBAF, ok := baf.(*types.SimpleBatchAttestationFragment)
		if !ok {
			panic("unexpected type for BatchAttestationFragment")
		}
		protoPending[i] = simpleBAF.ToProto()
	}

	protoState := &stateprotos.State{
		NumberOfParties: uint32(s.N),
		Shards:          protoShards,
		Pending:         protoPending,
		Complaints:      protoComplaints,
		AppContext:      s.AppContext,
	}

	buff, err := proto.MarshalOptions{Deterministic: true}.Marshal(protoState)
	if err != nil {
		panic(err)
	}

	return buff
}

func (s *State) Deserialize(rawBytes []byte) error {
	var ps stateprotos.State
	if err := proto.Unmarshal(rawBytes, &ps); err != nil {
		return err
	}

	if ps.NumberOfParties > math.MaxUint16 {
		return fmt.Errorf("the NumberOfParties value %d exceeds uint16 maximum %d", ps.NumberOfParties, math.MaxUint16)
	}

	s.N = uint16(ps.NumberOfParties)
	if s.N == 0 {
		s.Threshold = 0
		s.Quorum = 0
	} else {
		_, s.Threshold, s.Quorum = utils.ComputeFTQ(s.N)
	}

	s.Shards = nil
	s.Pending = nil
	s.Complaints = nil

	// Load shards
	if len(ps.Shards) > 0 {
		s.Shards = make([]ShardTerm, len(ps.Shards))
		for i, protoShard := range ps.Shards {
			if protoShard.Shard > math.MaxUint16 {
				return fmt.Errorf("the Shard value %d at index %d exceeds uint16 maximum %d", protoShard.Shard, i, math.MaxUint16)
			}
			s.Shards[i] = ShardTerm{
				Shard: types.ShardID(protoShard.Shard),
				Term:  protoShard.Term,
			}
		}
	}

	// Load pending
	if len(ps.Pending) > 0 {
		s.Pending = make([]types.BatchAttestationFragment, 0, len(ps.Pending))
		for _, bafProto := range ps.Pending {
			baf := &types.SimpleBatchAttestationFragment{}
			if err := baf.FromProto(bafProto); err != nil {
				return fmt.Errorf("failed loading batch attestation fragment: %v", err)
			}
			s.Pending = append(s.Pending, baf)
		}
	}

	// Load complaints
	if len(ps.Complaints) > 0 {
		s.Complaints = make([]Complaint, len(ps.Complaints))
		for i, protoComplaint := range ps.Complaints {
			if err := s.Complaints[i].fromProto(protoComplaint); err != nil {
				return fmt.Errorf("failed loading complaint at index %d: %v", i, err)
			}
		}
	}

	// Load app context - ensure it's never nil, always []byte{} at minimum
	s.AppContext = []byte{}
	if ps.AppContext != nil {
		s.AppContext = ps.AppContext
	}

	return nil
}

type ShardTerm struct {
	Shard types.ShardID
	Term  uint64
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
			ConfigSeq: ce.Complaint.ConfigSeq,
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

func (ce *ControlEvent) toProto() *stateprotos.ControlEvent {
	protoEvent := &stateprotos.ControlEvent{}

	switch {
	case ce.BAF != nil:
		bafProto, ok := ce.BAF.(*types.SimpleBatchAttestationFragment)
		if !ok {
			panic("unexpected type for BAF")
		}
		protoEvent.Event = &stateprotos.ControlEvent_Baf{
			Baf: bafProto.ToProto(),
		}
	case ce.Complaint != nil:
		protoEvent.Event = &stateprotos.ControlEvent_Complaint{
			Complaint: ce.Complaint.toProto(),
		}
	case ce.ConfigRequest != nil:
		protoEvent.Event = &stateprotos.ControlEvent_ConfigRequest{
			ConfigRequest: ce.ConfigRequest.toProto(),
		}
	default:
		panic("empty control event")
	}

	return protoEvent
}

func (ce *ControlEvent) fromProto(pe *stateprotos.ControlEvent) error {
	ce.BAF = nil
	ce.Complaint = nil
	ce.ConfigRequest = nil

	switch event := pe.Event.(type) {
	case *stateprotos.ControlEvent_Baf:
		if event.Baf == nil {
			return fmt.Errorf("BAF event payload is nil")
		}
		baf := &types.SimpleBatchAttestationFragment{}
		if err := baf.FromProto(event.Baf); err != nil {
			return err
		}
		ce.BAF = baf
	case *stateprotos.ControlEvent_Complaint:
		if event.Complaint == nil {
			return fmt.Errorf("Complaint event payload is nil")
		}
		ce.Complaint = &Complaint{}
		if err := ce.Complaint.fromProto(event.Complaint); err != nil {
			return err
		}
	case *stateprotos.ControlEvent_ConfigRequest:
		if event.ConfigRequest == nil {
			return fmt.Errorf("ConfigRequest event payload is nil")
		}
		ce.ConfigRequest = &ConfigRequest{}
		if err := ce.ConfigRequest.fromProto(event.ConfigRequest); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown control event type")
	}

	return nil
}

func (ce *ControlEvent) Bytes() []byte {
	protoEvent := ce.toProto()
	bytes, err := proto.Marshal(protoEvent)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal control event: %v", err))
	}
	return bytes
}

func (ce *ControlEvent) FromBytes(bytes []byte) error {
	protoEvent := &stateprotos.ControlEvent{}
	if err := proto.Unmarshal(bytes, protoEvent); err != nil {
		return fmt.Errorf("failed to unmarshal control event: %v", err)
	}
	return ce.fromProto(protoEvent)
}

func (s *State) Process(l *flogging.FabricLogger, configSeq types.ConfigSequence, ces ...ControlEvent) (*State, []types.BatchAttestationFragment, []*ConfigRequest) {
	nextState := s.Clone()

	filteredCEs := filterCEsWithDiffConfigSeq(configSeq, l, ces...)

	for _, rule := range Rules {
		rule(nextState, configSeq, l, filteredCEs...)
	}

	// After applying rules, extract all batch attestations for which enough fragments have been collected.
	extracted := ExtractBatchAttestationsFromPending(nextState, l)
	configRequests := ExtractConfigRequests(filteredCEs)
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

func CleanupOldComplaints(s *State, configSeq types.ConfigSequence, l *flogging.FabricLogger, _ ...ControlEvent) {
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

func PrimaryRotateDueToComplaints(s *State, configSeq types.ConfigSequence, l *flogging.FabricLogger, _ ...ControlEvent) {
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

			s.Shards[shardIndex].Term++
			newTerm := s.Shards[shardIndex].Term

			l.Infof("Shard %d advanced from term %d to term %d due to %d complaints (threshold is %d)",
				complaint.Shard, oldTerm, newTerm, complaintNum, s.Threshold)
		} else {
			newComplaints = append(newComplaints, complaint)
		}
	}

	s.Complaints = newComplaints
}

func CollectAndDeduplicateEvents(s *State, configSeq types.ConfigSequence, l *flogging.FabricLogger, ces ...ControlEvent) {
	// shardsAndSequences dedups incoming BAFs against ones already in Pending. It is seeded from
	// Pending and, unlike the complaints set below, is not updated as BAFs are appended in this
	// loop — so two identical BAFs arriving in the same round would both be kept here. That case
	// is prevented upstream: ControlEvent.ID() hashes the full <seq, configSeq, signer, primary,
	// shard, digest> tuple, and SmartBFT's request pool rejects a duplicate RequestInfo before it
	// ever reaches a proposal, so identical BAFs cannot co-occur in one round's events.
	shardsAndSequences := make(map[attestationKey]struct{}, len(s.Pending))
	complaints := make(map[ShardTerm]map[types.PartyID]struct{})

	for _, baf := range s.Pending {
		shardsAndSequences[attestationKeyOf(baf)] = struct{}{}
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

			if _, exists := shardsAndSequences[attestationKeyOf(ce.BAF)]; exists {
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

func filterCEsWithDiffConfigSeq(configSeq types.ConfigSequence, l *flogging.FabricLogger, ces ...ControlEvent) []ControlEvent {
	filteredEvents := make([]ControlEvent, 0)
	for _, ce := range ces {
		if ce.BAF != nil {
			if ce.BAF.ConfigSequence() == configSeq {
				filteredEvents = append(filteredEvents, ce)
			} else {
				l.Debugf("filtering ce baf with mismatch config seq (currently %d); %s", configSeq, ce.BAF.String())
			}
		}
		if ce.Complaint != nil {
			if ce.Complaint.ConfigSeq == configSeq {
				filteredEvents = append(filteredEvents, ce)
			} else {
				l.Debugf("filtering ce complaint with mismatch config seq (currently %d); %s", configSeq, ce.Complaint.String())
			}
		}
		if ce.ConfigRequest != nil {
			reqConfigSeq, err := ce.ConfigRequest.ConfigSequence()
			if err != nil {
				l.Errorf("failed to get config seq from config request: %s", err)
				continue
			}
			if reqConfigSeq == configSeq+1 {
				filteredEvents = append(filteredEvents, ce)
			} else {
				l.Debugf("filtering ce config request with mismatch config seq (currently %d); %s; (should be +1)", configSeq, ce.ConfigRequest.String())
			}
		}
	}
	return filteredEvents
}

func FilterPendingEventsWithDiffConfigSeq(s *State, configSeq types.ConfigSequence, l *flogging.FabricLogger, ces ...ControlEvent) {
	filteredPending := make([]types.BatchAttestationFragment, 0)
	for _, baf := range s.Pending {
		if baf.ConfigSequence() == configSeq {
			filteredPending = append(filteredPending, baf)
		} else {
			l.Debugf("filtering pending baf with mismatch config seq (currently %d); %s", configSeq, baf.String())
		}
	}
	s.Pending = filteredPending

	filteredComplaints := make([]Complaint, 0)
	for _, complaint := range s.Complaints {
		if complaint.ConfigSeq == configSeq {
			filteredComplaints = append(filteredComplaints, complaint)
		} else {
			l.Debugf("filtering complaint with mismatch config seq (currently %d); %s", configSeq, complaint.String())
		}
	}
	s.Complaints = filteredComplaints
}

func DetectEquivocation(s *State, _ types.ConfigSequence, l *flogging.FabricLogger, _ ...ControlEvent) {
	// Since the primary signs the BAF, if we see multiple different digests
	// for the same <seq, shard, primary> tuple, the primary has equivocated.
	// In this case, we rotate the primary by incrementing the term.

	// Note: This rule applies only on pending BAFs (and incoming BAFs for this round).
	// It does not include BAFs that reached the threshold of f+1 in previous rounds
	// and their digest was added to the BatchAttestationDB.

	// Only count BAFs where Signer != Primary: a BAF where the signer claims to be
	// the primary (Signer == Primary) is self-signed and can be forged by any node to
	// manufacture false equivocation evidence.  Genuine attestations from secondaries
	// always have Signer != Primary (the primary's own participation is captured by the
	// empty PrimarySignature field, not by it re-broadcasting its own BAF as a voter).
	m := batchAttestationVotesByDigests(s, isNotSelfSigned)

	// Sort batches for deterministic processing: by shard, then primary, then seq.
	batches := make([]batchKey, 0, len(m))
	for batch := range m {
		batches = append(batches, batch)
	}
	slices.SortFunc(batches, func(a, b batchKey) int {
		if c := cmp.Compare(a.shard, b.shard); c != 0 {
			return c
		}
		if c := cmp.Compare(a.primary, b.primary); c != 0 {
			return c
		}
		return cmp.Compare(a.seq, b.seq)
	})

	// Track which shards have already been rotated to ensure at most one rotation per shard.
	rotatedShards := make(map[types.ShardID]struct{})

	// For each <seq, shard, primary> check if it has multiple different digests
	for _, batch := range batches {
		digest2signers := m[batch]
		// If there are multiple different digests for the same <seq, shard, primary>,
		// the primary has equivocated
		if len(digest2signers) > 1 {
			l.Warnf("Detected equivocation: batch attestation sequence %d in shard %d from primary %d "+
				"has %d different digests (%v). Primary has sent conflicting batches.",
				batch.seq, batch.shard, batch.primary,
				len(digest2signers), getDigestSummary(digest2signers))

			// Rotate the primary in the affected shard at most once per DetectEquivocation call.
			if _, alreadyRotated := rotatedShards[batch.shard]; alreadyRotated {
				l.Warnf("Skipping additional rotation for shard %d (already rotated once this round)", batch.shard)
				continue
			}

			for i := range s.Shards {
				if s.Shards[i].Shard == batch.shard {
					l.Warnf("Rotating primary %d (term %d -> %d) in shard %d due to equivocation at sequence %d",
						batch.primary, s.Shards[i].Term, s.Shards[i].Term+1, s.Shards[i].Shard, batch.seq)
					s.Shards[i].Term++
					rotatedShards[batch.shard] = struct{}{}
					break
				}
			}
		}
	}
}

// getDigestSummary returns a summary of digests for logging purposes.
// digest2signers is keyed by hex-encoded digest strings.
func getDigestSummary(digest2signers map[string][]types.PartyID) string {
	hexDigests := make([]string, 0, len(digest2signers))
	for hexDigest := range digest2signers {
		hexDigests = append(hexDigests, hexDigest)
	}
	slices.Sort(hexDigests)

	var summary strings.Builder
	summary.WriteString("[")
	for i, hexDigest := range hexDigests {
		if i > 0 {
			summary.WriteString(", ")
		}
		fmt.Fprintf(&summary, "digest=%s (signers=%d)", hexDigest, len(digest2signers[hexDigest]))
	}
	summary.WriteString("]")
	return summary.String()
}

// batchAttestationVotesByDigests groups pending BAFs as <batchKey> --> { digest --> signers },
// including only fragments for which include(baf) is true. Digests are hex-encoded so they are
// map-comparable.
func batchAttestationVotesByDigests(s *State, include func(baf types.BatchAttestationFragment) bool) map[batchKey]map[string][]types.PartyID {
	m := make(map[batchKey]map[string][]types.PartyID)

	for _, baf := range s.Pending {
		if !include(baf) {
			continue
		}

		batch := batchKeyOf(baf)
		if m[batch] == nil {
			m[batch] = make(map[string][]types.PartyID)
		}

		hexDigest := hex.EncodeToString(baf.Digest())
		m[batch][hexDigest] = append(m[batch][hexDigest], baf.Signer())
	}
	return m
}

// allBAFs includes every pending BAF (used when tallying signatures toward the threshold).
func allBAFs(_ types.BatchAttestationFragment) bool { return true }

// isNotSelfSigned excludes BAFs where Signer() == Primary(). Such self-signed BAFs cannot serve
// as secondary attestations; excluding them prevents a non-primary from manufacturing
// equivocation evidence by sending two BAFs with Primary() == Signer() but different digests.
func isNotSelfSigned(baf types.BatchAttestationFragment) bool { return baf.Signer() != baf.Primary() }

func ExtractBatchAttestationsFromPending(s *State, l *flogging.FabricLogger) []types.BatchAttestationFragment {
	m := batchAttestationVotesByDigests(s, allBAFs)

	// decidedBatches holds the <seq, shard, primary> batches for which at least one digest
	// reached threshold. It governs which pending BAFs are removed: once a batch is decided,
	// all of its pending fragments are cleared (including stale minority/equivocating digests),
	// exactly as before.
	decidedBatches := make(map[batchKey]struct{})

	// thresholdDigests holds the specific <seq, shard, primary, digest> that individually
	// reached threshold. Only fragments of these digests are extracted as attestations, so an
	// under-attested (equivocating) digest can never back a committed block. If more than one
	// digest for the same batch reaches threshold (byzantine primary), each is extracted and
	// downstream becomes its own block.
	thresholdDigests := make(map[thresholdDigestKey]struct{})

	for batch, digest2signers := range m {
		l.Debugf("A total of %d digests were found for seq %d in shard %d with primary %d", len(digest2signers), batch.seq, batch.shard, batch.primary)

		for digest, signers := range digest2signers {
			if len(signers) >= int(s.Threshold) {
				l.Debugf("Found threshold (%d >= %d) of batch attestation fragments for shard %d, seq %d, digest %s", len(signers), s.Threshold, batch.shard, batch.seq, digest)
				decidedBatches[batch] = struct{}{}
				thresholdDigests[thresholdDigestKey{batch: batch, digest: digest}] = struct{}{}
			}
		}

		if _, ok := decidedBatches[batch]; !ok {
			l.Debugf("Could not find a threshold of batch attestation fragments for shard %d, seq %d", batch.shard, batch.seq)
		}
	} // for all <seq, shard, primary>

	var extracted []types.BatchAttestationFragment

	newPending := make([]types.BatchAttestationFragment, 0, len(s.Pending))

	// We iterate over the pending because we need deterministic processing
	for _, baf := range s.Pending {
		batch := batchKeyOf(baf)

		if _, decided := decidedBatches[batch]; !decided {
			// No digest for this batch reached threshold yet; keep it pending.
			newPending = append(newPending, baf)
			continue
		}

		// The batch is decided and thus removed from Pending. Only emit fragments whose digest
		// actually reached threshold; minority/equivocating digests are dropped, not extracted.
		if _, ok := thresholdDigests[thresholdDigestKey{batch: batch, digest: hex.EncodeToString(baf.Digest())}]; ok {
			extracted = append(extracted, baf)
		}
	}

	oldPendingCount := len(s.Pending)
	newPendingCount := len(newPending)

	l.Debugf("Pending attestations count changed from %d to %d", oldPendingCount, newPendingCount)
	s.Pending = newPending

	// TODO explicit digest selection: consider adding logic to explicitly choose the digest with the most signatures when equivocation is detected

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
