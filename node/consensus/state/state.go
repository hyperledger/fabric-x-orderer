/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"cmp"
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

// batchKey identifies a batch by <shard, primary, seq>: the batch-identity portion of
// types.BatchID with the digest excluded. It is the unit of equivocation detection and
// extraction — a byzantine primary produces several digests under a single batchKey.
type batchKey struct {
	shard   types.ShardID
	primary types.PartyID
	seq     types.BatchSequence
}

// compare orders batchKeys by shard, then primary, then seq, returning a negative, zero, or
// positive value in the manner of cmp.Compare. It gives DetectEquivocation a deterministic
// processing order (and is trivially unit-testable in isolation).
func (b batchKey) compare(other batchKey) int {
	if c := cmp.Compare(b.shard, other.shard); c != 0 {
		return c
	}
	if c := cmp.Compare(b.primary, other.primary); c != 0 {
		return c
	}
	return cmp.Compare(b.seq, other.seq)
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

func (s *State) Process(l *flogging.FabricLogger, configSeq types.ConfigSequence, ces ...ControlEvent) (*State, []types.BatchAttestationFragment, []*ConfigRequest) {
	nextState := s.Clone()

	filteredCEs := filterCEsWithDiffConfigSeq(configSeq, l, ces...)

	nextState.FilterPendingEventsWithDiffConfigSeq(configSeq, l)
	nextState.CollectAndDeduplicateEvents(l, filteredCEs...)
	nextState.DetectEquivocation(l)
	nextState.PrimaryRotateDueToComplaints(l)
	nextState.CleanupOldComplaints(l)

	// After applying the rules above, extract all batch attestations for which enough fragments have been collected.
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

func (s *State) CleanupOldComplaints(l *flogging.FabricLogger) {
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

func (s *State) PrimaryRotateDueToComplaints(l *flogging.FabricLogger) {
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

// CollectAndDeduplicateEvents ingests the BAFs and complaints carried by this round's ControlEvents
// into the State, skipping ones that are duplicates or that reference an unknown shard.
// Config-sequence filtering already happened before this step (FilterPendingEventsWithDiffConfigSeq
// / filterCEsWithDiffConfigSeq), so it is not repeated here.
//
// Inputs:
//   - s: the current consensus State. Its Pending (BAFs) and Complaints slices are the accumulators
//     this function grows, and its Shards list is consulted to validate that each event's shard exists.
//   - ces: the new ControlEvents to fold in. Only the BAF and Complaint variants are handled here;
//     any other event kind (e.g. ConfigRequest) and empty events are ignored.
//
// Mutations (the only fields it changes):
//   - s.Pending: each accepted BAF is appended.
//   - s.Complaints: each accepted complaint is appended.
//
// A BAF is skipped when its shard is not in s.Shards, or when an equal attestationKey
// (<shard, primary, seq, signer, digest>) is already present in s.Pending — i.e. the same signer has
// already attested that exact (batch, digest). Note two BAFs from one signer with *different* digests
// for the same batch are NOT duplicates: they are equivocation evidence and both are kept.
// A complaint is skipped when its ShardTerm is not in s.Shards, or when the same signer has already
// complained about that ShardTerm. The dedup sets are seeded from the existing s.Pending / s.Complaints
// and, for complaints, updated as events are accepted within this call.
func (s *State) CollectAndDeduplicateEvents(l *flogging.FabricLogger, ces ...ControlEvent) {
	// shardsAndSequences dedups incoming BAFs against ones already in Pending. It is seeded from
	// Pending and, unlike the complaints set below, is not updated as BAFs are appended in this
	// loop — so two identical BAFs arriving in the same round would both be kept here. That case
	// is prevented upstream: ControlEvent.ID() hashes the full <seq, configSeq, signer, primary,
	// shard, digest> tuple, and SmartBFT's request pool rejects a duplicate RequestInfo before it
	// ever reaches a proposal, so identical BAFs cannot co-occur in one round's events.
	shardsAndSequences := make(map[attestationKey]struct{}, len(s.Pending))
	for _, baf := range s.Pending {
		shardsAndSequences[attestationKeyOf(baf)] = struct{}{}
	}

	complaints := make(map[ShardTerm]map[types.PartyID]struct{})
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

func (s *State) FilterPendingEventsWithDiffConfigSeq(configSeq types.ConfigSequence, l *flogging.FabricLogger) {
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

// DetectEquivocation catches a byzantine primary which signed two or more conflicting batches for
// the same <shard, primary, seq>, and punishes it by rotating the shard to the next term. A primary
// signs every BAF it produces, so two distinct digests carrying its signature under one batchKey are
// proof it equivocated.
//
// It runs purely over s.Pending, which CollectAndDeduplicateEvents (the preceding step) has already
// populated with this round's incoming BAFs. It therefore only sees BAFs still pending — not ones
// from prior rounds that already reached the f+1 threshold and were committed to the
// BatchAttestationDB.
//
// Inputs it reads:
//   - s.Pending: the BAFs to inspect, grouped by batchKey and digest. Only BAFs with Signer != Primary
//     count (self-signed BAFs are excluded, so a non-primary cannot forge equivocation evidence by
//     broadcasting two self-signed BAFs with different digests).
//   - s.Shards: scanned to find the shard whose term must be bumped.
//
// Mutation (the only thing it changes):
//   - s.Shards[i].Term: incremented by one for each shard found to have an equivocating batch. To keep
//     the rotation bounded and deterministic, at most one rotation happens per shard per call, and
//     batches are processed in a fixed order (shard, then primary, then seq). No BAFs are removed here.
func (s *State) DetectEquivocation(l *flogging.FabricLogger) {
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
	m := batchAttestationVotesByDigests(s, true)

	// Sort batchKeys for deterministic processing: by shard, then primary, then seq.
	batchKeys := make([]batchKey, 0, len(m))
	for bk := range m {
		batchKeys = append(batchKeys, bk)
	}
	slices.SortFunc(batchKeys, batchKey.compare)

	// Track which shards have already been rotated to ensure at most one rotation per shard.
	rotatedShards := make(map[types.ShardID]struct{})

	// For each <seq, shard, primary> check if it has multiple different digests
	for _, bk := range batchKeys {
		digest2signers := m[bk]
		// If there are multiple different digests for the same <seq, shard, primary>,
		// the primary has equivocated
		if len(digest2signers) > 1 {
			l.Warnf("Detected equivocation: batch attestation sequence %d in shard %d from primary %d "+
				"has %d different digests (%v). Primary has sent conflicting batches.",
				bk.seq, bk.shard, bk.primary,
				len(digest2signers), getDigestSummary(digest2signers))

			// Rotate the primary in the affected shard at most once per DetectEquivocation call.
			if _, alreadyRotated := rotatedShards[bk.shard]; alreadyRotated {
				l.Warnf("Skipping additional rotation for shard %d (already rotated once this round)", bk.shard)
				continue
			}

			for i := range s.Shards {
				if s.Shards[i].Shard == bk.shard {
					l.Warnf("Rotating primary %d (term %d -> %d) in shard %d due to equivocation at sequence %d",
						bk.primary, s.Shards[i].Term, s.Shards[i].Term+1, s.Shards[i].Shard, bk.seq)
					s.Shards[i].Term++
					rotatedShards[bk.shard] = struct{}{}
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

// batchAttestationVotesByDigests groups pending BAFs as <batchKey> --> { digest --> signers }.
// Digests are hex-encoded so they are map-comparable.
//
// When excludeSelfSigned is true, BAFs where Signer() == Primary() are skipped. Such self-signed
// BAFs cannot serve as secondary attestations; excluding them prevents a non-primary from
// manufacturing equivocation evidence by sending two BAFs with Primary() == Signer() but different
// digests. Equivocation detection passes true; threshold tallying passes false to count every BAF.
func batchAttestationVotesByDigests(s *State, excludeSelfSigned bool) map[batchKey]map[string][]types.PartyID {
	m := make(map[batchKey]map[string][]types.PartyID)

	for _, baf := range s.Pending {
		if excludeSelfSigned && baf.Signer() == baf.Primary() {
			continue
		}

		bk := batchKeyOf(baf)
		if m[bk] == nil {
			m[bk] = make(map[string][]types.PartyID)
		}

		hexDigest := hex.EncodeToString(baf.Digest())
		m[bk][hexDigest] = append(m[bk][hexDigest], baf.Signer())
	}
	return m
}

func ExtractBatchAttestationsFromPending(s *State, l *flogging.FabricLogger) []types.BatchAttestationFragment {
	m := batchAttestationVotesByDigests(s, false)

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

	for bk, digest2signers := range m {
		l.Debugf("A total of %d digests were found for seq %d in shard %d with primary %d", len(digest2signers), bk.seq, bk.shard, bk.primary)

		for digest, signers := range digest2signers {
			if len(signers) >= int(s.Threshold) {
				l.Debugf("Found threshold (%d >= %d) of batch attestation fragments for shard %d, seq %d, digest %s", len(signers), s.Threshold, bk.shard, bk.seq, digest)
				decidedBatches[bk] = struct{}{}
				thresholdDigests[thresholdDigestKey{batch: bk, digest: digest}] = struct{}{}
			}
		}

		if _, ok := decidedBatches[bk]; !ok {
			l.Debugf("Could not find a threshold of batch attestation fragments for shard %d, seq %d", bk.shard, bk.seq)
		}
	} // for all <seq, shard, primary>

	var extracted []types.BatchAttestationFragment

	newPending := make([]types.BatchAttestationFragment, 0, len(s.Pending))

	// We iterate over the pending because we need deterministic processing
	for _, baf := range s.Pending {
		bk := batchKeyOf(baf)

		if _, decided := decidedBatches[bk]; !decided {
			// No digest for this batch reached threshold yet; keep it pending.
			newPending = append(newPending, baf)
			continue
		}

		// The batch is decided and thus removed from Pending. Only emit fragments whose digest
		// actually reached threshold; minority/equivocating digests are dropped, not extracted.
		if _, ok := thresholdDigests[thresholdDigestKey{batch: bk, digest: hex.EncodeToString(baf.Digest())}]; ok {
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
