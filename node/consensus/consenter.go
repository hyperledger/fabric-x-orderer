/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
)

type TotalOrder interface {
	SubmitRequest(req []byte) error
}

//go:generate counterfeiter -o mocks/batch_attestation_db.go . BatchAttestationDB
type BatchAttestationDB interface {
	Exists(digest []byte) bool
	Put(digest [][]byte, epoch []uint64) // TODO remove epochs from BADB
	Clean(epoch uint64)                  // TODO remove clean by epoch
}

type Consenter struct {
	Logger *flogging.FabricLogger
	DB     BatchAttestationDB
}

func (c *Consenter) SimulateStateTransition(prevState *state.State, configSeq types.ConfigSequence, requests [][]byte) (*state.State, [][]types.BatchAttestationFragment, []*state.ConfigRequest) {
	controlEvents, err := requestsToControlEvents(requests)
	if err != nil {
		panic(err)
	}

	filteredControlEvents := make([]state.ControlEvent, 0, len(controlEvents))

	// Iterate over all control events and prune those that already exist in our DB
	for _, ce := range controlEvents {
		if ce.BAF != nil {
			if c.DB.Exists(ce.BAF.Digest()) {
				c.Logger.Debugf("Batch attestation for digest %x already exists", ce.BAF.Digest())
				continue
			}
		}
		filteredControlEvents = append(filteredControlEvents, ce)
	}

	newState, fragments, configRequests := prevState.Process(c.Logger, configSeq, filteredControlEvents...)
	batchAttestations := aggregateFragments(fragments)

	return newState, batchAttestations, configRequests
}

// Index indexes BAs (digests).
// Note that this must hold: Index(digests) with the same digests is idempotent.
// TODO revise the recovery from failure or shutdown, specifically the order of Index and Append.
func (c *Consenter) Index(digests [][]byte) {
	if len(digests) > 0 {
		epochs := make([]uint64, len(digests)) // TODO remove
		c.DB.Put(digests, epochs)
	}
}

// bafGroupKey is the comparable form of types.BatchID (<shard, primary, seq, digest>): the
// tuple that identifies a batch. digest is a string because BatchID.Digest() ([]byte) is not
// map-key-comparable. Fragments are grouped by batch identity so that each block corresponds
// to exactly one batch. In the honest case there is one batch per <seq, shard> (unchanged
// behavior); distinct digests (an equivocating primary) or distinct primaries (e.g. the same
// batch re-produced across a rotation) are different BatchIDs and therefore different blocks.
// This guarantees every group is a single batch, so consumers that read only the first
// fragment (ba[0]) always see a threshold-attested batch rather than a stale/minority digest
// that happened to be first in Pending order.
type bafGroupKey struct {
	seq     types.BatchSequence
	shard   types.ShardID
	primary types.PartyID
	digest  string
}

func bafKey(baf types.BatchAttestationFragment) bafGroupKey {
	return bafGroupKey{seq: baf.Seq(), shard: baf.Shard(), primary: baf.Primary(), digest: string(baf.Digest())}
}

func aggregateFragments(batchAttestationFragments []types.BatchAttestationFragment) [][]types.BatchAttestationFragment {
	index := indexBAFs(batchAttestationFragments)

	var attestations [][]types.BatchAttestationFragment

	added := make(map[bafGroupKey]struct{})

	for _, baf := range batchAttestationFragments {
		key := bafKey(baf)

		if _, added := added[key]; added {
			continue
		}

		added[key] = struct{}{}

		fragments := index[key]
		attestations = append(attestations, fragments)
	}

	return attestations
}

func indexBAFs(batchAttestationFragments []types.BatchAttestationFragment) map[bafGroupKey][]types.BatchAttestationFragment {
	index := make(map[bafGroupKey][]types.BatchAttestationFragment)

	for _, baf := range batchAttestationFragments {
		key := bafKey(baf)
		index[key] = append(index[key], baf)
	}
	return index
}

func requestsToControlEvents(requests [][]byte) ([]state.ControlEvent, error) {
	events := make([]state.ControlEvent, 0, len(requests))
	for i := 0; i < len(requests); i++ {
		ce := state.ControlEvent{}
		if err := ce.FromBytes(requests[i]); err != nil {
			return nil, err
		}
		events = append(events, ce)
	}

	return events, nil
}
