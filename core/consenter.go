/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core

import (
	"slices"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
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
	Logger          types.Logger
	DB              BatchAttestationDB
	BAFDeserializer BAFDeserializer
	State           *State
}

func (c *Consenter) SimulateStateTransition(prevState *State, requests [][]byte) (*State, [][]BatchAttestationFragment) {
	controlEvents, err := requestsToControlEvents(requests, c.BAFDeserializer.Deserialize)
	if err != nil {
		panic(err)
	}

	filteredControlEvents := make([]ControlEvent, 0, len(controlEvents))

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

	newState, fragments := prevState.Process(c.Logger, filteredControlEvents...)
	batchAttestations := aggregateFragments(fragments)

	return newState, batchAttestations
}

// Commit indexes BAs and updates the state.
// Note that this must hold: Commit(controlEvents) with the same controlEvents is idempotent.
// TODO revise the recovery from failure or shutdown, specifically the order of Commit and Append.
func (c *Consenter) Commit(events [][]byte) {
	state, batchAttestations := c.SimulateStateTransition(c.State, events)
	if len(batchAttestations) > 0 {
		c.indexAttestationsInDB(batchAttestations)
	}
	c.State = state
}

func (c *Consenter) indexAttestationsInDB(batchAttestations [][]BatchAttestationFragment) {
	digests := make([][]byte, 0, len(batchAttestations))
	epochs := make([]uint64, 0, len(batchAttestations))
	for _, bafs := range batchAttestations {
		if len(bafs) == 0 {
			continue
		}
		digests = append(digests, bafs[0].Digest())
		epochs = append(epochs, epochOfBatchAttestations(bafs))

		c.Logger.Debugf("Indexed batch attestation for digest %x", bafs[0].Digest())
	}
	c.DB.Put(digests, epochs)
}

func aggregateFragments(batchAttestationFragments []BatchAttestationFragment) [][]BatchAttestationFragment {
	index := indexBAFs(batchAttestationFragments)

	var attestations [][]BatchAttestationFragment

	added := make(map[struct {
		seq   types.BatchSequence
		shard types.ShardID
	}]struct{})

	for _, baf := range batchAttestationFragments {
		key := struct {
			seq   types.BatchSequence
			shard types.ShardID
		}{seq: baf.Seq(), shard: baf.Shard()}

		if _, added := added[key]; added {
			continue
		}

		added[key] = struct{}{}

		fragments := index[key]
		attestations = append(attestations, fragments)
	}

	return attestations
}

func indexBAFs(batchAttestationFragments []BatchAttestationFragment) map[struct {
	seq   types.BatchSequence
	shard types.ShardID
}][]BatchAttestationFragment {
	index := make(map[struct {
		seq   types.BatchSequence
		shard types.ShardID
	}][]BatchAttestationFragment)

	for _, baf := range batchAttestationFragments {
		key := struct {
			seq   types.BatchSequence
			shard types.ShardID
		}{seq: baf.Seq(), shard: baf.Shard()}
		fragments := index[key]
		fragments = append(fragments, baf)
		index[key] = fragments
	}
	return index
}

func requestsToControlEvents(requests [][]byte, fragmentFromBytes func([]byte) (BatchAttestationFragment, error)) ([]ControlEvent, error) {
	events := make([]ControlEvent, 0, len(requests))
	for i := 0; i < len(requests); i++ {
		ce := ControlEvent{}
		if err := ce.FromBytes(requests[i], fragmentFromBytes); err != nil {
			return nil, err
		}
		events = append(events, ce)
	}

	return events, nil
}

func epochOfBatchAttestations(bafs []BatchAttestationFragment) uint64 {
	epochs := make([]uint64, len(bafs))
	for i := 0; i < len(bafs); i++ {
		epochs[i] = uint64(bafs[i].Epoch())
	}

	slices.Sort(epochs)

	return epochs[len(epochs)/2]
}
