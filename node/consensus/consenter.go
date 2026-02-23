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
	Logger          *flogging.FabricLogger
	DB              BatchAttestationDB
	BAFDeserializer state.BAFDeserializer
}

func (c *Consenter) SimulateStateTransition(prevState *state.State, configSeq types.ConfigSequence, requests [][]byte) (*state.State, [][]types.BatchAttestationFragment, []*state.ConfigRequest) {
	controlEvents, err := requestsToControlEvents(requests, c.BAFDeserializer.Deserialize)
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

func aggregateFragments(batchAttestationFragments []types.BatchAttestationFragment) [][]types.BatchAttestationFragment {
	index := indexBAFs(batchAttestationFragments)

	var attestations [][]types.BatchAttestationFragment

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

func indexBAFs(batchAttestationFragments []types.BatchAttestationFragment) map[struct {
	seq   types.BatchSequence
	shard types.ShardID
}][]types.BatchAttestationFragment {
	index := make(map[struct {
		seq   types.BatchSequence
		shard types.ShardID
	}][]types.BatchAttestationFragment)

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

func requestsToControlEvents(requests [][]byte, fragmentFromBytes func([]byte) (types.BatchAttestationFragment, error)) ([]state.ControlEvent, error) {
	events := make([]state.ControlEvent, 0, len(requests))
	for i := 0; i < len(requests); i++ {
		ce := state.ControlEvent{}
		if err := ce.FromBytes(requests[i], fragmentFromBytes); err != nil {
			return nil, err
		}
		events = append(events, ce)
	}

	return events, nil
}
