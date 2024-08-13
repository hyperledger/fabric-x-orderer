package core

import (
	"slices"
)

type TotalOrder interface {
	SubmitRequest(req []byte) error
}

type BatchAttestationDB interface {
	Exists(digest []byte) bool
	Put(digest [][]byte, epoch []uint64)
	Clean(epoch uint64)
}

type Consenter struct {
	Logger          Logger
	TotalOrder      TotalOrder
	DB              BatchAttestationDB
	BAFDeserializer BAFDeserializer
	State           *State
}

func (c *Consenter) SimulateStateTransition(prevState *State, requests [][]byte) (*State, [][]BatchAttestationFragment) {
	controlEvents, err := requestsToControlEvents(requests, c.BAFDeserializer.Deserialize)
	if err != nil {
		panic(err)
	}

	newState, fragments := prevState.Process(c.Logger, controlEvents...)

	filteredFragments := make([]BatchAttestationFragment, 0, len(fragments))

	// Iterate over all fragments and prune those that already exist in our DB
	for _, baf := range fragments {
		if c.DB.Exists(baf.Digest()) {
			c.Logger.Debugf("Batch attestation for digest %x already exists", baf.Digest())
			continue
		}
		filteredFragments = append(filteredFragments, baf)
	}

	batchAttestations := aggregateFragments(filteredFragments)

	return newState, batchAttestations
}

// Commit indexes BAs and updates the state.
// Note that this must hold: Commit(controlEvents) with the same controlEvents is idempotent.
// TODO revise the recovery from failure or shutdown, specifically the order of Commit and Append.
func (c *Consenter) Commit(events [][]byte) {
	state, batchAttestations := c.SimulateStateTransition(c.State, events)
	c.indexAttestationsInDB(batchAttestations)
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

func (c *Consenter) Submit(rawControlEvent []byte) {
	if err := c.TotalOrder.SubmitRequest(rawControlEvent); err != nil {
		c.Logger.Warnf("Failed submitting request:", err)
		return
	}
}

func aggregateFragments(batchAttestationFragments []BatchAttestationFragment) [][]BatchAttestationFragment {
	index := indexBAFs(batchAttestationFragments)

	var attestations [][]BatchAttestationFragment

	added := make(map[struct {
		seq   BatchSequence
		shard ShardID
	}]struct{})

	for _, baf := range batchAttestationFragments {
		key := struct {
			seq   BatchSequence
			shard ShardID
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
	seq   BatchSequence
	shard ShardID
}][]BatchAttestationFragment {
	index := make(map[struct {
		seq   BatchSequence
		shard ShardID
	}][]BatchAttestationFragment)

	for _, baf := range batchAttestationFragments {
		key := struct {
			seq   BatchSequence
			shard ShardID
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
