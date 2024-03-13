package node

import (
	"arma"
	"bytes"
	"encoding/asn1"
	"encoding/binary"
	"github.com/SmartBFT-Go/consensus/v2/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/v2/pkg/types"
)

type Storage interface {
	Append([]byte)
}

type Proposer interface {
	Propose([]byte, func([]byte) arma.BatchAttestationFragment, ...arma.ControlEvent) []byte
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

type Consensus struct {
	Signer       Signer
	Proposer     Proposer
	ArmaState    []byte
	CurrentNodes []uint64
	consensus.Consensus
	Storage      Storage
	commitEvents armaCommit
}

type block struct {
	Attestations []arma.SimpleBatchAttestation
	State        []byte
	Decision     []byte
}

type toBeSignedBlockHeader struct {
	Sequence int64
	Digest   []byte
	PrevHash []byte
}

func (tbsbh toBeSignedBlockHeader) Bytes() []byte {
	buff := make([]byte, 8+64)
	binary.BigEndian.PutUint64(buff, uint64(tbsbh.Sequence))
	copy(buff[8:], tbsbh.Digest)
	copy(buff[40:], tbsbh.PrevHash)
	return buff
}

func (b *block) Bytes() []byte {
	rawAttestations := bytes.Buffer{}
	for _, att := range b.Attestations {
		hdr := make([]byte, 4)
		data := att.Serialize()
		binary.BigEndian.PutUint32(hdr, uint32(len(data)))
		rawAttestations.Write(hdr)
		rawAttestations.Write(data)
	}

	buff := bytes.Buffer{}

	attestationsLen := make([]byte, 4)
	binary.BigEndian.PutUint32(attestationsLen, uint32(rawAttestations.Len()))

	stateLen := make([]byte, 4)
	binary.BigEndian.PutUint32(stateLen, uint32(len(b.State)))

	buff.Write(attestationsLen)
	buff.Write(rawAttestations.Bytes())
	buff.Write(stateLen)
	buff.Write(b.State)
	buff.Write(b.Decision)

	return buff.Bytes()
}

func controlEventsFromData(data []byte) []arma.ControlEvent {
	var controlEventCount int
	scanControlEvents(data, func(_ []byte) {
		controlEventCount++
	})

	controlEvents := make([]arma.ControlEvent, 0, controlEventCount)

	scanControlEvents(data, func(rawCE []byte) {
		var ce arma.ControlEvent
		if err := ce.FromBytes(rawCE, func([]byte) (arma.BatchAttestationFragment, error) {
			var sbaf arma.SimpleBatchAttestationFragment
			err := sbaf.Deserialize(rawCE)
			return &sbaf, err
		}); err != nil {
			panic(err)
		}

		controlEvents = append(controlEvents, ce)
	})

	return controlEvents
}

func scanControlEvents(data []byte, f func([]byte)) {
	var pos int
	var c int
	for pos < len(data) {
		size := binary.BigEndian.Uint32(data[pos:])
		pos += 4
		rawCE := data[pos : pos+int(size)]
		f(rawCE)
		pos += int(size)
		c++
	}
}

type Bytes [][]byte

func (c *Consensus) Sign(msg []byte) []byte {
	sig, err := c.Signer.Sign(msg)
	if err != nil {
		panic(err)
	}

	return sig
}

func (c *Consensus) SignProposal(proposal types.Proposal, _ []byte) *types.Signature {
	requests := arma.BatchFromRaw(proposal.Payload)

	ces, err := requestsToControlEvents(requests, BatchAttestationFromBytes)

	if err != nil {
		panic(err)
	}

	bafs := make([]arma.BatchAttestationFragment, 0, len(ces))
	for _, ce := range ces {
		if ce.BAF == nil {
			continue
		}
		bafs = append(bafs, ce.BAF)
	}

	batchAttestations := aggregateFragments(bafs)

	sigs := make(Bytes, 0, len(batchAttestations)+1)
	msgs := make(Bytes, 0, len(batchAttestations))

	for _, ba := range batchAttestations {
		var hdr toBeSignedBlockHeader
		hdr.Digest = ba.Digest()
		hdr.Sequence = int64(ba.Seq())
		// TODO: prev hash
		msg := hdr.Bytes()
		sig, err := c.Signer.Sign(msg)
		if err != nil {
			panic(err)
		}

		sigs = append(sigs, sig)
		msgs = append(msgs, msg)
	}

	proposalSig, err := c.Signer.Sign([]byte(proposal.Digest()))
	if err != nil {
		panic(err)
	}

	sigs = append(sigs, proposalSig)

	msgsRaw, err := asn1.Marshal(msgs)
	if err != nil {
		panic(err)
	}

	sigsRaw, err := asn1.Marshal(sigs)
	if err != nil {
		panic(err)
	}

	return &types.Signature{
		Msg:   msgsRaw,
		Value: sigsRaw,
		ID:    c.Config.SelfID,
	}

}

func (c *Consensus) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	ces, err := requestsToControlEvents(requests, BatchAttestationFromBytes)

	if err != nil {
		panic(err)
	}

	newRawState := c.Proposer.Propose(c.ArmaState, nil, ces...)
	return types.Proposal{
		Header:   metadata,
		Metadata: newRawState,
		Payload:  arma.BatchedRequests(requests).ToBytes(),
	}
}

func BatchAttestationFromBytes(in []byte) (arma.BatchAttestationFragment, error) {
	var baf arma.SimpleBatchAttestationFragment
	if err := baf.Deserialize(in); err != nil {
		return nil, err
	}

	return &baf, nil
}

func requestsToControlEvents(requests [][]byte, fragmentFromBytes func([]byte) (arma.BatchAttestationFragment, error)) ([]arma.ControlEvent, error) {
	events := make([]arma.ControlEvent, 0, len(requests))
	for i := 0; i < len(requests); i++ {
		ce := arma.ControlEvent{}
		if err := ce.FromBytes(requests[i], fragmentFromBytes); err != nil {
			return nil, err
		}
		events = append(events, ce)
	}

	return events, nil
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	rawDecision := decisionToBytes(proposal, signatures)
	controlEvents := controlEventsFromData(proposal.Payload)

	signal := make(chan struct{})

	c.commitEvents <- commitEvent{
		events: controlEvents,
		f: func(batchAttestationFragments []arma.BatchAttestationFragment, newState []byte) {
			defer close(signal)

			batchAttestations := aggregateFragments(batchAttestationFragments)
			b := block{
				Attestations: batchAttestations,
				State:        newState,
				Decision:     rawDecision,
			}
			c.Storage.Append(b.Bytes())
		},
	}

	// Wait for Arma to process the commit event
	<-signal

	return types.Reconfig{
		CurrentNodes:  c.CurrentNodes,
		CurrentConfig: c.Consensus.Config,
	}
}

func aggregateFragments(batchAttestationFragments []arma.BatchAttestationFragment) []arma.SimpleBatchAttestation {
	index := indexBAFs(batchAttestationFragments)

	var attestations []arma.SimpleBatchAttestation

	added := make(map[struct {
		seq   uint64
		shard uint16
	}]struct{})

	for _, baf := range batchAttestationFragments {
		key := struct {
			seq   uint64
			shard uint16
		}{seq: baf.Seq(), shard: baf.Shard()}

		if _, added := added[key]; added {
			continue
		}

		added[key] = struct{}{}

		fragments := index[key]

		var simpleFragments []arma.SimpleBatchAttestationFragment
		for _, fragment := range fragments {
			simpleFragments = append(simpleFragments, *fragment.(*arma.SimpleBatchAttestationFragment))
		}
		attestations = append(attestations, arma.SimpleBatchAttestation{
			F: simpleFragments,
		})
	}

	return attestations
}

func indexBAFs(batchAttestationFragments []arma.BatchAttestationFragment) map[struct {
	seq   uint64
	shard uint16
}][]arma.BatchAttestationFragment {
	index := make(map[struct {
		seq   uint64
		shard uint16
	}][]arma.BatchAttestationFragment)

	for _, baf := range batchAttestationFragments {
		key := struct {
			seq   uint64
			shard uint16
		}{seq: baf.Seq(), shard: baf.Shard()}
		fragments := index[key]
		fragments = append(fragments, baf)
		index[key] = fragments
	}
	return index
}

func decisionToBytes(proposal types.Proposal, signatures []types.Signature) []byte {
	sigBuff := bytes.Buffer{}

	sigBuff.Write([]byte{uint8(len(signatures))})
	for _, sig := range signatures {
		rawSig, err := asn1.Marshal(sig)
		if err != nil {
			panic(err)
		}
		sigBuff.Write(rawSig)
	}

	buff := make([]byte, 0, 4*3+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata)+sigBuff.Len())
	binary.BigEndian.PutUint32(buff, uint32(len(proposal.Header)))
	binary.BigEndian.PutUint32(buff[4:], uint32(len(proposal.Payload)))
	binary.BigEndian.PutUint32(buff[8:], uint32(len(proposal.Metadata)))
	copy(buff[12:], proposal.Header)
	copy(buff[12+len(proposal.Header):], proposal.Payload)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload):], proposal.Metadata)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata):], sigBuff.Bytes())

	return buff
}

type commitEvent struct {
	events []arma.ControlEvent
	f      func([]arma.BatchAttestationFragment, []byte)
}

type armaCommit chan commitEvent

func (ac armaCommit) Deliver() ([]arma.ControlEvent, func([]arma.BatchAttestationFragment, []byte)) {
	commit := <-ac
	return commit.events, commit.f
}
