package core

import (
	"sync"
	"time"
)

type SeqAcker interface {
	Stop()
	HandleAck(seq BatchSequence, from PartyID)
	WaitForSecondaries(seq BatchSequence)
}

type Acker struct {
	logger             Logger
	threshold          uint16
	numOfParties       uint16
	gap                BatchSequence
	confirmedSeq       BatchSequence
	confirmedSequences map[BatchSequence]map[PartyID]bool
	lock               sync.Mutex
	signal             sync.Cond
	stopped            bool
}

func NewAcker(confirmedSeq BatchSequence, gap BatchSequence, numOfParties uint16, threshold uint16, logger Logger) *Acker {
	a := &Acker{
		logger:             logger,
		threshold:          threshold,
		numOfParties:       numOfParties,
		gap:                gap,
		confirmedSeq:       confirmedSeq,
		confirmedSequences: make(map[BatchSequence]map[PartyID]bool),
	}
	a.signal = sync.Cond{L: &a.lock}
	return a
}

func (a *Acker) Stop() {
	a.logger.Infof("Stopping")
	a.lock.Lock()
	defer a.lock.Unlock()
	a.stopped = true
	a.signal.Broadcast()
}

func (a *Acker) HandleAck(seq BatchSequence, from PartyID) {
	a.logger.Infof("Called handle ack on sequence %d from %d", seq, from)

	a.lock.Lock()
	defer a.lock.Unlock()

	if seq < a.confirmedSeq {
		a.logger.Debugf("Received message on sequence %d but we expect sequence %d to %d", seq, a.confirmedSeq, a.confirmedSeq+gap)
		return
	}

	if seq-a.confirmedSeq > gap {
		a.logger.Warnf("Received message on sequence %d but our confirmed sequence is only at %d", seq, a.confirmedSeq)
		return
	}

	_, exists := a.confirmedSequences[seq]
	if !exists {
		a.confirmedSequences[seq] = make(map[PartyID]bool, a.numOfParties)
	}

	if _, exists := a.confirmedSequences[seq][from]; exists {
		a.logger.Warnf("Already received conformation on %d from %d", seq, from)
		return
	}

	a.confirmedSequences[seq][from] = true

	signatureCollectCount := len(a.confirmedSequences[a.confirmedSeq])
	if signatureCollectCount >= int(a.threshold) {
		a.logger.Infof("Removing %d digest mapping from memory as we received enough (%d) conformations", a.confirmedSeq, signatureCollectCount)
		delete(a.confirmedSequences, a.confirmedSeq)
		a.confirmedSeq++
		a.signal.Broadcast()
	} else {
		a.logger.Infof("Collected %d out of %d conformations on sequence %d", signatureCollectCount, a.threshold, seq)
	}
}

func (a *Acker) WaitForSecondaries(seq BatchSequence) {
	a.logger.Infof("Called wait with sequence %d", seq)
	t1 := time.Now()
	defer func() {
		a.logger.Infof("Waiting for secondaries to keep up with me took %v", time.Since(t1))
	}()
	a.lock.Lock()
	for !a.secondariesKeepUpWithMe(seq) {
		a.signal.Wait()
		if a.stopped {
			a.logger.Infof("Stopped waiting with sequence %d", seq)
			a.lock.Unlock()
			return
		}
	}
	a.lock.Unlock()
}

func (a *Acker) secondariesKeepUpWithMe(seq BatchSequence) bool {
	a.logger.Debugf("Current sequence: %d, confirmed sequence: %d", seq, a.confirmedSeq)
	return seq-a.confirmedSeq < a.gap
}
