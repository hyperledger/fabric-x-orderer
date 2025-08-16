/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type SeqAcker interface {
	Stop()
	HandleAck(seq types.BatchSequence, from types.PartyID)
	WaitForSecondaries(seq types.BatchSequence) chan struct{}
}

// Acker handles the acks coming from secondaries
type Acker struct {
	logger             types.Logger
	threshold          uint16
	numOfParties       uint16
	gap                types.BatchSequence
	confirmedSeq       types.BatchSequence
	confirmedSequences map[types.BatchSequence]map[types.PartyID]bool
	lock               sync.Mutex
	signal             sync.Cond
	stopped            bool
}

// NewAcker returns a new acker
func NewAcker(confirmedSeq types.BatchSequence, gap types.BatchSequence, numOfParties uint16, threshold uint16, logger types.Logger) *Acker {
	a := &Acker{
		logger:             logger,
		threshold:          threshold,
		numOfParties:       numOfParties,
		gap:                gap,
		confirmedSeq:       confirmedSeq,
		confirmedSequences: make(map[types.BatchSequence]map[types.PartyID]bool),
	}
	a.signal = sync.Cond{L: &a.lock}
	return a
}

// Stop stops the acker (the waiting)
func (a *Acker) Stop() {
	a.logger.Debugf("Stopping")
	a.lock.Lock()
	defer a.lock.Unlock()
	a.stopped = true
	a.signal.Broadcast()
}

// HandleAck handles an ack with a given seq from a specific party
func (a *Acker) HandleAck(seq types.BatchSequence, from types.PartyID) {
	a.logger.Debugf("Called handle ack on sequence %d from %d", seq, from)

	a.lock.Lock()
	defer a.lock.Unlock()

	if seq < a.confirmedSeq {
		a.logger.Debugf("Received message on sequence %d but we expect sequence %d to %d", seq, a.confirmedSeq, a.confirmedSeq+a.gap)
		return
	}

	if seq-a.confirmedSeq > a.gap {
		a.logger.Warnf("Received message on sequence %d but our confirmed sequence is only at %d", seq, a.confirmedSeq)
		return
	}

	_, exists := a.confirmedSequences[seq]
	if !exists {
		a.confirmedSequences[seq] = make(map[types.PartyID]bool, a.numOfParties)
	}

	if _, exists := a.confirmedSequences[seq][from]; exists {
		a.logger.Warnf("Already received conformation on %d from %d", seq, from)
		return
	}

	a.confirmedSequences[seq][from] = true

	signatureCollectCount := len(a.confirmedSequences[a.confirmedSeq])
	if signatureCollectCount >= int(a.threshold) {
		a.logger.Debugf("Removing %d digest mapping from memory as we received enough (%d) conformations", a.confirmedSeq, signatureCollectCount)
		delete(a.confirmedSequences, a.confirmedSeq)
		a.confirmedSeq++
		a.signal.Broadcast()
	} else {
		a.logger.Debugf("Collected %d out of %d conformations on sequence %d", signatureCollectCount, a.threshold, seq)
	}
}

// WaitForSecondaries waits for the secondaries to keep up with the primary (to send enough acks)
func (a *Acker) WaitForSecondaries(seq types.BatchSequence) chan struct{} {
	c := make(chan struct{})
	go func() {
		a.wait(seq)
		close(c)
	}()
	return c
}

func (a *Acker) wait(seq types.BatchSequence) {
	a.logger.Debugf("Called wait with sequence %d", seq)
	t1 := time.Now()
	defer func() {
		a.logger.Debugf("Waiting for secondaries to keep up with me took %v", time.Since(t1))
	}()
	a.lock.Lock()
	for !a.secondariesKeepUpWithMe(seq) {
		a.signal.Wait()
		if a.stopped {
			a.logger.Debugf("Stopped waiting with sequence %d", seq)
			a.lock.Unlock()
			return
		}
	}
	a.lock.Unlock()
}

func (a *Acker) secondariesKeepUpWithMe(seq types.BatchSequence) bool {
	a.logger.Debugf("Current sequence: %d, confirmed sequence: %d", seq, a.confirmedSeq)
	return seq-a.confirmedSeq < a.gap
}
