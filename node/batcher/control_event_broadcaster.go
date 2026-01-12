/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

type ControlEventBroadcaster struct {
	senders          []ConsenterControlEventSender
	threshold        int
	n                int
	f                int
	minRetryInterval time.Duration
	maxRetryDelay    time.Duration
	logger           types.Logger
	ctx              context.Context
	cancelFunc       context.CancelFunc
}

func (b *ControlEventBroadcaster) BroadcastControlEvent(ce state.ControlEvent, ctx context.Context) error {
	retrySenders := b.senders
	delay := b.minRetryInterval

	for {
		var failed []ConsenterControlEventSender
		var failedMu sync.Mutex
		var wg sync.WaitGroup

		for _, sender := range retrySenders {
			wg.Add(1)
			go func(s ConsenterControlEventSender) {
				defer wg.Done()
				if err := b.sendControlEvent(ce, s); err != nil {
					failedMu.Lock()
					failed = append(failed, s)
					failedMu.Unlock()
				}
			}(sender)
		}

		wg.Wait()

		if len(b.senders)-len(failed) >= b.threshold {
			b.logger.Infof("Control event sent to quorum (%d out of %d)", len(b.senders)-len(failed), len(b.senders))
			return nil
		}

		delay *= 2
		if delay > b.maxRetryDelay {
			delay = b.maxRetryDelay
		}

		b.logger.Warnf("Only %d successful sends (need %d); going to retry %d failed after %s", len(b.senders)-len(failed), b.threshold, len(failed), delay)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			b.logger.Errorf("broadcast cancelled")
			return errors.Errorf("broadcast was cancelled: %v", b.ctx.Err())
		case <-b.ctx.Done():
			timer.Stop()
			b.logger.Errorf("broadcaster cancelled")
			return errors.Errorf("broadcaster was cancelled: %v", b.ctx.Err())
		case <-timer.C:
		}

		retrySenders = failed
	}
}

func (b *ControlEventBroadcaster) sendControlEvent(ce state.ControlEvent, sender ConsenterControlEventSender) error {
	t1 := time.Now()

	defer func() {
		b.logger.Infof("Sending control event took %v", time.Since(t1))
	}()

	if err := sender.SendControlEvent(ce); err != nil {
		b.logger.Errorf("Failed sending control event; err: %v", err)
		return err
	}

	return nil
}

func NewControlEventBroadcaster(senders []ConsenterControlEventSender, n int, f int, minRetryInterval time.Duration, maxRetryDelay time.Duration, logger types.Logger, ctx context.Context, cancelFunc context.CancelFunc) *ControlEventBroadcaster {
	return &ControlEventBroadcaster{
		senders:          senders,
		n:                n,
		f:                f,
		threshold:        n - f,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
		logger:           logger,
		ctx:              ctx,
		cancelFunc:       cancelFunc,
	}
}

func (b *ControlEventBroadcaster) Stop() {
	b.cancelFunc()
}
