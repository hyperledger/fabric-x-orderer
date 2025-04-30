package batcher

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
)

type ControlEventBroadcaster struct {
	senders          []ConsenterControlEventSender
	threshold        int
	n                int
	f                int
	minRetryInterval time.Duration
	maxRetryDelay    time.Duration
	logger           types.Logger
}

func (b *ControlEventBroadcaster) BroadcastControlEvent(ctx context.Context, ce core.ControlEvent) error {
	retrySenders := b.senders
	delay := b.minRetryInterval

	for {
		var failed []ConsenterControlEventSender

		for _, sender := range retrySenders {
			// TODO: Add goroutines for sending control events
			if err := b.sendControlEvent(ce, sender); err != nil {
				failed = append(failed, sender)
			}
		}

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
			return errors.Errorf("broadcast was cancelled: %v", ctx.Err())
		case <-timer.C:
		}

		retrySenders = failed
	}
}

func (b *ControlEventBroadcaster) sendControlEvent(ce core.ControlEvent, sender ConsenterControlEventSender) error {
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

func NewControlEventBroadcaster(senders []ConsenterControlEventSender, n int, f int, minRetryInterval time.Duration, maxRetryDelay time.Duration, logger types.Logger) *ControlEventBroadcaster {
	return &ControlEventBroadcaster{
		senders:          senders,
		n:                n,
		f:                f,
		threshold:        n - f,
		minRetryInterval: minRetryInterval,
		maxRetryDelay:    maxRetryDelay,
		logger:           logger,
	}
}
