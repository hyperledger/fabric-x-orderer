package arma

import "sync"

type TotalOrder interface {
	SubmitRequest(req []byte) error
	// Deliver returns a series of control events to be processed by Arma,
	// and returns a feedback function which receives two parameters:
	// (1) Batch Attestation Fragments for which a threshold has been collected
	// (2) The new bytes of the new state of the Consenter, to be persisted in stable storage.
	// The next time the Consenter is instantiated, these bytes are to be passed as input.
	Deliver() ([]ControlEvent, func([]BatchAttestationFragment, []byte))
}

type Consenter struct {
	Logger     Logger
	TotalOrder TotalOrder
	lock       sync.RWMutex
	State      State
}

func (c *Consenter) ReadyBatchAttestations(events []ControlEvent) []BatchAttestationFragment {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, fragments := c.State.Process(c.Logger, events...)
	return fragments
}

func (c *Consenter) Run() {
	go func() {
		for {
			events, feedback := c.TotalOrder.Deliver()
			state, fragments := c.State.Process(c.Logger, events...)

			c.lock.Lock()
			c.State = state
			c.lock.Unlock()

			feedback(fragments, state.Serialize())
		}
	}()
}

func (c *Consenter) Submit(rawControlEvent []byte) {
	if err := c.TotalOrder.SubmitRequest(rawControlEvent); err != nil {
		c.Logger.Warnf("Failed submitting request:", err)
		return
	}
}
