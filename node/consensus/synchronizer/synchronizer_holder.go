/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"sync"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

// Holder is a stable indirection over a SynchronizerWithStop.
//
// SmartBFT reads its Synchronizer field without a lock in pkg/consensus.Sync(), and the
// viewchanger goroutine can drive a Sync() independently of the controller goroutine. On the
// dynamic reconfiguration path the inner synchronizer must be replaced while the BFT instance
// keeps running, which would race that unlocked read if we reassigned the field directly.
//
// Instead, SmartBFT (and arma's own c.Synchronizer) holds the Holder once, at BFT creation, and
// never sees it reassigned. Reconfiguration calls Swap to replace the inner synchronizer under
// the Holder's own lock, so Sync()/Stop() reads and the swap are serialized.
type Holder struct {
	mu    sync.RWMutex
	inner SynchronizerWithStop
}

// NewHolder returns a Holder wrapping the given inner synchronizer.
func NewHolder(inner SynchronizerWithStop) *Holder {
	return &Holder{inner: inner}
}

func (h *Holder) Sync() smartbft_types.SyncResponse {
	h.mu.RLock()
	s := h.inner
	h.mu.RUnlock()
	return s.Sync()
}

func (h *Holder) Stop() {
	h.mu.RLock()
	s := h.inner
	h.mu.RUnlock()
	s.Stop()
}

// Swap replaces the inner synchronizer under the Holder's lock. It is safe to call concurrently
// with Sync()/Stop().
func (h *Holder) Swap(inner SynchronizerWithStop) {
	h.mu.Lock()
	h.inner = inner
	h.mu.Unlock()
}

var _ SynchronizerWithStop = (*Holder)(nil)
