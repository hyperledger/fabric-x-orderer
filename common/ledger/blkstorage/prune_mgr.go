/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"sync/atomic"

	"github.com/pkg/errors"
)

// ErrPruned is returned when a block below the prune point is requested, whether or not its bytes are
// still on disk.
var ErrPruned = errors.New("block has been pruned")

// pruneMgr owns the prune info: the live value readers consult, and the store that makes it durable. It is
// deliberately independent of blockfileMgr: it needs only the ledger's root directory.
type pruneMgr struct {
	store *pruneStore

	// info is held atomically so that read guards can consult it without locking, and is non-nil once the
	// manager is constructed.
	info atomic.Pointer[pruneInfo]
}

func newPruneMgr(rootDir string) (*pruneMgr, error) {
	store, err := newPruneStore(rootDir)
	if err != nil {
		return nil, err
	}

	info, err := store.getInfo()
	if err != nil {
		return nil, err
	}

	p := &pruneMgr{store: store}
	p.info.Store(info)
	return p, nil
}

func (p *pruneMgr) firstReadableBlockNum() uint64 {
	return p.info.Load().firstReadableBlockNum
}

func (p *pruneMgr) firstStoredBlockfileNum() int {
	return p.info.Load().firstStoredBlockfileNum
}

// setInfo persists the prune info and only then publishes it to readers; removing the blocks the new bound
// covers follows. That order is what makes a crash survivable without a single atomic write: a reader refused
// a block the store could still serve is harmless, whereas one served a block about to disappear is not.
//
// The caller must serialize this against itself and against the removals that follow, so that two prunes
// cannot publish their bounds in one order and remove their blocks in the other.
func (p *pruneMgr) setInfo(info *pruneInfo) error {
	if err := p.store.saveInfo(info); err != nil {
		return err
	}
	p.info.Store(info)
	return nil
}
