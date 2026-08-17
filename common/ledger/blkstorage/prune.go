/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"sync/atomic"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/util/leveldbhelper"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

// ErrPruned is returned when a block below the prune point is requested, whether or not its bytes are
// still on disk.
var ErrPruned = errors.New("block has been pruned")

// pruneMarkerKey holds the durable prune marker, next to blkMgrInfoKey, inside the ledger's DBHandle.
var pruneMarkerKey = []byte("blkPruneMarker")

// pruneMarker records how far the front of the ledger has been pruned. Absent (or zero) means nothing has
// been pruned and the ledger still starts at block 0.
//
// Its two fields answer different questions and move independently:
//
//   - firstReadableBlockNum is a logical bound. It is what a caller asked to prune below, so reads under
//     it are refused whether or not the bytes happen to still be on disk.
//   - firstStoredBlockfileNum is a physical fact: the lowest block file still present. Recovery scans start
//     there, and the orphan sweep bounds itself by it.
//
// They may diverge because pruning removes whole files only.
// Therefore, firstReadableBlockNum is at least the firstBlockNum in firstStoredBlockfileNum.
type pruneMarker struct {
	firstReadableBlockNum   uint64
	firstStoredBlockfileNum int
}

func (m *pruneMarker) marshal() []byte {
	var buf []byte
	buf = protowire.AppendVarint(buf, m.firstReadableBlockNum)
	buf = protowire.AppendVarint(buf, uint64(m.firstStoredBlockfileNum))
	return buf
}

func (m *pruneMarker) unmarshal(b []byte) error {
	var position int

	val, n := protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	m.firstReadableBlockNum = val

	val, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	m.firstStoredBlockfileNum = int(val)

	return nil
}

func (m *pruneMarker) String() string {
	return fmt.Sprintf("firstReadableBlockNum=[%d], firstStoredBlockfileNum=[%d]",
		m.firstReadableBlockNum, m.firstStoredBlockfileNum)
}

// pruneMgr owns the prune marker. It is deliberately independent of blockfileMgr: it needs only the
// ledger's slice of the index database, which makes it constructible and testable on its own.
type pruneMgr struct {
	db *leveldbhelper.DBHandle

	// marker is the durable prune marker. It is held atomically so that read guards can consult it without
	// locking, and is always non-nil once the manager is constructed.
	marker atomic.Pointer[pruneMarker]
}

func newPruneMgr(db *leveldbhelper.DBHandle) (*pruneMgr, error) {
	p := &pruneMgr{db: db}
	marker, err := p.load()
	if err != nil {
		return nil, err
	}
	p.marker.Store(marker)
	return p, nil
}

func (p *pruneMgr) firstReadableBlockNum() uint64 {
	return p.marker.Load().firstReadableBlockNum
}

func (p *pruneMgr) firstStoredBlockfileNum() int {
	return p.marker.Load().firstStoredBlockfileNum
}

// setMarker persists the marker and then publishes it to readers. It is the path for a call that moves the
// readable bound without removing a file; when a file is removed, the marker is written inside the same
// batch as the index deletions, so that each file's commit is atomic.
func (p *pruneMgr) setMarker(marker *pruneMarker) error {
	if err := p.save(marker); err != nil {
		return err
	}
	p.marker.Store(marker)
	return nil
}

func (p *pruneMgr) save(marker *pruneMarker) error {
	return p.db.Put(pruneMarkerKey, marker.marshal(), true)
}

// load reads the durable marker. An absent key means nothing has been pruned, and yields a zero-valued
// marker rather than nil so that readers never have to nil-check.
func (p *pruneMgr) load() (*pruneMarker, error) {
	b, err := p.db.Get(pruneMarkerKey)
	if err != nil {
		return nil, err
	}
	marker := &pruneMarker{}
	if b == nil {
		return marker, nil
	}
	if err := marker.unmarshal(b); err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling prune marker")
	}
	logger.Debugf("loaded prune marker:%s", marker)
	return marker, nil
}
