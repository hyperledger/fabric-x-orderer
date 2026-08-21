/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-x-common/tools/fileutil"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/util/leveldbhelper"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

// ErrPruned is returned when a block that has been physically reclaimed by pruning is
// requested.
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
// They may diverge because only whole files can be reclaimed.
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

// pruneMgr owns the prune marker and the reclamation operation.
type pruneMgr struct {
	rootDir string
	db      *leveldbhelper.DBHandle
	index   *blockIndex

	// marker is the durable prune marker. It is held atomically so that read guards can consult it without
	// locking, and is always non-nil once the manager is constructed.
	marker atomic.Pointer[pruneMarker]

	// mutex serializes pruneBefore operations.
	mutex sync.Mutex
}

func newPruneMgr(rootDir string, db *leveldbhelper.DBHandle, index *blockIndex) (*pruneMgr, error) {
	p := &pruneMgr{rootDir: rootDir, db: db, index: index}
	marker, err := p.load()
	if err != nil {
		return nil, err
	}
	p.marker.Store(marker)
	return p, nil
}

// pruneBefore reclaims block files whose blocks all lie below blockNum.
// The bound is capped at lastPersistedBlock in tail, so that the last block always stays readable. newBlockfileMgr
// reads its header on open, and would panic otherwise. It is monotone: a blockNum below the current bound
// never lowers it.
//
// The algorithm:
//
//  1. Take the prune mutex, which serializes this against another prune.
//  2. Unlink any block file below the lowest recorded one: an earlier call may have committed without
//     finishing its unlinks, and the walk below would never revisit those files.
//  3. Compute the readable bound, then walk files upward from the lowest one. A file is reclaimable when
//     every block in it lies below the bound; the walk stops at the first one that is not. Only whole
//     files are eligible, because the index records offsets absolute within a file.
//  4. Reclaim each such file in its own atomic commit: delete the index entries of its blocks and record
//     it as gone, then unlink it. If no file was reclaimable, commit the new bound on its own.
func (p *pruneMgr) pruneBefore(blockNum uint64, tail *blockfilesInfo) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	current := p.marker.Load()

	if err := p.sweepOrphans(current.firstStoredBlockfileNum); err != nil {
		return err
	}

	// bound is how far reclamation may reach: the request, held back so that the file holding the last
	// block is never reclaimed.
	bound := min(blockNum, tail.lastPersistedBlock)
	// firstAvailable is the lowest block a read may return after this call: the caller's request, never
	// lowered, and never past the last block, whose header must stay readable when the store reopens.
	firstAvailable := max(current.firstReadableBlockNum, bound)

	firstReclaimed, lastReclaimed := -1, -1
	var offDisk uint64
	for f := current.firstStoredBlockfileNum; f < tail.latestFileNumber; f++ {
		nextFirst, err := p.firstBlockNumAfter(f, tail)
		if err != nil {
			return err
		}
		if nextFirst > bound {
			break
		}
		if err := p.reclaimBlockfile(f, firstAvailable); err != nil {
			return err
		}
		if firstReclaimed < 0 {
			firstReclaimed = f
		}
		lastReclaimed, offDisk = f, nextFirst
	}

	if lastReclaimed >= 0 {
		logger.Infof("Pruned blocks below [%d]; reclaimed block files [%d] to [%d], so blocks below [%d] are off disk",
			firstAvailable, firstReclaimed, lastReclaimed, offDisk)
		return nil
	}

	if firstAvailable == current.firstReadableBlockNum {
		return nil
	}
	// Nothing became reclaimable, but the bound still moves: the request falls inside the lowest file.
	logger.Infof("Pruned blocks below [%d]; no block file became reclaimable", firstAvailable)
	return p.setMarker(&pruneMarker{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: current.firstStoredBlockfileNum,
	})
}

// firstBlockNumAfter returns the block number following the last block of fileNum, which is the first
// block of the file above it. Reading that file's first record is O(1), where finding fileNum's own last
// block would mean scanning all of it.
func (p *pruneMgr) firstBlockNumAfter(fileNum int, tail *blockfilesInfo) (uint64, error) {
	if fileNum+1 == tail.latestFileNumber && tail.latestFileSize == 0 {
		return tail.lastPersistedBlock + 1, nil
	}
	n, err := retrieveFirstBlockNumFromFile(p.rootDir, fileNum+1)
	if err != nil {
		return 0, errors.WithMessagef(err, "cannot determine the first block of block file [%d]", fileNum+1)
	}
	return n, nil
}

// reclaimBlockfile deletes the index entries of every block in one file and records the file as gone, in a
// single atomic batch, then unlinks it. firstAvailable is the call's readable bound, written with every
// commit so that it is durable from the first one onward.
func (p *pruneMgr) reclaimBlockfile(fileNum int, firstAvailable uint64) error {
	batch := p.db.NewUpdateBatch()
	blocks, err := p.addIndexDeletionsForFile(batch, fileNum)
	if err != nil {
		return err
	}
	marker := &pruneMarker{firstReadableBlockNum: firstAvailable, firstStoredBlockfileNum: fileNum + 1}
	batch.Put(pruneMarkerKey, marker.marshal())

	// The atomic commit point. A crash before it reclaims nothing; a crash after it leaves a block file
	// that nothing references, which sweepOrphans collects.
	if err := p.db.WriteBatch(batch, true); err != nil {
		return err
	}
	p.marker.Store(marker)

	logger.Debugf("Reclaiming block file [%d], dropping the index entries of [%d] blocks", fileNum, blocks)
	return p.removeBlockfile(fileNum)
}

// addIndexDeletionsForFile queues deletion of the index entries of every block in one block file, and
// returns how many blocks it queued.
func (p *pruneMgr) addIndexDeletionsForFile(batch *leveldbhelper.UpdateBatch, fileNum int) (int, error) {
	stream, err := newBlockStream(p.rootDir, fileNum, 0, fileNum)
	if err != nil {
		return 0, err
	}
	defer stream.close()

	blocks := 0
	for {
		// Nil bytes with no error is the stream's clean end of file. A partially written record at the tail
		// comes back as ErrUnexpectedEndOfBlockfile instead, and is returned as the error it is.
		blockBytes, _, err := stream.nextBlockBytesAndPlacementInfo()
		if err != nil {
			return blocks, err
		}
		if blockBytes == nil {
			return blocks, nil
		}
		blockInfo, err := extractSerializedBlockInfo(blockBytes)
		if err != nil {
			return blocks, err
		}
		if err := addIndexEntriesToBeDeleted(batch, blockInfo, p.index); err != nil {
			return blocks, err
		}
		blocks++
	}
}

// sweepOrphans unlinks block files below the lowest one the marker records. An earlier call may have
// committed and then failed to unlink; the marker has already moved past those files, so the walk in
// pruneBefore would never revisit them and they would leak. It lists the directory rather than probing
// every number from zero. Unlike reclamation this syncs the directory once, at the end: nothing is
// interleaved with the unlinks, and an unlink lost to a crash only leaves an orphan for the next call to find again.
func (p *pruneMgr) sweepOrphans(below int) error {
	if below == 0 {
		return nil
	}
	// List the directory files numbers in ascending order
	nums, err := blockfileNumsIn(p.rootDir)
	if err != nil {
		return err
	}

	swept := 0
	for _, n := range nums {
		if n >= below {
			break // ascending, so nothing lower remains
		}
		if err := os.Remove(deriveBlockfilePath(p.rootDir, n)); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "error removing orphaned block file [%d]", n)
		}
		swept++
	}
	if swept == 0 {
		return nil
	}
	logger.Infof("Removed [%d] block files below [%d], left behind by an earlier prune", swept, below)
	return fileutil.SyncDir(p.rootDir)
}

// removeBlockfile removes a block file and syncs the directory, rather than once per prune, so that a
// reclamation is durable by the time this returns and does not depend on its caller finishing the job.
func (p *pruneMgr) removeBlockfile(fileNum int) error {
	if err := os.Remove(deriveBlockfilePath(p.rootDir, fileNum)); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "error removing block file [%d]", fileNum)
	}
	return fileutil.SyncDir(p.rootDir)
}

func (p *pruneMgr) firstReadableBlockNum() uint64 {
	return p.marker.Load().firstReadableBlockNum
}

func (p *pruneMgr) firstStoredBlockfileNum() int {
	return p.marker.Load().firstStoredBlockfileNum
}

// setMarker persists the marker and then publishes it to readers. It is the path for a call that moves the
// readable bound without reclaiming a file; reclamation instead writes the marker inside the same batch as
// the index deletions, so that each file's commit is atomic.
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
