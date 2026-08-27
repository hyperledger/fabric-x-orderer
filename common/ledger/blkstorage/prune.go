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
//     it fail with ErrPruned whether or not the bytes happen to still be on disk.
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

// pruneMgr owns the prune marker and the pruning of individual block files.
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

// pruneBefore removes the block files whose blocks all lie below blockNum, and fails reads below it.
// blockNum is an upper bound: only whole files can go, because index offsets are absolute within a file.
// It is capped at lastPersistedBlock, whose header newBlockfileMgr reads on open, and it never lowers the
// bound already in force.
//
// The algorithm:
//
//  1. Take the prune mutex, and sweep any file left behind below the lowest one the marker records.
//  2. Count the files that lie entirely below the capped request.
//  3. If none qualify, persist the new bound alone, or return if the bound did not move either.
//  4. Otherwise advance the bound, then remove each counted file in its own atomic commit.
//
// A call that stops partway keeps the bound it reached and the files it committed; the next call counts
// again to find the rest, which is why step 2 cannot be skipped when the bound is unchanged.
func (p *pruneMgr) pruneBefore(blockNum uint64, tail *blockfilesInfo) error {
	if tail.noBlockFiles {
		return nil
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	currentMarker := p.marker.Load()

	if err := p.sweepOrphans(currentMarker.firstStoredBlockfileNum); err != nil {
		return err
	}

	// lastPersistedBlock+1 is the ordinary way to ask for everything; above that the caller names a block
	// the ledger does not have.
	if blockNum > tail.lastPersistedBlock+1 {
		logger.Warnf("Prune request [%d] names a block the ledger does not have; its last block is [%d], "+
			"so the request is capped there", blockNum, tail.lastPersistedBlock)
	}

	// bound is how far removal may reach; firstAvailable is the lowest block a read may return. They differ
	// when the request is below the bound already in force, which must not lower it.
	bound := min(blockNum, tail.lastPersistedBlock)
	firstAvailable := max(currentMarker.firstReadableBlockNum, bound)

	lowestFile := currentMarker.firstStoredBlockfileNum
	eligible, err := p.countEligibleFiles(bound, lowestFile, tail)
	if err != nil {
		return err
	}

	if eligible == 0 {
		if firstAvailable == currentMarker.firstReadableBlockNum {
			// Nothing moved, so nothing is worth a durable write.
			return nil
		}
		// The request falls inside the lowest file, so only the bound moves.
		logger.Infof("Pruned blocks below [%d]; no block file became eligible", firstAvailable)
		return p.setMarker(&pruneMarker{
			firstReadableBlockNum:   firstAvailable,
			firstStoredBlockfileNum: lowestFile,
		})
	}

	// Advance the bound before any index entry goes, or a read could pass the check and then miss the index.
	// firstStoredBlockfileNum advances per file, as each commit lands.
	p.marker.Store(&pruneMarker{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: lowestFile,
	})

	for f := lowestFile; f < lowestFile+eligible; f++ {
		if err := p.pruneBlockfile(f, firstAvailable); err != nil {
			return err
		}
	}

	logger.Infof("Pruned blocks below [%d]; removed block files [%d] to [%d]",
		firstAvailable, lowestFile, lowestFile+eligible-1)
	return nil
}

// countEligibleFiles returns how many files, counting up from lowestFile, hold only blocks below bound.
// The active file is excluded, so the last block always survives.
func (p *pruneMgr) countEligibleFiles(bound uint64, lowestFile int, tail *blockfilesInfo) (int, error) {
	count := 0
	for f := lowestFile; f < tail.latestFileNumber; f++ {
		firstAbove, err := p.firstBlockNumAfter(f, tail)
		if err != nil {
			return 0, err
		}
		if firstAbove > bound {
			break
		}
		count++
	}
	return count, nil
}

// firstBlockNumAfter returns the first block number of the file above fileNum. Reading that file's first
// record is O(1); finding fileNum's own last block would mean scanning it.
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

// pruneBlockfile deletes the index entries of one file's blocks and records the file as gone in a single
// atomic batch, then unlinks it.
func (p *pruneMgr) pruneBlockfile(fileNum int, firstAvailable uint64) error {
	batch := p.db.NewUpdateBatch()
	blocks, err := p.addIndexDeletionsForFile(batch, fileNum)
	if err != nil {
		return err
	}
	marker := &pruneMarker{firstReadableBlockNum: firstAvailable, firstStoredBlockfileNum: fileNum + 1}
	batch.Put(pruneMarkerKey, marker.marshal())

	// The atomic commit point. A crash before it removes nothing; a crash after it leaves a block file
	// that nothing references, which sweepOrphans collects.
	if err := p.db.WriteBatch(batch, true); err != nil {
		return err
	}
	// Only now may the marker claim the file is gone: sweepOrphans unlinks everything below
	// firstStoredBlockfileNum, and a store that cannot reopen is the cost of claiming it early.
	p.marker.Store(marker)

	logger.Debugf("Pruning block file [%d], dropping the index entries of [%d] blocks", fileNum, blocks)
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

// sweepOrphans unlinks the block files below the lowest one the marker records. A call that committed and
// then failed to unlink leaves them behind the marker, where counting would never reach them again. It
// lists the directory rather than probing from zero, and syncs it once at the end.
func (p *pruneMgr) sweepOrphans(below int) error {
	if below == 0 {
		return nil
	}
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

// removeBlockfile unlinks a block file and syncs the directory, so each removal is durable on return
// instead of depending on the caller finishing the loop.
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

// setMarker persists the marker, then publishes it. It is for a call that moves the bound without removing
// a file; a removal writes the marker inside its own commit instead.
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
