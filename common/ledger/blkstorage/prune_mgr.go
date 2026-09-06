/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/util/leveldbhelper"

	"github.com/hyperledger/fabric-x-common/tools/fileutil"
	"github.com/pkg/errors"
)

// ErrPruned is returned when a block below the prune point is requested, whether or not its bytes are
// still on disk.
var ErrPruned = errors.New("block has been pruned")

// pruneMgr owns the prune info -- the live value readers consult, and the store that makes it durable --
// and the removal of the block files a new bound covers.
type pruneMgr struct {
	rootDir string
	index   *blockIndex

	// store is only touched by setInfo, under mutex.
	store *pruneStore

	// info is held atomically so that read guards can consult it without locking, and is non-nil once the
	// manager is constructed.
	info atomic.Pointer[pruneInfo]

	// mutex serializes pruneBefore against itself, which is what setInfo requires of its caller.
	mutex sync.Mutex
}

func newPruneMgr(rootDir string, index *blockIndex) (*pruneMgr, error) {
	store := newPruneStore(rootDir)

	// A ledger that has never been pruned has nothing stored, which reads as a zero bound.
	info, err := store.getInfo()
	if err != nil {
		return nil, err
	}

	p := &pruneMgr{rootDir: rootDir, index: index, store: store}
	p.info.Store(info)
	return p, nil
}

// pruneBefore removes the block files whose blocks all lie below blockNum, and refuses reads below it.
// blockNum is an upper bound: only whole files can go, because index offsets are absolute within a file.
// It is capped at lastPersistedBlock, whose header newBlockfileMgr reads on open, and it never lowers the
// bound already in force.
//
// The algorithm:
//
//  1. Take the prune mutex, and sweep any file left behind below the lowest one the info records.
//  2. Count the files that lie entirely below the capped request.
//  3. If none qualify, persist the new bound alone, or return if the bound did not move either.
//  4. Otherwise remove each counted file, advancing the durable info ahead of each removal.
//
// A call that stops partway keeps the bound it reached and the files it removed; the next call counts again
// to find the rest, which is why step 2 cannot be skipped when the bound is unchanged.
func (p *pruneMgr) pruneBefore(blockNum uint64, tail *blockfilesInfo) error {
	if tail.noBlockFiles {
		return nil
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	current := p.info.Load()

	if err := p.sweepOrphans(current.firstStoredBlockfileNum); err != nil {
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
	firstAvailable := max(current.firstReadableBlockNum, bound)

	lowestFile := current.firstStoredBlockfileNum
	eligible, err := p.countEligibleFiles(bound, lowestFile, tail)
	if err != nil {
		return err
	}

	if eligible == 0 {
		if firstAvailable == current.firstReadableBlockNum {
			// Nothing moved, so nothing is worth a durable write.
			return nil
		}
		// The request falls inside the lowest file, so only the bound moves.
		logger.Infof("Pruned blocks below [%d]; no block file became eligible", firstAvailable)
		return p.setInfo(&pruneInfo{
			firstReadableBlockNum:   firstAvailable,
			firstStoredBlockfileNum: lowestFile,
		})
	}

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

// pruneBlockfile removes one block file: it records the file as gone, deletes the index entries of the
// blocks in it, and only then unlinks it. Each step is durable before the next begins, which is what makes
// an interrupted prune recoverable without a single atomic write across the three of them.
//
// A crash after the first step leaves a file recorded as gone, which sweepOrphans unlinks on the next call.
// A crash after the second leaves index entries pointing into a file that is no longer there, which the
// read guards already refuse: their location's file number is below firstStoredBlockfileNum.
func (p *pruneMgr) pruneBlockfile(fileNum int, firstAvailable uint64) error {
	err := p.setInfo(&pruneInfo{firstReadableBlockNum: firstAvailable, firstStoredBlockfileNum: fileNum + 1})
	if err != nil {
		return err
	}

	batch := p.index.db.NewUpdateBatch()
	blocks, err := p.addIndexDeletionsForFile(batch, fileNum)
	if err != nil {
		return err
	}
	if err := p.index.db.WriteBatch(batch, true); err != nil {
		return err
	}

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
		p.index.addEntriesToBeDeleted(batch, blockInfo)
		blocks++
	}
}

// sweepOrphans unlinks the block files below the lowest one the info records. A prune that recorded a file
// as gone and then failed to unlink it leaves it behind the bound, where counting would never reach it
// again. It lists the directory rather than probing from zero, and syncs it once at the end.
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
	return p.info.Load().firstReadableBlockNum
}

func (p *pruneMgr) firstStoredBlockfileNum() int {
	return p.info.Load().firstStoredBlockfileNum
}

// setInfo persists the prune info and only then publishes it to readers; removing the blocks the new bound
// covers follows. That order is what makes a crash survivable without a single atomic write: a reader refused
// a block the store could still serve is harmless, whereas one served a block about to disappear is not.
//
// It must be called under mutex, so that two prunes cannot publish their bounds in one order and remove
// their blocks in the other.
//
// The removals must include the covered blocks' index entries, not only their block files: the hash and
// txID lookups resolve a location before they can learn which block it belongs to.
func (p *pruneMgr) setInfo(info *pruneInfo) error {
	if err := p.store.saveInfo(info); err != nil {
		return err
	}
	p.info.Store(info)
	return nil
}
