/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/tools/fileutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// ErrPruned is returned when a block below the prune point is requested, whether or not its bytes are
// still on disk.
var ErrPruned = errors.New("block has been pruned")

// LastConfigBlockLocator returns the number of the newest config block at or before the given block, and
// whether there is one. Which block is a config block is the caller's knowledge, not the block store's: an
// assembler ledger reads a block's LAST_CONFIG metadata, while on a consensus ledger the config block is
// nested inside a decision.
//
// found is a separate result rather than a sentinel number, because block 0 is the config block of a ledger
// that has never been reconfigured. A batcher's ledger holds batches and no configuration at all, so its
// locator reports found=false; passing a nil locator says the same thing.
type LastConfigBlockLocator func(block *common.Block) (blockNum uint64, found bool, err error)

// pruneMgr owns the prune info -- the live value readers consult, and the store that makes it durable --
// and the removal of the block files a new bound covers.
type pruneMgr struct {
	rootDir string
	index   *blockIndex

	// readBlock reads a block the ledger still holds. Retaining a config block has to read it before the
	// bound that hides it is published, and this is the only block the manager ever reads.
	readBlock func(blockNum uint64) (*common.Block, error)

	// store is only touched by setInfo, under mutex.
	store *pruneStore

	// info is held atomically so that read guards can consult it without locking, and is non-nil once the
	// manager is constructed.
	info atomic.Pointer[pruneInfo]

	// mutex serializes pruneBefore against itself, which is what setInfo requires of its caller.
	mutex sync.Mutex
}

func newPruneMgr(
	rootDir string, index *blockIndex, readBlock func(blockNum uint64) (*common.Block, error),
) (*pruneMgr, error) {
	store := newPruneStore(rootDir)

	// A ledger that has never been pruned has nothing stored, which reads as a zero bound.
	info, err := store.getInfo()
	if err != nil {
		return nil, err
	}

	p := &pruneMgr{rootDir: rootDir, index: index, readBlock: readBlock, store: store}
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
//  2. Retain the config block the new bound would hide, before anything is removed or hidden.
//  3. Count the files that lie entirely below the capped request.
//  4. If none qualify, persist the new bound alone, or return if nothing moved at all.
//  5. Otherwise remove each counted file, advancing the durable info ahead of each removal, and drop the
//     config blocks the retained one has superseded.
//
// A call that stops partway keeps the bound it reached and the files it removed; the next call counts again
// to find the rest, which is why step 3 cannot be skipped when the bound is unchanged.
func (p *pruneMgr) pruneBefore(
	blockNum uint64, tail *blockfilesInfo, locateLastConfig LastConfigBlockLocator,
) error {
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

	// Retaining comes before any removal, since the block to retain may well be in the first file to go.
	retained, err := p.retainConfigBlock(current, firstAvailable, locateLastConfig)
	if err != nil {
		return err
	}
	newInfo := func(firstStoredBlockfileNum int) *pruneInfo {
		return &pruneInfo{
			firstReadableBlockNum:   firstAvailable,
			firstStoredBlockfileNum: firstStoredBlockfileNum,
			retainedConfigBlockNum:  retained.blockNum,
			hasRetainedConfigBlock:  retained.present,
		}
	}

	lowestFile := current.firstStoredBlockfileNum
	eligible, err := p.countEligibleFiles(bound, lowestFile, tail)
	if err != nil {
		return err
	}

	if eligible == 0 {
		if firstAvailable == current.firstReadableBlockNum && retained == current.retainedConfig() {
			// Nothing moved, so nothing is worth a durable write.
			return nil
		}
		// The request falls inside the lowest file, so only the bound moves.
		logger.Infof("Pruned blocks below [%d]; no block file became eligible", firstAvailable)
		if err := p.setInfo(newInfo(lowestFile)); err != nil {
			return err
		}
		p.store.dropConfigBlocksBelow(retained.blockNum)
		return nil
	}

	// Block numbers run on across files, so a file holds the blocks from its own first one up to the first
	// one of the file above. Each file's range is known from these first blocks without reading the file.
	from, err := retrieveFirstBlockNumFromFile(p.rootDir, lowestFile)
	if err != nil {
		return errors.WithMessagef(err, "cannot determine the first block of block file [%d]", lowestFile)
	}
	for f := lowestFile; f < lowestFile+eligible; f++ {
		to, err := p.firstBlockNumAfter(f, tail)
		if err != nil {
			return err
		}
		if err := p.setInfo(newInfo(f + 1)); err != nil {
			return err
		}
		if err := p.pruneBlockfile(f, from, to); err != nil {
			return err
		}
		from = to
	}

	p.store.dropConfigBlocksBelow(retained.blockNum)

	logger.Infof("Pruned blocks below [%d]; removed block files [%d] to [%d]",
		firstAvailable, lowestFile, lowestFile+eligible-1)
	return nil
}

// retainConfigBlock copies the newest config block that the new bound would hide into the prune store, and
// returns what the prune info should record for it. What is already retained is returned unchanged when there
// is nothing new to keep: when the ledger holds no config blocks, when the bound did not move, or when the
// config block the bound needs stays readable on its own.
//
// The block handed to the locator is the one at the new bound, the lowest that stays readable. That is enough
// for a node resuming from the ledger, which needs the newest config block at or before its *last* block: if
// that one were below the bound, then no config block lies between the bound and the last block, so the
// bound's own config ancestor is that same block.
//
// A config block that is neither readable nor the one already retained fails the whole prune. That means an
// earlier prune hid it, so the ledger is already one a node cannot resume from -- which pruning further would
// only bury deeper, and which the caller should hear about rather than have papered over.
func (p *pruneMgr) retainConfigBlock(
	current *pruneInfo, firstAvailable uint64, locateLastConfig LastConfigBlockLocator,
) (retainedConfig, error) {
	if locateLastConfig == nil || firstAvailable == current.firstReadableBlockNum {
		return current.retainedConfig(), nil
	}

	boundBlock, err := p.readBlock(firstAvailable)
	if err != nil {
		return retainedConfig{}, errors.WithMessagef(err,
			"cannot read block [%d] to find its config block", firstAvailable)
	}
	configNum, found, err := locateLastConfig(boundBlock)
	if err != nil {
		return retainedConfig{}, errors.WithMessagef(err,
			"cannot locate the config block of block [%d]", firstAvailable)
	}

	// A ledger of batches has no configuration to keep; above the bound it needs no help; and the block
	// already retained needs no second copy.
	if !found || configNum >= firstAvailable || current.retains(configNum) {
		return current.retainedConfig(), nil
	}

	configBlock, err := p.readBlock(configNum)
	if err != nil {
		return retainedConfig{}, errors.WithMessagef(err, "cannot read config block [%d] to retain it", configNum)
	}
	blockBytes, err := proto.Marshal(configBlock)
	if err != nil {
		return retainedConfig{}, errors.Wrapf(err, "error marshalling config block [%d] to retain it", configNum)
	}
	if err := p.store.saveConfigBlock(configNum, blockBytes); err != nil {
		return retainedConfig{}, err
	}

	logger.Infof("Retained config block [%d], which pruning below [%d] would otherwise remove",
		configNum, firstAvailable)
	return retainedConfig{blockNum: configNum, present: true}, nil
}

// retainedConfigBlock returns the retained config block if blockNum names it, and nil if it does not. It is
// the one hole in the readable bound: a block below it that reads back anyway, though only through
// retrieveConfigBlockByNumber.
func (p *pruneMgr) retainedConfigBlock(blockNum uint64) (*common.Block, error) {
	if !p.info.Load().retains(blockNum) {
		return nil, nil
	}

	blockBytes, err := p.store.getConfigBlock(blockNum)
	if err != nil {
		return nil, p.errAfterFailedRetainedRead(blockNum, err)
	}
	block := &common.Block{}
	if err := proto.Unmarshal(blockBytes, block); err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling retained config block [%d]", blockNum)
	}
	return block, nil
}

// errAfterFailedRetainedRead decides what a failed read of the retained config block means. A prune that
// superseded the block between the check and the read has taken its copy away, and the block is then pruned
// like any other: that is reported as no block and no error, which sends the caller back to the refusal it
// was already holding. Anything else is a store that cannot read a block it says it has, and is returned.
func (p *pruneMgr) errAfterFailedRetainedRead(blockNum uint64, err error) error {
	if !p.info.Load().retains(blockNum) {
		return nil
	}
	return err
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

// pruneBlockfile deletes the index entries of the blocks in one block file, [from, to), and then unlinks it.
// Its caller has already recorded the file as gone, and each step here is durable before the next begins,
// which is what makes an interrupted prune recoverable without a single atomic write across the three of them.
//
// A crash before this runs leaves a file recorded as gone, which sweepOrphans unlinks on the next call. Its
// index entries stay behind: no read reaches them, since their blocks lie below firstReadableBlockNum and
// checkBlockAvailable refuses those before the index is consulted, and an index rebuild drops them. A crash
// between the index deletion and the unlink leaves the same kind of file without the entries.
func (p *pruneMgr) pruneBlockfile(fileNum int, from, to uint64) error {
	batch := p.index.db.NewUpdateBatch()
	p.index.addBlockNumDeletions(batch, from, to)
	if err := p.index.db.WriteBatch(batch, true); err != nil {
		return err
	}

	logger.Debugf("Pruning block file [%d], dropping the index entries of blocks [%d] to [%d]", fileNum, from, to-1)
	return p.removeBlockfile(fileNum)
}

// sweepOrphans unlinks the block files below the lowest one the info records. A prune that recorded a file
// as gone and then failed to unlink it leaves it behind the bound, where counting would never reach it
// again. It lists the directory rather than probing from zero, and syncs it once at the end.
//
// It leaves the index alone: entries such a prune did not get to delete stay behind, unreachable (see
// pruneBlockfile).
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
func (p *pruneMgr) setInfo(info *pruneInfo) error {
	if err := p.store.saveInfo(info); err != nil {
		return err
	}
	p.info.Store(info)
	return nil
}
