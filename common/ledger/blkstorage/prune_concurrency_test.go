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
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"
	"github.com/stretchr/testify/require"
)

// maxBlockfileSizeForConcurrencyTest keeps block files small enough that appends roll over on their own
// and pruning has several files to remove while other goroutines are reading. It holds about five of
// these test blocks: a file must hold more than one for a reader to have anything left to read after
// the files around it are removed.
const maxBlockfileSizeForConcurrencyTest = 20480

// pruneAttemptInterval keeps the background pruners from spinning on LevelDB between attempts; it is
// short enough that each test still makes hundreds of prune calls against a live reader or appender.
const pruneAttemptInterval = 100 * time.Microsecond

// newConcurrentTestLedger builds numBlocks blocks and stores the first numToStore of them, letting
// addBlock roll over between files by itself rather than driving the boundaries from the test. The
// returned slice holds every block, so a test can append the remainder while other goroutines run. The
// block cache is disabled so that reads reach the index and the block files, which is where pruning and
// reading actually contend.
func newConcurrentTestLedger(
	t *testing.T, numBlocks int, numToStore int,
) (*testEnv, *testBlockfileMgrWrapper, []*common.Block) {
	blocks := testutil.ConstructTestBlocks(t, numBlocks)
	env := newTestEnv(t, NewConf(t.TempDir(), maxBlockfileSizeForConcurrencyTest))
	w := newTestBlockfileWrapper(env, "testLedger")
	w.blockfileMgr.cache = newCache(0)
	for _, b := range blocks[:numToStore] {
		require.NoError(t, w.blockfileMgr.addBlock(b))
	}

	require.Greater(t, len(blockfileNums(t, w.blockfileMgr.rootDir)), 3,
		"fixture must span several block files for pruning to have anything to remove")

	return env, w, blocks
}

// Scenario:
//  1. Store 60 blocks spread over several block files, with the block cache disabled.
//  2. Append 60 more blocks in one goroutine while a second goroutine repeatedly prunes below a bound
//     that trails both the growing height and the live reader below.
//  3. Stream the appended blocks through a live iterator in a third goroutine, which the prune point is
//     bounded by and so never overtakes.
//  4. Read every block number from 0 to the current height on the test goroutine throughout, and sample
//     the prune marker.
//  5. Expect every read to either return the requested block or report ErrPruned, and nothing else.
//  6. Expect the live iterator to deliver every block unchanged and without an error, since pruning stayed
//     behind it.
//  7. Expect the height to reach 120, the marker to have advanced, and the marker never to move backwards.
func TestPruneConcurrentWithAppendsAndReads(t *testing.T) {
	const seeded, appended = 60, 60
	env, w, blocks := newConcurrentTestLedger(t, seeded+appended, seeded)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	var wg sync.WaitGroup
	stop := make(chan struct{})
	appendDone := make(chan struct{})
	appendErrs := make(chan error, appended)
	pruneErrs := make(chan error, 1024)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(appendDone)
		for _, b := range blocks[seeded:] {
			if err := mgr.addBlock(b); err != nil {
				appendErrs <- err
				return
			}
		}
	}()

	// readerBlock is where the live iterator has reached. The pruner is bounded by it, so the prune point
	// stays behind the reader: that is the ordinary case in production, and the one being asserted here.
	// A prune that overtakes a reader has its own test.
	var readerBlock atomic.Uint64
	readerBlock.Store(seeded)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			time.Sleep(pruneAttemptInterval)
			// Trail the height so the bound keeps climbing as the appender makes progress.
			height := mgr.getBlockchainInfo().Height
			if height > 20 {
				if err := mgr.pruneBefore(min(height-20, readerBlock.Load())); err != nil {
					pruneErrs <- err
					return
				}
			}
		}
	}()

	// The streamed blocks are asserted after the wait below, since a failure raised from another goroutine
	// would not be reported against this test.
	var (
		streamed  []*common.Block
		streamErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		itr, err := mgr.retrieveBlocks(seeded)
		if err != nil {
			streamErr = err
			return
		}
		defer itr.Close()
		for want := uint64(seeded); want < seeded+appended; want++ {
			result, err := itr.Next()
			if err != nil {
				streamErr = err
				return
			}
			block, ok := result.(*common.Block)
			if !ok {
				streamErr = fmt.Errorf("iterator returned %T at block %d", result, want)
				return
			}
			streamed = append(streamed, block)
			readerBlock.Store(want + 1)
		}
	}()

	// Reads and marker sampling run on the test goroutine so a failure is reported by the test itself.
	// Only blocks the appender has already committed are read, since a block that does not exist yet is
	// legitimately absent rather than pruned.
	var highestMarker uint64
	requireMarkerMonotone := func() {
		seen := mgr.pruner.firstReadableBlockNum()
		require.GreaterOrEqual(t, seen, highestMarker, "prune marker moved backwards")
		highestMarker = seen
	}
	requireReadable := func(i uint64) {
		block, err := mgr.retrieveBlockByNumber(i)
		if err != nil {
			require.ErrorIs(t, err, ErrPruned, "block %d failed for a reason other than pruning", i)
			return
		}
		require.Equal(t, blocks[i], block, "block %d came back wrong", i)
	}

	for appending := true; appending; {
		select {
		case <-appendDone:
			appending = false
		default:
		}
		requireMarkerMonotone()
		for i := uint64(0); i < mgr.getBlockchainInfo().Height; i++ {
			requireReadable(i)
		}
	}

	close(stop)
	wg.Wait()
	close(appendErrs)
	close(pruneErrs)

	// With appends and pruning both finished, every block must now be either readable or pruned.
	requireMarkerMonotone()
	for i := uint64(0); i < seeded+appended; i++ {
		requireReadable(i)
	}

	for err := range appendErrs {
		require.NoError(t, err, "appending must not fail while pruning runs")
	}
	for err := range pruneErrs {
		require.NoError(t, err, "pruning must not fail while appending runs")
	}
	require.NoError(t, streamErr, "a prune that stays behind a live iterator must not disturb it")
	require.Equal(t, blocks[seeded:], streamed, "the live iterator must deliver every block unchanged")

	require.Equal(t, uint64(seeded+appended), mgr.getBlockchainInfo().Height)
	require.Greater(t, mgr.pruner.firstReadableBlockNum(), uint64(0), "pruning made no progress")
	requirePruneInvariant(t, mgr)
}

// Scenario:
//  1. Store 120 blocks spread over several block files, with the block cache disabled.
//  2. Open an iterator at block 20 and read one block, so its stream is open on a block file.
//  3. Prune far past the reader, removing files it has not reached yet, then keep reading.
//  4. Expect every block the iterator returns to be the next one in sequence and byte-identical, and
//     the read to end in ErrPruned rather than a gap: the reader drains the file it holds open, then
//     reaches one that is gone.
//  5. Read once more, as a consumer that retries instead of closing, and expect the same failure again
//     rather than a panic: the stream owns no block file after a failed advance.
//  6. Expect closing the iterator after that failure to succeed, since a consumer always closes the
//     stream it was handed.
func TestPruneOvertakingLiveIteratorStopsWithoutReturningWrongBlocks(t *testing.T) {
	const numBlocks, start = 120, 20
	env, w, blocks := newConcurrentTestLedger(t, numBlocks, numBlocks)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	itr, err := mgr.retrieveBlocks(start)
	require.NoError(t, err)

	// Read one block first, so the iterator holds an open stream before anything is removed.
	result, err := itr.Next()
	require.NoError(t, err)
	require.Equal(t, blocks[start], result)

	require.NoError(t, mgr.pruneBefore(start+40))

	delivered := 0
	var lastErr error
	for want := uint64(start + 1); want < numBlocks; want++ {
		result, err := itr.Next()
		if err != nil {
			lastErr = err
			break
		}
		block, ok := result.(*common.Block)
		require.True(t, ok, "iterator returned %T at block %d", result, want)
		require.Equal(t, want, block.Header.Number, "iterator skipped or repeated a block")
		require.Equal(t, blocks[want], block, "block %d came back with the wrong bytes", want)
		delivered++
	}

	require.Positive(t, delivered, "the iterator must drain the block file it already holds open")
	require.Error(t, lastErr, "the iterator must fail rather than skip the removed files")
	require.ErrorIs(t, lastErr, ErrPruned, "the iterator must report why it stopped")

	_, retryErr := itr.Next()
	require.Equal(t, lastErr.Error(), retryErr.Error(), "a retry must report the same failure, not panic")

	itr.Close()
	requirePruneInvariant(t, mgr)
}

// Scenario:
//  1. Store 120 blocks spread over several block files, with the block cache disabled.
//  2. Open an iterator at block 20 but do not read from it, so it has not opened a stream yet.
//  3. Prune past block 20.
//  4. Expect the first read to report ErrPruned and return no block, rather than serve a block from a
//     removed file, even though availability was checked when the iterator was created.
func TestIteratorStartPrunedBeforeFirstReadReportsErrPruned(t *testing.T) {
	const numBlocks, start = 120, 20
	env, w, _ := newConcurrentTestLedger(t, numBlocks, numBlocks)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	itr, err := mgr.retrieveBlocks(start)
	require.NoError(t, err)
	defer itr.Close()

	require.NoError(t, mgr.pruneBefore(start+40))

	result, err := itr.Next()
	require.ErrorIs(t, err, ErrPruned)
	require.Nil(t, result)
	requirePruneInvariant(t, mgr)
}

// Scenario:
//  1. Store 60 blocks spread over several block files.
//  2. Call pruneBefore concurrently with bounds that arrive out of order, including bounds far below
//     the highest one.
//  3. Expect no call to fail, and the marker to settle at the highest bound rather than at whichever
//     call happened to finish last.
//  4. Expect blocks below the highest bound to report ErrPruned and the rest to be readable.
func TestPruneBeforeConcurrentCallsWithDifferentBounds(t *testing.T) {
	const numBlocks = 60
	env, w, blocks := newConcurrentTestLedger(t, numBlocks, numBlocks)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	bounds := []uint64{5, 45, 15, 50, 25, 50, 35, 10}
	var highestBound uint64
	for _, b := range bounds {
		highestBound = max(highestBound, b)
	}

	errs := make(chan error, len(bounds))
	var wg sync.WaitGroup
	for _, bound := range bounds {
		wg.Add(1)
		go func(bound uint64) {
			defer wg.Done()
			errs <- mgr.pruneBefore(bound)
		}(bound)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, highestBound, mgr.pruner.firstReadableBlockNum(),
		"a lower bound arriving last must not lower the marker")
	requirePruneInvariant(t, mgr)
	requireBlocksPruned(t, mgr, blocks, int(highestBound))
}

// Scenario:
//  1. Store 120 blocks spread over several block files, with the block cache disabled.
//  2. Resolve block 20 through the index while it is still available, which is what a read does before
//     it opens the block file.
//  3. Prune past block 20, deleting its index entry and removing the file the read just resolved.
//  4. Expect the fetch of that stale location to report ErrPruned, so a reader that lost the race learns
//     the block was pruned instead of that a block file is missing.
//  5. Expect the same of a raw byte read of that location, which is the path a read keyed by transaction
//     ID takes.
func TestReadLosingTheRaceWithPruneReportsErrPruned(t *testing.T) {
	const numBlocks, target = 120, 20
	env, w, _ := newConcurrentTestLedger(t, numBlocks, numBlocks)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.checkBlockAvailable(target))
	loc, err := mgr.index.getBlockLocByBlockNum(target)
	require.NoError(t, err)

	require.NoError(t, mgr.pruneBefore(target+40))

	_, fetchErr := mgr.fetchBlock(loc)
	require.ErrorIs(t, fetchErr, ErrPruned, "the removed block file must not still look readable")

	_, rawErr := mgr.fetchRawBytes(loc)
	require.ErrorIs(t, rawErr, ErrPruned)
}

// Scenario:
//  1. Store 60 blocks spread over several block files.
//  2. Close LevelDB so the prune batch cannot commit, then prune below block 40.
//  3. Expect the call to fail.
//  4. Expect the first readable block to have advanced anyway, since refusing blocks that are still on
//     disk is harmless.
//  5. Expect the lowest stored block file to be unchanged, because nothing was removed: claiming a
//     file is gone before the commit would let a later orphan sweep unlink a file the durable marker
//     still expects, leaving a store that cannot reopen.
func TestPruneBeforeCommitFailureDoesNotClaimFilesAreGone(t *testing.T) {
	env, w, _ := newConcurrentTestLedger(t, 60, 60)
	mgr := w.blockfileMgr

	before := mgr.pruner.marker.Load()
	filesBefore := blockfileNums(t, mgr.rootDir)

	env.provider.Close()

	require.Error(t, mgr.pruneBefore(40))

	after := mgr.pruner.marker.Load()
	require.Equal(t, uint64(40), after.firstReadableBlockNum)
	require.Equal(t, before.firstStoredBlockfileNum, after.firstStoredBlockfileNum)
	require.Equal(t, filesBefore, blockfileNums(t, mgr.rootDir))
}

// Scenario:
//  1. Store 120 blocks spread over several block files, with the block cache disabled.
//  2. Open an iterator at block 20 and read one block, so its stream is open on a block file.
//  3. Delete a block file ahead of the reader without pruning, so the marker still expects it.
//  4. Expect a fetch of a location inside that file to fail without being reported as pruning.
//  5. Expect the iterator, reading forward into it, to fail the same way, naming the file it could not
//     open, so that a caller can still tell a fault from a block that is gone for good.
func TestFailingOnAFileTheMarkerStillExpectsIsNotPruned(t *testing.T) {
	const numBlocks, start, ahead = 120, 20, 80
	env, w, _ := newConcurrentTestLedger(t, numBlocks, numBlocks)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	itr, err := mgr.retrieveBlocks(start)
	require.NoError(t, err)
	defer itr.Close()

	_, err = itr.Next()
	require.NoError(t, err)

	loc, err := mgr.index.getBlockLocByBlockNum(ahead)
	require.NoError(t, err)
	require.Greater(t, loc.fileSuffixNum, mgr.pruner.firstStoredBlockfileNum())
	require.NoError(t, os.Remove(deriveBlockfilePath(mgr.rootDir, loc.fileSuffixNum)))

	_, fetchErr := mgr.fetchBlock(loc)
	require.Error(t, fetchErr)
	require.NotErrorIs(t, fetchErr, ErrPruned)
	require.ErrorContains(t, fetchErr, "error opening block file")

	var stopErr error
	for i := start + 1; i < numBlocks && stopErr == nil; i++ {
		_, stopErr = itr.Next()
	}
	require.Error(t, stopErr)
	require.NotErrorIs(t, stopErr, ErrPruned)
	require.ErrorContains(t, stopErr, "error opening block file")
}
