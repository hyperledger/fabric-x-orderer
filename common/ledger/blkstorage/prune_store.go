/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/filerepo"

	"github.com/hyperledger/fabric-x-common/tools/fileutil"
	"github.com/pkg/errors"
)

const (
	// pruneStoreFileSuffix names both the prune store's directory under the ledger's root directory and the
	// suffix of the files in it.
	pruneStoreFileSuffix = "prune"
	// pruneInfoPrefix makes the prune info's first file prune/info-00000000000000000001.prune. The sequence
	// number is zero padded so that the directory reads in order.
	pruneInfoPrefix = "info-"
	// pruneConfigPrefix makes a retained config block's file prune/config-00000000000000000042.prune. Its
	// sequence number is the block's own number rather than a counter, so the file the prune info names can
	// be read straight back without searching, and superseded ones sort below it.
	pruneConfigPrefix = "config-"
	seqFormat         = "%020d"
)

// pruneStore keeps the state that has to outlive the blocks pruning removes. It sits beside the block files,
// in the ledger's own directory, rather than in the index database, because the index is disposable by
// contract: delete it and it is rebuilt from the block files. firstReadableBlockNum cannot be recovered that
// way, since pruning removes whole block files and the readable bound may fall inside the oldest surviving
// one.
//
// Its entry points take its own mutex, because reading the retained config block happens on the read path
// rather than under the prune operation's lock, and would otherwise race a prune opening the directory or
// superseding a file. The mutex covers only the store's own state and its file I/O; nothing above it is
// called while it is held.
type pruneStore struct {
	rootDir string
	// repo is nil until the store's directory is opened, which the first save does if it is absent.
	repo *filerepo.Repo

	// infoSeq is the sequence number of the file holding the durable prune info, or 0 if there is none.
	infoSeq uint64

	mutex sync.Mutex
}

// newPruneStore prepares the store without touching the disk, so that a ledger that is never pruned never
// gets a directory.
func newPruneStore(rootDir string) *pruneStore {
	return &pruneStore{rootDir: rootDir}
}

// getInfo reads the prune info from its latest file. A ledger that has never been pruned has no file, which
// yields a zero-valued pruneInfo rather than nil so that callers never have to nil-check.
func (s *pruneStore) getInfo() (*pruneInfo, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	exists, err := s.dirExists()
	if err != nil {
		return nil, err
	}
	if !exists {
		return &pruneInfo{}, nil
	}
	if err := s.open(); err != nil {
		return nil, err
	}

	seqs, err := s.seqs(pruneInfoPrefix)
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		return &pruneInfo{}, nil
	}

	latest := seqs[len(seqs)-1]
	name := baseName(pruneInfoPrefix, latest)

	b, err := s.repo.Read(name)
	if err != nil {
		return nil, errors.WithMessagef(err, "error reading prune info [%s]", name)
	}
	info := &pruneInfo{}
	if err := info.unmarshal(b); err != nil {
		return nil, errors.WithMessagef(err, "error unmarshalling prune info [%s]", name)
	}

	// Only now that the latest file has been read: a failure above leaves every file in place for whoever
	// has to work out why the ledger will not open.
	s.infoSeq = latest
	s.drop(pruneInfoPrefix, seqs[:len(seqs)-1])

	logger.Debugf("loaded prune info from [%s]: %s", name, info)
	return info, nil
}

// saveInfo persists the prune info as the next file, and returns once both the content and the directory
// entry are on disk. The file it supersedes is removed afterwards.
func (s *pruneStore) saveInfo(info *pruneInfo) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.open(); err != nil {
		return err
	}

	next := s.infoSeq + 1

	// A base name is written once, so filerepo refusing to overwrite is a check rather than an obstacle: it
	// can only fire here if the sequence went backwards.
	if err := s.repo.Save(baseName(pruneInfoPrefix, next), info.marshal()); err != nil {
		return errors.WithMessagef(err, "error saving prune info %s", info)
	}

	superseded := s.infoSeq
	s.infoSeq = next
	if superseded != 0 {
		s.drop(pruneInfoPrefix, []uint64{superseded})
	}
	return nil
}

// saveConfigBlock stores one config block's bytes under its own block number, and returns once they are on
// disk. It is called before the prune info that names the block, so that the info never points at bytes that
// are not there.
//
// A file already under that name is left alone rather than treated as a conflict: base names are written
// once, and this one is derived from the block, so an existing file is the same block's bytes written by a
// prune that did not get as far as recording them. Retrying that prune must not fail on its own progress.
func (s *pruneStore) saveConfigBlock(blockNum uint64, blockBytes []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.open(); err != nil {
		return err
	}

	name := baseName(pruneConfigPrefix, blockNum)
	if err := s.repo.Save(name, blockBytes); err != nil && !errors.Is(err, os.ErrExist) {
		return errors.WithMessagef(err, "error saving retained config block [%s]", name)
	}
	return nil
}

// getConfigBlock reads back the bytes of the retained config block with the given number. The caller has the
// number from the prune info, so a missing file is a broken store rather than an ordinary absence.
func (s *pruneStore) getConfigBlock(blockNum uint64) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.open(); err != nil {
		return nil, err
	}

	name := baseName(pruneConfigPrefix, blockNum)
	b, err := s.repo.Read(name)
	if err != nil {
		return nil, errors.WithMessagef(err, "error reading retained config block [%s]", name)
	}
	return b, nil
}

// dropConfigBlocksBelow removes the config blocks a newer retained one has superseded. Like drop, it only
// tidies up, and it must run after the prune info naming the survivor is durable: until then the info still
// names one of the files it would remove. Nothing is below block 0, so a ledger retaining no config block at
// all -- a batcher's, whose blocks are batches -- has nothing here to supersede either.
func (s *pruneStore) dropConfigBlocksBelow(blockNum uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.repo == nil {
		return
	}

	seqs, err := s.seqs(pruneConfigPrefix)
	if err != nil {
		logger.Warnf("Could not list the retained config blocks to supersede: %s", err)
		return
	}

	superseded := make([]uint64, 0, len(seqs))
	for _, seq := range seqs {
		if seq >= blockNum {
			break // ascending, so nothing lower remains
		}
		superseded = append(superseded, seq)
	}
	s.drop(pruneConfigPrefix, superseded)
}

// open opens the store's directory, creating it if absent, and clears any transient file left mid-save. It
// is idempotent, so every entry point that needs the directory can call it. Like the other helpers below, it
// runs under the caller's lock and takes none of its own.
func (s *pruneStore) open() error {
	if s.repo != nil {
		return nil
	}

	repo, err := filerepo.New(s.rootDir, pruneStoreFileSuffix)
	if err != nil {
		return errors.WithMessagef(err, "error opening prune store under [%s]", s.rootDir)
	}
	s.repo = repo
	return nil
}

// dirExists reports whether the store has a directory yet; a ledger never pruned has none.
func (s *pruneStore) dirExists() (bool, error) {
	exists, err := fileutil.DirExists(filepath.Join(s.rootDir, pruneStoreFileSuffix))
	if err != nil {
		return false, errors.WithMessagef(err, "error checking for a prune store under [%s]", s.rootDir)
	}
	return exists, nil
}

// seqs lists the sequence numbers of the store's files carrying the given prefix, lowest first. Files of
// another prefix are ignored, so that unrelated base names can share the store.
func (s *pruneStore) seqs(prefix string) ([]uint64, error) {
	fileNames, err := s.repo.List()
	if err != nil {
		return nil, errors.WithMessage(err, "error listing prune store")
	}

	var seqs []uint64
	for _, fileName := range fileNames {
		digits, hasPrefix := strings.CutPrefix(s.repo.FileToBaseName(fileName), prefix)
		if !hasPrefix {
			continue
		}
		// A file that cannot be ordered may be the latest one, and guessing the readable bound is worse
		// than refusing to open the ledger.
		seq, err := strconv.ParseUint(digits, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "prune store file [%s] has no readable sequence number", fileName)
		}
		seqs = append(seqs, seq)
	}

	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

// drop removes superseded files. It only tidies up: the file that replaced them is already durable, so one
// that will not go away is reported and left for a later read to retry.
func (s *pruneStore) drop(prefix string, seqs []uint64) {
	for _, seq := range seqs {
		name := baseName(prefix, seq)
		if err := s.repo.Remove(name); err != nil {
			logger.Warnf("Could not remove superseded prune store file [%s]: %s", name, err)
		}
	}
}

func baseName(prefix string, seq uint64) string {
	return prefix + fmt.Sprintf(seqFormat, seq)
}
