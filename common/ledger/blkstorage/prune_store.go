/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-x-orderer/common/filerepo"

	"github.com/pkg/errors"
)

const (
	// pruneStoreFileSuffix names both the prune store's directory under the ledger's root directory and the
	// suffix of the files in it.
	pruneStoreFileSuffix = "prune"
	// pruneInfoPrefix makes the prune info's first file prune/info-00000000000000000001.prune. The sequence
	// number is zero padded so that the directory reads in order.
	pruneInfoPrefix = "info-"
	seqFormat       = "%020d"
)

// pruneStore keeps the state that has to outlive the blocks pruning removes. It sits beside the block files,
// in the ledger's own directory, rather than in the index database, because the index is disposable by
// contract: delete it and it is rebuilt from the block files. firstReadableBlockNum cannot be recovered that
// way, since pruning removes whole block files and the readable bound may fall inside the oldest surviving
// one.
//
// Each file is written once, under the next sequence number of its base name, and the file it supersedes is
// removed afterwards. Nothing is overwritten, so the highest sequence number of a base name is always a whole
// file; a crash between the two steps leaves two behind, which the next read resolves in favour of the higher.
//
// It is not safe for concurrent use; the prune operation above it serializes access.
type pruneStore struct {
	repo *filerepo.Repo

	// infoSeq is the sequence number of the file holding the durable prune info, or 0 if there is none.
	infoSeq uint64
}

func newPruneStore(rootDir string) (*pruneStore, error) {
	repo, err := filerepo.New(rootDir, pruneStoreFileSuffix)
	if err != nil {
		return nil, errors.WithMessagef(err, "error opening prune store under [%s]", rootDir)
	}
	return &pruneStore{repo: repo}, nil
}

// getInfo reads the prune info from its latest file. A ledger that has never been pruned has no file, which
// yields a zero-valued pruneInfo rather than nil so that callers never have to nil-check.
func (s *pruneStore) getInfo() (*pruneInfo, error) {
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
