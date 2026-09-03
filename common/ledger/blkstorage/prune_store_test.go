/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func pruneStoreDir(rootDir string) string {
	return filepath.Join(rootDir, pruneStoreFileSuffix)
}

func pruneInfoFilePath(rootDir string, seq uint64) string {
	return filepath.Join(pruneStoreDir(rootDir), baseName(pruneInfoPrefix, seq)+"."+pruneStoreFileSuffix)
}

// pruneStoreFileNames lets a test assert what a save left behind, not only what it can read back.
func pruneStoreFileNames(t *testing.T, rootDir string) []string {
	entries, err := os.ReadDir(pruneStoreDir(rootDir))
	require.NoError(t, err)

	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// Scenario:
// 1. Open a prune store over an empty ledger directory.
// 2. Expect its directory to exist and to hold no file.
// 3. Expect getInfo to report a zero-valued record rather than nil or an error.
func TestPruneStoreEmptyBeforeFirstSave(t *testing.T) {
	rootDir := t.TempDir()

	s, err := newPruneStore(rootDir)
	require.NoError(t, err)
	require.DirExists(t, pruneStoreDir(rootDir))
	require.Empty(t, pruneStoreFileNames(t, rootDir))

	info, err := s.getInfo()
	require.NoError(t, err)
	require.Equal(t, &pruneInfo{}, info)
}

// Scenario:
//  1. Open a prune store and save a record.
//  2. Expect it to be file 1, and getInfo to return it.
//  3. Save a second record and expect it to be file 2, with file 1 removed, so that one file remains.
//  4. Expect a store reopened over the same directory to return the second record.
func TestPruneStoreSaveAndGetInfo(t *testing.T) {
	rootDir := t.TempDir()

	s, err := newPruneStore(rootDir)
	require.NoError(t, err)

	first := &pruneInfo{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2}
	require.NoError(t, s.saveInfo(first))
	require.FileExists(t, pruneInfoFilePath(rootDir, 1))

	got, err := s.getInfo()
	require.NoError(t, err)
	require.Equal(t, first, got)

	second := &pruneInfo{firstReadableBlockNum: 35, firstStoredBlockfileNum: 3}
	require.NoError(t, s.saveInfo(second))
	require.FileExists(t, pruneInfoFilePath(rootDir, 2))
	require.NoFileExists(t, pruneInfoFilePath(rootDir, 1))
	require.Len(t, pruneStoreFileNames(t, rootDir), 1)

	got, err = s.getInfo()
	require.NoError(t, err)
	require.Equal(t, second, got)

	reopened, err := newPruneStore(rootDir)
	require.NoError(t, err)
	got, err = reopened.getInfo()
	require.NoError(t, err)
	require.Equal(t, second, got)

	// The reopened store continues the sequence rather than colliding with what is on disk.
	require.NoError(t, reopened.saveInfo(&pruneInfo{firstReadableBlockNum: 40, firstStoredBlockfileNum: 4}))
	require.FileExists(t, pruneInfoFilePath(rootDir, 3))
	require.Len(t, pruneStoreFileNames(t, rootDir), 1)
}

// A crash between saving a file and removing the one it supersedes leaves both on disk, the higher sequence
// number being the whole one.
//
// Scenario:
// 1. Place two prune info files, holding different records.
// 2. Open a prune store and expect getInfo to return the record of the higher one.
// 3. Expect the lower one to have been removed.
// 4. Expect the next save to continue from the higher one.
func TestPruneStoreReadsTheLatestFile(t *testing.T) {
	rootDir := t.TempDir()
	older := &pruneInfo{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2}
	newer := &pruneInfo{firstReadableBlockNum: 35, firstStoredBlockfileNum: 3}

	s, err := newPruneStore(rootDir)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pruneInfoFilePath(rootDir, 7), older.marshal(), 0o600))
	require.NoError(t, os.WriteFile(pruneInfoFilePath(rootDir, 8), newer.marshal(), 0o600))

	got, err := s.getInfo()
	require.NoError(t, err)
	require.Equal(t, newer, got)
	require.NoFileExists(t, pruneInfoFilePath(rootDir, 7))
	require.FileExists(t, pruneInfoFilePath(rootDir, 8))

	require.NoError(t, s.saveInfo(&pruneInfo{firstReadableBlockNum: 40, firstStoredBlockfileNum: 4}))
	require.FileExists(t, pruneInfoFilePath(rootDir, 9))
	require.Len(t, pruneStoreFileNames(t, rootDir), 1)
}

// Scenario:
// 1. Open a prune store and save a record.
// 2. Overwrite its file with a varint that claims more continuation bytes than are present.
// 3. Expect getInfo to fail with an unmarshalling error naming the file.
func TestPruneStoreGetInfoOnCorruptFile(t *testing.T) {
	rootDir := t.TempDir()

	s, err := newPruneStore(rootDir)
	require.NoError(t, err)
	require.NoError(t, s.saveInfo(&pruneInfo{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2}))

	require.NoError(t, os.WriteFile(pruneInfoFilePath(rootDir, 1), []byte{0xff}, 0o600))

	_, err = s.getInfo()
	require.ErrorContains(t, err, "error unmarshalling prune info")
	require.ErrorContains(t, err, baseName(pruneInfoPrefix, 1))
}

// Scenario:
//  1. Place a prune info file whose sequence number is not a number.
//  2. Expect getInfo to fail rather than ignore it, because a file that cannot be ordered may be the latest
//     one, and guessing the readable bound is worse than refusing to open the ledger.
//  3. Expect the file to still be there afterwards.
func TestPruneStoreGetInfoOnUnreadableSeq(t *testing.T) {
	rootDir := t.TempDir()

	s, err := newPruneStore(rootDir)
	require.NoError(t, err)

	strayPath := filepath.Join(pruneStoreDir(rootDir), pruneInfoPrefix+"latest."+pruneStoreFileSuffix)
	require.NoError(t, os.WriteFile(strayPath, []byte{0}, 0o600))

	_, err = s.getInfo()
	require.ErrorContains(t, err, "has no readable sequence number")
	require.FileExists(t, strayPath)
}

// The store is shared by base name, so that later kinds of retained state can live beside the prune info.
//
// Scenario:
// 1. Open a prune store and save a record.
// 2. Place a file of an unrelated base name in the same directory.
// 3. Expect getInfo to return the record and to leave that file alone.
func TestPruneStoreIgnoresOtherBaseNames(t *testing.T) {
	rootDir := t.TempDir()

	s, err := newPruneStore(rootDir)
	require.NoError(t, err)
	info := &pruneInfo{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2}
	require.NoError(t, s.saveInfo(info))

	otherPath := filepath.Join(pruneStoreDir(rootDir), "other-00000000000000000004."+pruneStoreFileSuffix)
	require.NoError(t, os.WriteFile(otherPath, []byte("unrelated"), 0o600))

	got, err := s.getInfo()
	require.NoError(t, err)
	require.Equal(t, info, got)
	require.FileExists(t, otherPath)
}
