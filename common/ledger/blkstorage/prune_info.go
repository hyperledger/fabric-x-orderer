/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// pruneInfo records how far the front of the ledger has been pruned. Absent (or zero) means nothing has
// been pruned and the ledger still starts at block 0.
//
// Its two fields answer different questions and move independently:
//
//   - firstReadableBlockNum is a logical bound. It is what a caller asked to prune below, so reads under
//     it are refused whether or not the bytes happen to still be on disk.
//   - firstStoredBlockfileNum is a physical fact: the lowest block file still present. Recovery scans start
//     there, and the orphan sweep bounds itself by it.
//   - retainedConfigBlockNum names the one block below the bound that stays readable, held as a copy in the
//     prune store because a node resuming from this ledger has to read its config block before it can read
//     anything else. It is meaningful only when hasRetainedConfigBlock is set: block 0 is the config block
//     of a ledger that has never been reconfigured, so it cannot double as "none retained".
//
// The first two may diverge because pruning removes whole files only.
// Therefore, firstReadableBlockNum is at least the firstBlockNum in firstStoredBlockfileNum.
type pruneInfo struct {
	firstReadableBlockNum   uint64
	firstStoredBlockfileNum int
	retainedConfigBlockNum  uint64
	hasRetainedConfigBlock  bool
}

// marshal writes the fields as a varint sequence. The retained block's number comes before the flag that
// makes it meaningful, so that a record truncated between them reads as nothing retained rather than as
// block 0 retained: forgetting a retained block closes a hole in the bound, whereas inventing one opens a
// read that has nothing behind it.
func (i *pruneInfo) marshal() []byte {
	var buf []byte
	buf = protowire.AppendVarint(buf, i.firstReadableBlockNum)
	buf = protowire.AppendVarint(buf, uint64(i.firstStoredBlockfileNum))
	buf = protowire.AppendVarint(buf, i.retainedConfigBlockNum)
	buf = protowire.AppendVarint(buf, boolToVarint(i.hasRetainedConfigBlock))
	return buf
}

// unmarshal decodes the fields in the order marshal writes them. A field missing from the end of the record
// reads as zero, so that a field appended by a later version is absent rather than fatal when an older
// version reads the record back. A field that is present but truncated is still an error: ConsumeVarint
// reports a varint whose continuation bytes are missing.
func (i *pruneInfo) unmarshal(b []byte) error {
	var position int

	val, n := protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	i.firstReadableBlockNum = val

	if position == len(b) {
		return nil
	}

	val, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	i.firstStoredBlockfileNum = int(val)

	if position == len(b) {
		return nil
	}

	val, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	i.retainedConfigBlockNum = val

	if position == len(b) {
		return nil
	}

	val, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	i.hasRetainedConfigBlock = val != 0

	return nil
}

// retainedConfig is what the prune info records about the config block kept below the bound, carried as one
// value so that the number and the flag that makes it meaningful cannot be threaded apart.
type retainedConfig struct {
	blockNum uint64
	present  bool
}

func (i *pruneInfo) retainedConfig() retainedConfig {
	return retainedConfig{blockNum: i.retainedConfigBlockNum, present: i.hasRetainedConfigBlock}
}

// retains reports whether blockNum is the config block held in the prune store.
func (i *pruneInfo) retains(blockNum uint64) bool {
	return i.hasRetainedConfigBlock && i.retainedConfigBlockNum == blockNum
}

func (i *pruneInfo) String() string {
	return fmt.Sprintf(
		"firstReadableBlockNum=[%d], firstStoredBlockfileNum=[%d], retainedConfigBlockNum=[%d], "+
			"hasRetainedConfigBlock=[%t]",
		i.firstReadableBlockNum, i.firstStoredBlockfileNum, i.retainedConfigBlockNum, i.hasRetainedConfigBlock,
	)
}

func boolToVarint(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
