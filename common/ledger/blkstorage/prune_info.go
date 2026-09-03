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
//
// They may diverge because pruning removes whole files only.
// Therefore, firstReadableBlockNum is at least the firstBlockNum in firstStoredBlockfileNum.
type pruneInfo struct {
	firstReadableBlockNum   uint64
	firstStoredBlockfileNum int
}

func (i *pruneInfo) marshal() []byte {
	var buf []byte
	buf = protowire.AppendVarint(buf, i.firstReadableBlockNum)
	buf = protowire.AppendVarint(buf, uint64(i.firstStoredBlockfileNum))
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
	i.firstStoredBlockfileNum = int(val)

	return nil
}

func (i *pruneInfo) String() string {
	return fmt.Sprintf("firstReadableBlockNum=[%d], firstStoredBlockfileNum=[%d]",
		i.firstReadableBlockNum, i.firstStoredBlockfileNum)
}
