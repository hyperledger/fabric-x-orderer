/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package applicationpb

// NewVersion is a convenient method to create a version pointer in a single line.
func NewVersion(version uint64) *uint64 {
	return &version
}
