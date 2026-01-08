/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"math"
)

// ComputeFTQ computes the F, Threshold and Quorum corresponds to N.
func ComputeFTQ(n uint16) (uint16, uint16, uint16) {
	f := (n - 1) / 3
	threshold := f + 1
	quorum := uint16(math.Ceil((float64(n) + float64(f) + 1) / 2.0))
	return f, threshold, quorum
}
