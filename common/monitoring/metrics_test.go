/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Scenario:
// 1. nothing was added during the interval: the average is 0
// 2. the first interval, starting from zero: the average is the sum divided by the count
// 3. a later interval: only what was added during the interval counts, not the totals since the start
func TestIntervalAverage(t *testing.T) {
	for _, tst := range []struct {
		name                                           string
		sumAtStart, sumAtEnd, countAtStart, countAtEnd uint64
		expectedAverage                                uint64
	}{
		{
			name:       "nothing added",
			sumAtStart: 1000, sumAtEnd: 1000, countAtStart: 2, countAtEnd: 2,
			expectedAverage: 0,
		},
		{
			name:       "first interval",
			sumAtStart: 0, sumAtEnd: 900, countAtStart: 0, countAtEnd: 3,
			expectedAverage: 300,
		},
		{
			// 2000 bytes in 2 items were added during the interval; the totals since the start would give 3000 / 4 = 750
			name:       "only the interval counts",
			sumAtStart: 1000, sumAtEnd: 3000, countAtStart: 2, countAtEnd: 4,
			expectedAverage: 1000,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			require.Equal(t, tst.expectedAverage, IntervalAverage(tst.sumAtStart, tst.sumAtEnd, tst.countAtStart, tst.countAtEnd))
		})
	}
}
