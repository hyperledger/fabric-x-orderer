/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeFTQ(t *testing.T) {
	type quorum struct {
		N uint16
		F uint16
		T uint16
		Q uint16
	}

	quorums := []quorum{
		{1, 0, 1, 1},
		{2, 0, 1, 2},
		{3, 0, 1, 2},
		{4, 1, 2, 3},
		{5, 1, 2, 4},
		{6, 1, 2, 4},
		{7, 2, 3, 5},
		{8, 2, 3, 6},
		{9, 2, 3, 6},
		{10, 3, 4, 7},
		{11, 3, 4, 8},
		{12, 3, 4, 8},
	}

	for _, testCase := range quorums {
		t.Run(fmt.Sprintf("%d nodes", testCase.N), func(t *testing.T) {
			F, T, Q := ComputeFTQ(testCase.N)
			assert.Equal(t, testCase.F, F)
			assert.Equal(t, testCase.T, T)
			assert.Equal(t, testCase.Q, Q)
		})
	}
}
