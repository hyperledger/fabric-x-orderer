/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"bytes"
	"fmt"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type tHelper = interface {
	Helper()
}

// RequireProtoEqual verifies that two proto are equal.
func RequireProtoEqual(t require.TestingT, expected, actual proto.Message) {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}
	require.Truef(
		t,
		proto.Equal(expected, actual),
		"EXPECTED: %v\nACTUAL: %v",
		protojson.Format(expected),
		protojson.Format(actual),
	)
}

// RequireProtoElementsMatch verifies that two arrays of proto have the same elements.
func RequireProtoElementsMatch[T proto.Message](
	t require.TestingT, expected, actual []T, msgAndArgs ...any,
) {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}
	if expected == nil {
		require.Nil(t, actual, msgAndArgs...)
		return
	}
	if msg, haveDiff := protoElemDiff(expected, actual); haveDiff {
		require.Fail(t, msg, msgAndArgs...)
	}
}

func protoElemDiff[T proto.Message](expected, actual []T) (string, bool) { //nolint:gocognit // cognitive complexity 23
	var msg bytes.Buffer
	if len(expected) != len(actual) {
		msg.WriteString(fmt.Sprintf("\nSIZE MISMATCH: expected=%d != actual=%d\n", len(expected), len(actual)))
	}
	expectedMatched := make([]bool, len(expected))
	actualMatched := make([]bool, len(actual))
	for aInd, aElem := range actual {
		for eInd, eElem := range expected {
			if expectedMatched[eInd] {
				continue
			}
			if proto.Equal(eElem, aElem) {
				expectedMatched[eInd] = true
				actualMatched[aInd] = true
				break
			}
		}
	}

	var missing []string
	for eInd, eElem := range expected {
		if !expectedMatched[eInd] {
			missing = append(missing, protojson.Format(eElem))
		}
	}
	var extra []string
	for aInd, aElem := range actual {
		if !actualMatched[aInd] {
			extra = append(extra, protojson.Format(aElem))
		}
	}

	if len(extra) == 0 && len(missing) == 0 {
		return "", false
	}

	if len(extra) > 0 {
		msg.WriteString(fmt.Sprintf("\nEXTRA ELEMENTS (%d):\n\n", len(extra)))
		for _, extraElem := range extra {
			msg.WriteString(extraElem)
			msg.WriteString("\n")
		}
	}
	if len(missing) > 0 {
		msg.WriteString(fmt.Sprintf("\n\nMISSING ELEMENTS (%d):\n\n", len(missing)))
		for _, missingElem := range missing {
			msg.WriteString(missingElem)
			msg.WriteString("\n")
		}
	}
	return msg.String(), true
}
