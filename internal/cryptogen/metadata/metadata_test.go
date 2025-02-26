/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package metadata_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/internal/cryptogen/metadata"

	"github.com/stretchr/testify/require"
)

// This code originates from hyperledger fabric project. Source: https://github.com/hyperledger/fabric/blob/main/internal/cryptogen/metadata/metadata_test.go

func TestGetVersionInfo(t *testing.T) {
	expected := fmt.Sprintf(
		"%s:\n Version: %s\n Commit SHA: %s\n Go version: %s\n OS/Arch: %s",
		metadata.ProgramName,
		metadata.Version,
		"development build",
		runtime.Version(),
		fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	)
	require.Equal(t, expected, metadata.GetVersionInfo())

	testSHA := "abcdefg"
	metadata.CommitSHA = testSHA
	expected = fmt.Sprintf(
		"%s:\n Version: %s\n Commit SHA: %s\n Go version: %s\n OS/Arch: %s",
		metadata.ProgramName,
		metadata.Version,
		testSHA,
		runtime.Version(),
		fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	)
	require.Equal(t, expected, metadata.GetVersionInfo())
}
