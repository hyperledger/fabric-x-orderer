/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package metadata

import (
	"fmt"
	"runtime"

	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata-vars"
)

// This code originates from hyperledger fabric project. Source: https://github.com/hyperledger/fabric/blob/main/internal/cryptogen/metadata/metadata.go

const ProgramName = "cryptogen"

var (
	CommitSHA = metadata.CommitSHA
	Version   = metadata.Version
)

func GetVersionInfo() string {
	return fmt.Sprintf(
		"%s:\n Version: %s\n Commit SHA: %s\n Go version: %s\n OS/Arch: %s",
		ProgramName,
		Version,
		CommitSHA,
		runtime.Version(),
		fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	)
}
