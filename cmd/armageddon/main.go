/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"os"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
)

func main() {
	cli := armageddon.NewCLI()
	cli.Run(os.Args[1:])
}
