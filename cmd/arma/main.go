/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"os"

	arma "github.com/hyperledger/fabric-x-orderer/node/server"
)

func main() {
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}
