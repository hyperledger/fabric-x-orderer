package main

import (
	"os"

	"github.ibm.com/decentralized-trust-research/arma/common/tools/armageddon"
)

func main() {
	cli := armageddon.NewCLI()
	cli.Run(os.Args[1:])
}
