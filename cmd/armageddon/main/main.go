package main

import (
	"os"

	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
)

func main() {
	cli := armageddon.NewCLI()
	cli.Run(os.Args[1:])
}
