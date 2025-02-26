package main

import (
	"os"

	"github.ibm.com/decentralized-trust-research/arma/cmd/arma"
)

func main() {
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}
