package main

import (
	"os"

	arma "github.ibm.com/decentralized-trust-research/arma/node/server"
)

func main() {
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}
