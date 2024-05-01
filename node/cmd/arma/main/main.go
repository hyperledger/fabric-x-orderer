package main

import (
	"github.ibm.com/Yacov-Manevich/ARMA/node/cmd/arma"
	"os"
)

func main() {
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}
