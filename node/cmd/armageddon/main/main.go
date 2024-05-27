package main

import (
	"os"

	"github.ibm.com/Yacov-Manevich/ARMA/node/cmd/armageddon"
)

func main() {
	cli := armageddon.NewCLI()
	cli.Run(os.Args[1:])
}
