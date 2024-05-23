package main

import (
	"github.ibm.com/Yacov-Manevich/ARMA/node/cmd/armageddon"
	"os"
)

func main() {
	cli := armageddon.NewCLI()
	cli.Run(os.Args[1:])
}
