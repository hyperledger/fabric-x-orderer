package main

import (
	"os"

	"arma/cmd/armageddon"
)

func main() {
	cli := armageddon.NewCLI()
	cli.Run(os.Args[1:])
}
