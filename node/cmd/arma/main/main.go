package main

import (
	"os"

	"arma/node/cmd/arma"
)

func main() {
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}
