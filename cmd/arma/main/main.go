package main

import (
	"os"

	"arma/cmd/arma"
)

func main() {
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}
