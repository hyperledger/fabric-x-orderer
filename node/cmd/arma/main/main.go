package main

import (
	"github.ibm.com/Yacov-Manevich/ARMA/node/cmd/arma"
	"google.golang.org/grpc/grpclog"
	"os"
)

func main() {
	grpclog.SetLoggerV2(&silentLogger{})
	cli := arma.NewCLI()
	<-cli.Run(os.Args[1:])
}

type silentLogger struct {
}

func (s *silentLogger) Info(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Infoln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Infof(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) Warning(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Warningln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Warningf(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) Error(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Errorln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Errorf(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) Fatal(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Fatalln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Fatalf(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) V(l int) bool {
	return false

}
