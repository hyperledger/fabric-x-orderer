/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package arma

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/msp"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	"google.golang.org/grpc/grpclog"
	"gopkg.in/alecthomas/kingpin.v2"
)

func init() {
	grpclog.SetLoggerV2(&silentLogger{})
}

var help = map[string]string{
	"router":    "run a router node",
	"assembler": "run an assembler node",
	"batcher":   "run a batcher node",
	"consensus": "run a consensus node",
}

var testLogger *flogging.FabricLogger

type CLI struct {
	app         *kingpin.Application
	dispatchers map[string]func(configFile *os.File)
	stop        chan struct{}
}

func (cli *CLI) Command(name, help string, onCmd func(configFile *os.File)) {
	cli.app.Command(name, help)
	cli.dispatchers[name] = onCmd
}

func (cli *CLI) configureNodesCommands() {
	for name, f := range map[string]func(configFile *os.File){
		"router":    launchRouter(cli.stop),
		"assembler": launchAssembler(cli.stop),
		"batcher":   launchBatcher(cli.stop),
		"consensus": launchConsensus(cli.stop),
	} {
		cli.Command(name, help[name], f)
	}
}

type NodeStopper interface {
	Stop()
}

func stopSignalListen(node NodeStopper, logger *flogging.FabricLogger, nodeAddr string) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	go func() {
		<-signalChan

		logger.Infof("SIGTERM signal caught, the node listening on %s is about to shutdown:", nodeAddr)
		node.Stop()
	}()
}

func launchAssembler(stop chan struct{}) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent, lastConfigBlock, err := config.ReadConfig(configFile.Name(), flogging.MustGetLogger("ReadConfigAssembler"))
		if err != nil {
			panic(fmt.Sprintf("error launching assembler, err: %s", err))
		}
		conf := configContent.ExtractAssemblerConfig()

		var assemblerLogger *flogging.FabricLogger
		if testLogger != nil {
			assemblerLogger = testLogger
		} else {
			assemblerLogger = flogging.MustGetLogger(fmt.Sprintf("Assembler%d", conf.PartyId))
		}

		srv := node.CreateGRPCAssembler(conf)
		assembler := assembler.NewAssembler(conf, srv, lastConfigBlock, assemblerLogger)

		orderer.RegisterAtomicBroadcastServer(srv.Server(), assembler)

		go func() {
			_ = srv.Start()
			close(stop)
		}()

		stopSignalListen(assembler, assemblerLogger, srv.Address())

		assemblerLogger.Infof("Assembler listening on %s", srv.Address())
	}
}

func launchConsensus(stop chan struct{}) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent, genesisBlock, err := config.ReadConfig(configFile.Name(), flogging.MustGetLogger("ReadConfigConsensus"))
		if err != nil {
			panic(fmt.Sprintf("error launching consensus, err: %s", err))
		}

		conf := configContent.ExtractConsenterConfig()

		localmsp := msp.BuildLocalMSP(configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP)
		signer, err := localmsp.GetDefaultSigningIdentity()
		if err != nil {
			panic(fmt.Sprintf("Failed to get local MSP identity: %s", err))
		}

		var consenterLogger *flogging.FabricLogger
		if testLogger != nil {
			consenterLogger = testLogger
		} else {
			consenterLogger = flogging.MustGetLogger(fmt.Sprintf("Consensus%d", conf.PartyId))
		}

		srv := node.CreateGRPCConsensus(conf)
		consensus := consensus.CreateConsensus(conf, srv, genesisBlock, consenterLogger, signer)

		defer consensus.Start()

		protos.RegisterConsensusServer(srv.Server(), consensus)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), consensus.DeliverService)
		orderer.RegisterClusterNodeServiceServer(srv.Server(), consensus)

		go func() {
			srv.Start()
			close(stop)
		}()

		stopSignalListen(consensus, consenterLogger, srv.Address())

		consenterLogger.Infof("Consensus listening on %s", srv.Address())
	}
}

func launchBatcher(stop chan struct{}) func(configFile *os.File) {
	return func(configFile *os.File) {
		config, lastConfigBlock, err := config.ReadConfig(configFile.Name(), flogging.MustGetLogger("ReadConfigBatcher"))
		if err != nil {
			panic(fmt.Sprintf("error launching batcher, err: %s", err))
		}

		conf := config.ExtractBatcherConfig(lastConfigBlock)

		localmsp := msp.BuildLocalMSP(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID, config.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP)
		signer, err := localmsp.GetDefaultSigningIdentity()
		if err != nil {
			panic(fmt.Sprintf("Failed to get local MSP identity: %s", err))
		}

		var batcherLogger *flogging.FabricLogger
		if testLogger != nil {
			batcherLogger = testLogger
		} else {
			batcherLogger = flogging.MustGetLogger(fmt.Sprintf("Batcher%dShard%d", conf.PartyId, conf.ShardId))
		}

		srv := node.CreateGRPCBatcher(conf)

		batcher := batcher.CreateBatcher(conf, batcherLogger, srv, &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{}, signer)
		defer batcher.Run()

		protos.RegisterRequestTransmitServer(srv.Server(), batcher)
		protos.RegisterBatcherControlServiceServer(srv.Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), batcher)

		go func() {
			srv.Start()
			close(stop)
		}()

		stopSignalListen(batcher, batcherLogger, srv.Address())

		batcherLogger.Infof("Batcher listening on %s", srv.Address())
	}
}

func launchRouter(stop chan struct{}) func(configFile *os.File) {
	return func(configFile *os.File) {
		conf, lastConfigBlock, err := config.ReadConfig(configFile.Name(), flogging.MustGetLogger("ReadConfigRouter"))
		if err != nil {
			panic(fmt.Sprintf("error launching router, err: %s", err))
		}

		routerConf := conf.ExtractRouterConfig(lastConfigBlock)

		var routerLogger *flogging.FabricLogger
		if testLogger != nil {
			routerLogger = testLogger
		} else {
			routerLogger = flogging.MustGetLogger(fmt.Sprintf("Router%d", routerConf.PartyID))
		}
		r := router.NewRouter(routerConf, routerLogger)
		ch := r.StartRouterService()

		go func() {
			<-ch
			close(stop)
		}()

		stopSignalListen(r, routerLogger, r.Address())
		routerLogger.Infof("Router listening on %s, PartyID: %d", r.Address(), routerConf.PartyID)
	}
}

func (cli *CLI) Run(args []string) <-chan struct{} {
	configFile := cli.app.Flag("config", "Specifies the config file to load the configuration from").File()
	command := kingpin.MustParse(cli.app.Parse(args))
	f, exists := cli.dispatchers[command]
	if !exists {
		fmt.Fprintf(os.Stderr, "command %s doesn't exist \n", command)
		os.Exit(2)
	}
	if *configFile == nil {
		fmt.Fprintf(os.Stderr, "config parameter missing \n")
		os.Exit(2)
	}
	f(*configFile)

	return cli.stop
}

func NewCLI() *CLI {
	app := kingpin.New("Arma", "Launches an Arma node (Router | Assembler | Batcher | Consensus)")
	cli := &CLI{
		app:         app,
		dispatchers: make(map[string]func(configFile *os.File)),
		stop:        make(chan struct{}),
	}
	cli.configureNodesCommands()
	return cli
}

type silentLogger struct{}

func (s *silentLogger) Info(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Infoln(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Infof(format string, args ...any) {
	// TODO implement me
}

func (s *silentLogger) Warning(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Warningln(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Warningf(format string, args ...any) {
	// TODO implement me
}

func (s *silentLogger) Error(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Errorln(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Errorf(format string, args ...any) {
	// TODO implement me
}

func (s *silentLogger) Fatal(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Fatalln(args ...any) {
	// TODO implement me
}

func (s *silentLogger) Fatalf(format string, args ...any) {
	// TODO implement me
}

func (s *silentLogger) V(l int) bool {
	return false
}
