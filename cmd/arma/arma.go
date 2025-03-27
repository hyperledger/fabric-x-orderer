package arma

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"github.ibm.com/decentralized-trust-research/arma/config"
	"github.ibm.com/decentralized-trust-research/arma/node"
	"github.ibm.com/decentralized-trust-research/arma/node/assembler"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/router"
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
		"assembler": launchAssembler(cli.stop, loadConfigAndGenesis),
		"batcher":   launchBatcher(cli.stop),
		"consensus": launchConsensus(cli.stop, loadConfigAndGenesis),
	} {
		cli.Command(name, help[name], f)
	}
}

func launchAssembler(
	stop chan struct{},
	loadConfigAndGenesis func(configFile *os.File) (*config.Configuration, *common.Block),
) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent, genesisBlock := loadConfigAndGenesis(configFile)
		conf := configContent.ExtractAssemblerConfig()

		var assemblerLogger *flogging.FabricLogger
		if testLogger != nil {
			assemblerLogger = testLogger
		} else {
			assemblerLogger = flogging.MustGetLogger(fmt.Sprintf("Assembler%d", conf.PartyId))
		}
		assembler := assembler.NewAssembler(conf, genesisBlock, assemblerLogger)

		srv := node.CreateGRPCAssembler(conf)

		orderer.RegisterAtomicBroadcastServer(srv.Server(), assembler)

		go func() {
			_ = srv.Start()
			close(stop)
		}()

		assemblerLogger.Infof("Assembler listening on %s", srv.Address())
	}
}

func launchConsensus(
	stop chan struct{},
	loadConfigAndGenesis func(configFile *os.File) (*config.Configuration, *common.Block),
) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent, genesisBlock := loadConfigAndGenesis(configFile)
		conf := configContent.ExtractConsenterConfig()

		var consenterLogger *flogging.FabricLogger
		if testLogger != nil {
			consenterLogger = testLogger
		} else {
			consenterLogger = flogging.MustGetLogger(fmt.Sprintf("Consensus%d", conf.PartyId))
		}

		consensus := consensus.CreateConsensus(conf, genesisBlock, consenterLogger)

		defer consensus.Start()

		srv := node.CreateGRPCConsensus(conf)

		protos.RegisterConsensusServer(srv.Server(), consensus)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), consensus.DeliverService)
		orderer.RegisterClusterNodeServiceServer(srv.Server(), consensus)

		go func() {
			srv.Start()
			close(stop)
		}()

		consenterLogger.Infof("Consensus listening on %s", srv.Address())
	}
}

func launchBatcher(stop chan struct{}) func(configFile *os.File) {
	return func(configFile *os.File) {
		config, err := config.ReadConfig(configFile.Name())
		if err != nil {
			panic(fmt.Sprintf("error launching batcher, err: %s", err))
		}
		conf := config.ExtractBatcherConfig()

		var batcherLogger *flogging.FabricLogger
		if testLogger != nil {
			batcherLogger = testLogger
		} else {
			batcherLogger = flogging.MustGetLogger(fmt.Sprintf("Batcher%dShard%d", conf.PartyId, conf.ShardId))
		}

		batcher := batcher.CreateBatcher(conf, batcherLogger, nil, &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{})
		defer batcher.Run()

		srv := node.CreateGRPCBatcher(conf)

		protos.RegisterRequestTransmitServer(srv.Server(), batcher)
		protos.RegisterBatcherControlServiceServer(srv.Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), batcher)

		go func() {
			srv.Start()
			close(stop)
		}()

		batcherLogger.Infof("Batcher listening on %s", srv.Address())
	}
}

func launchRouter(stop chan struct{}) func(configFile *os.File) {
	return func(configFile *os.File) {
		config, err := config.ReadConfig(configFile.Name())
		if err != nil {
			panic(fmt.Sprintf("error launching router, err: %s", err))
		}
		conf := config.ExtractRouterConfig()

		var routerLogger *flogging.FabricLogger
		if testLogger != nil {
			routerLogger = testLogger
		} else {
			routerLogger = flogging.MustGetLogger(fmt.Sprintf("Router%d", conf.PartyID))
		}
		r := router.NewRouter(conf, routerLogger)
		ch := r.StartRouterService()

		go func() {
			<-ch
			close(stop)
		}()

		routerLogger.Infof("Router listening on %s, PartyID: %d", r.Address(), conf.PartyID)
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

func loadConfigAndGenesis(configFile *os.File) (*config.Configuration, *common.Block) {
	absConfigFileName, err := filepath.Abs(configFile.Name())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed extracting absolute path from file %s: %v \n", configFile.Name(), err)
		os.Exit(2)
	}
	config, err := config.ReadConfig(absConfigFileName)
	if err != nil {
		panic(fmt.Sprintf("error reading local config, err: %s", err))
	}

	configPath, _ := filepath.Split(absConfigFileName)
	blockFileName := path.Join(configPath, "genesis.block")
	statBlock, err := os.Stat(blockFileName)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stdout, "genesis block file does not exist in config path: %s (ignoring) \n", configPath)
			return config, nil
		} else {
			fmt.Fprintf(os.Stderr, "failed stat file %s: %v \n", blockFileName, err)
			os.Exit(2)
		}
	}

	blockFile, err := os.Open(blockFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can not open genesis block file from: %s (ignoring): %v \n", blockFileName, err)
		return config, nil
	}

	genesisBlockBytes := make([]byte, statBlock.Size())
	_, err = io.ReadFull(blockFile, genesisBlockBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed reading file %s: %v \n", blockFile.Name(), err)
		os.Exit(2)
	}

	block, err := protoutil.UnmarshalBlock(genesisBlockBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed unmarshalling block %s: %v \n", blockFile.Name(), err)
		os.Exit(2)
	}

	return config, block
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
