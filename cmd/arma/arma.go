package arma

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"arma/common/types"
	"arma/node"
	"arma/node/assembler"
	"arma/node/batcher"
	"arma/node/config"
	"arma/node/consensus"
	protos "arma/node/protos/comm"
	"arma/node/router"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc/grpclog"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"
)

func init() {
	// flogging.ActivateSpec("error")
	grpclog.SetLoggerV2(&silentLogger{})
}

var DirOverride = os.Getenv(DirOverrideEnvName)

const (
	DirOverrideEnvName = "DIRECTORY"
)

var (
	help = map[string]string{
		"router":    "run a router node",
		"assembler": "run an assembler node",
		"batcher":   "run a batcher node",
		"consensus": "run a consensus node",
	}

	logger = flogging.MustGetLogger("arma")
)

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
		"router":    launchRouter(cli.stop, loadConfig, logger),
		"assembler": launchAssembler(cli.stop, loadConfigAndGenesis),
		"batcher":   launchBatcher(cli.stop, loadConfig),
		"consensus": launchConsensus(cli.stop, loadConfigAndGenesis),
	} {
		cli.Command(name, help[name], f)
	}
}

func launchAssembler(
	stop chan struct{},
	loadConfigAndGenesis func(configFile *os.File) ([]byte, *common.Block),
) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent, genesisBlock := loadConfigAndGenesis(configFile)
		conf := parseAssemblerConfig(configContent)
		assembler := assembler.NewAssembler(conf, genesisBlock, logger)

		srv := node.CreateGRPCAssembler(conf)

		orderer.RegisterAtomicBroadcastServer(srv.Server(), assembler)

		go func() {
			_ = srv.Start()
			close(stop)
		}()

		logger.Infof("Assembler listening on %s", srv.Address())
	}
}

func launchConsensus(
	stop chan struct{},
	loadConfigAndGenesis func(configFile *os.File) ([]byte, *common.Block),
) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent, genesisBlock := loadConfigAndGenesis(configFile)
		conf := parseConsensusConfig(configContent)
		consensus := consensus.CreateConsensus(conf, genesisBlock, logger)
		defer consensus.Start()

		srv := node.CreateGRPCConsensus(conf)

		protos.RegisterConsensusServer(srv.Server(), consensus)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), consensus.DeliverService)
		orderer.RegisterClusterNodeServiceServer(srv.Server(), consensus)

		go func() {
			srv.Start()
			close(stop)
		}()

		logger.Infof("Consensus listening on %s", srv.Address())
	}
}

func launchBatcher(stop chan struct{}, loadConfig func(configFile *os.File) []byte) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent := loadConfig(configFile)
		conf := parseBatcherConfig(configContent)
		batcher := batcher.CreateBatcher(conf, logger, nil, &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{})
		defer batcher.Run()

		srv := node.CreateGRPCBatcher(conf)

		protos.RegisterRequestTransmitServer(srv.Server(), batcher)
		protos.RegisterAckServiceServer(srv.Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), batcher)

		go func() {
			srv.Start()
			close(stop)
		}()

		logger.Infof("Batcher listening on %s", srv.Address())
	}
}

func launchRouter(stop chan struct{}, loadConfig func(configFile *os.File) []byte, logger types.Logger) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent := loadConfig(configFile)
		conf := parseRouterConfig(configContent)
		router := router.NewRouter(conf, logger)

		srv := node.CreateGRPCRouter(conf)

		protos.RegisterRequestTransmitServer(srv.Server(), router)
		orderer.RegisterAtomicBroadcastServer(srv.Server(), router)

		go func() {
			srv.Start()
			close(stop)
		}()

		logger.Infof("Router listening on %s", srv.Address())
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

func loadConfig(configFile *os.File) []byte {
	stat, err := configFile.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed opening file %s: %v \n", configFile.Name(), err)
		os.Exit(2)
	}

	content := make([]byte, stat.Size())
	_, err = io.ReadFull(configFile, content)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed reading file%s: %v \n", configFile.Name(), err)
		os.Exit(2)
	}

	return content
}

func loadConfigAndGenesis(configFile *os.File) ([]byte, *common.Block) {
	content := loadConfig(configFile)

	absConfigFileName, err := filepath.Abs(configFile.Name())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed extracting absolute path from file %s: %v \n", configFile.Name(), err)
		os.Exit(2)
	}
	configPath, _ := filepath.Split(absConfigFileName)
	blockFileName := path.Join(configPath, "genesis.block")
	statBlock, err := os.Stat(blockFileName)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stdout, "genesis block file does not exist in config path: %s (ignoring) \n", configPath)
			return content, nil
		} else {
			fmt.Fprintf(os.Stderr, "failed stat file %s: %v \n", blockFileName, err)
			os.Exit(2)
		}
	}

	blockFile, err := os.Open(blockFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can not open genesis block file from: %s (ignoring): %v \n", blockFileName, err)
		return content, nil
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

	return content, block
}

func parseRouterConfig(rawConfig []byte) config.RouterNodeConfig {
	var conf config.RouterNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing router config: %v \n", err)
		os.Exit(2)
	}

	return conf
}

func parseAssemblerConfig(rawConfig []byte) config.AssemblerNodeConfig {
	var conf config.AssemblerNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing assembler config: %v \n", err)
		os.Exit(2)
	}

	if DirOverride != "" {
		conf.Directory = DirOverride
	}

	return conf
}

func parseBatcherConfig(rawConfig []byte) config.BatcherNodeConfig {
	var conf config.BatcherNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing batcher config: %v \n", err)
		os.Exit(2)
	}

	if DirOverride != "" {
		conf.Directory = DirOverride
	}

	return conf
}

func parseConsensusConfig(rawConfig []byte) config.ConsenterNodeConfig {
	var conf config.ConsenterNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing consensus config: %v \n", err)
		os.Exit(2)
	}

	if DirOverride != "" {
		conf.Directory = DirOverride
	}

	return conf
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
