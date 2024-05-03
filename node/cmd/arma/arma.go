package arma

import (
	arma "arma/pkg"
	"fmt"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"io"
	"os"
	"time"

	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"

	"github.ibm.com/Yacov-Manevich/ARMA/node"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"
)

const (
	RouterListenPort    = 6022
	AssemblerListenPort = 6023
	BatcherListenPort   = 6024
	ConsensusListenPort = 6025
)

var (
	help = map[string]string{
		"router":    "run a router node",
		"assembler": "run an assembler node",
		"batcher":   "run a batcher node",
		"consensus": "run a consensus node",
	}

	logger = flogging.MustGetLogger("arma")

	RouterListenAddr    = fmt.Sprintf("0.0.0.0:%d", RouterListenPort)
	AssemblerListenAddr = fmt.Sprintf("0.0.0.0:%d", AssemblerListenPort)
	BatcherListenAddr   = fmt.Sprintf("0.0.0.0:%d", BatcherListenPort)
	ConsensusListenAddr = fmt.Sprintf("0.0.0.0:%d", ConsensusListenPort)
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
	loadConfig := func(configFile *os.File) []byte {
		stat, err := configFile.Stat()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed opening file %s: %v", configFile.Name(), err)
			os.Exit(2)
		}

		content := make([]byte, stat.Size())
		_, err = io.ReadFull(configFile, content)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed reading file%s: %v", configFile.Name(), err)
			os.Exit(2)
		}

		return content
	}

	for name, f := range map[string]func(configFile *os.File){
		"router":    launchRouter(cli.stop, loadConfig, logger),
		"assembler": launchAssembler(cli.stop, loadConfig),
		"batcher":   launchBatcher(cli.stop, loadConfig),
		"consensus": launchConsensus(cli.stop, loadConfig),
	} {
		cli.Command(name, help[name], f)
	}
}

func launchAssembler(stop chan struct{}, loadConfig func(configFile *os.File) []byte) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent := loadConfig(configFile)
		conf := parseAssemblerConfig(configContent)
		assembler := node.CreateAssembler(conf, logger)

		srv := createGRPCAssembler(conf)

		orderer.RegisterAtomicBroadcastServer(srv.Server(), assembler)

		go func() {
			srv.Start()
			close(stop)
		}()

		logger.Infof("Assembler listening on %s", srv.Address())
	}
}

func launchConsensus(stop chan struct{}, loadConfig func(configFile *os.File) []byte) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent := loadConfig(configFile)
		conf := parseConsensusConfig(configContent)
		consensus := node.CreateConsensus(conf, logger)

		srv := createGRPCConsensus(conf)

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
		batcher := node.CreateBatcher(conf, logger)

		srv := createGRPCBatcher(conf)

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

func launchRouter(stop chan struct{}, loadConfig func(configFile *os.File) []byte, logger arma.Logger) func(configFile *os.File) {
	return func(configFile *os.File) {
		configContent := loadConfig(configFile)
		conf := parseRouterConfig(configContent)
		router := node.CreateRouter(conf, logger)

		srv := createGRPCRouter(conf)

		protos.RegisterRequestTransmitServer(srv.Server(), router)

		go func() {
			srv.Start()
			close(stop)
		}()

		logger.Infof("Router listening on %s", srv.Address())
	}
}

func createGRPCRouter(conf node.RouterNodeConfig) *comm.GRPCServer {
	srv, err := comm.NewGRPCServer(RouterListenAddr, comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: conf.TLSCertificateFile,
			Key:         conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func createGRPCAssembler(conf node.AssemblerNodeConfig) *comm.GRPCServer {
	srv, err := comm.NewGRPCServer(AssemblerListenAddr, comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: conf.TLSCertificateFile,
			Key:         conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func createGRPCBatcher(conf node.BatcherNodeConfig) *comm.GRPCServer {
	var clientRootCAs [][]byte

	for _, shard := range conf.Shards {
		if shard.ShardId != conf.ShardId {
			continue
		}
		for _, batchers := range shard.Batchers {
			for _, tlsCA := range batchers.TLSCACerts {
				clientRootCAs = append(clientRootCAs, tlsCA)
			}
		}
	}

	srv, err := comm.NewGRPCServer(BatcherListenAddr, comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     clientRootCAs,
			ServerRootCAs:     clientRootCAs,
			RequireClientCert: true,
			UseTLS:            true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func createGRPCConsensus(conf node.ConsenterNodeConfig) *comm.GRPCServer {
	var clientRootCAs [][]byte

	for _, shard := range conf.Shards {
		for _, batchers := range shard.Batchers {
			for _, tlsCA := range batchers.TLSCACerts {
				clientRootCAs = append(clientRootCAs, tlsCA)
			}
		}
	}

	for _, consenter := range conf.Consenters {
		for _, tlsCA := range consenter.TLSCACerts {
			clientRootCAs = append(clientRootCAs, tlsCA)
		}
	}

	srv, err := comm.NewGRPCServer(ConsensusListenAddr, comm.ServerConfig{
		KaOpts: comm.KeepaliveOptions{
			ServerMinInterval: time.Microsecond,
		},
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     clientRootCAs,
			ServerRootCAs:     clientRootCAs,
			RequireClientCert: true,
			UseTLS:            true,
			Certificate:       conf.TLSCertificateFile,
			Key:               conf.TLSPrivateKeyFile,
		},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed running gRPC service: %v", err)
		os.Exit(1)
	}
	return srv
}

func (cli *CLI) Run(args []string) <-chan struct{} {
	configFile := cli.app.Flag("config", "Specifies the config file to load the configuration from").File()
	command := kingpin.MustParse(cli.app.Parse(args))
	f, exists := cli.dispatchers[command]
	if !exists {
		fmt.Fprintf(os.Stderr, "command %s doesn't exist", command)
		os.Exit(2)
	}

	if *configFile == nil {
		fmt.Fprintf(os.Stderr, "config parameter missing")
		os.Exit(2)
	}

	f(*configFile)

	return cli.stop
}

func NewCLI() *CLI {
	app := kingpin.New("Arma", "Launches an Arma node (Router | Assembler | Batcher | Consensus)")
	cli := &CLI{app: app, dispatchers: make(map[string]func(configFile *os.File)), stop: make(chan struct{})}
	cli.configureNodesCommands()
	return cli
}

func parseRouterConfig(rawConfig []byte) node.RouterNodeConfig {
	var conf node.RouterNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing router config: %v", err)
		os.Exit(2)
	}

	return conf
}

func parseAssemblerConfig(rawConfig []byte) node.AssemblerNodeConfig {
	var conf node.AssemblerNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing assembler config: %v", err)
		os.Exit(2)
	}

	return conf
}

func parseBatcherConfig(rawConfig []byte) node.BatcherNodeConfig {
	var conf node.BatcherNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing batcher config: %v", err)
		os.Exit(2)
	}

	return conf
}

func parseConsensusConfig(rawConfig []byte) node.ConsenterNodeConfig {
	var conf node.ConsenterNodeConfig
	err := yaml.Unmarshal(rawConfig, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing consensus config: %v", err)
		os.Exit(2)
	}

	return conf
}
