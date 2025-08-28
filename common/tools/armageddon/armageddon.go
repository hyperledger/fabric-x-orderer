/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"

	"github.com/hyperledger/fabric-x-orderer/config/protos"

	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/alecthomas/kingpin"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

var defaultConfig = `Parties:
  - ID: 1
    AssemblerEndpoint: "127.0.0.1:7050"
    ConsenterEndpoint: "127.0.0.1:7051"
    RouterEndpoint: "127.0.0.1:7052"
    BatchersEndpoints:
      - "127.0.0.1:7053"
      - "127.0.0.1:7054"
  - ID: 2
    AssemblerEndpoint: "127.0.0.1:7055"
    ConsenterEndpoint: "127.0.0.1:7056"
    RouterEndpoint: "127.0.0.1:7057"
    BatchersEndpoints:
      - "127.0.0.1:7058"
      - "127.0.0.1:7059"
  - ID: 3
    AssemblerEndpoint: "127.0.0.1:7060"
    ConsenterEndpoint: "127.0.0.1:7061"
    RouterEndpoint: "127.0.0.1:7062"
    BatchersEndpoints:
      - "127.0.0.1:7063"
      - "127.0.0.1:7064"
  - ID: 4
    AssemblerEndpoint: "127.0.0.1:7065"
    ConsenterEndpoint: "127.0.0.1:7066"
    RouterEndpoint: "127.0.0.1:7067"
    BatchersEndpoints:
      - "127.0.0.1:7068"
      - "127.0.0.1:7069"
`

func init() {
	// set the gRPC logger to a logger that discards the log output.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
}

var logger = flogging.MustGetLogger("armageddon")

type protectedMap struct {
	keyValMap map[string]bool
	mutex     sync.Mutex
}

func (pm *protectedMap) Add(key string) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.keyValMap[key] = true
}

func (pm *protectedMap) Remove(key string) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	delete(pm.keyValMap, key)
}

func (pm *protectedMap) IsEmpty() bool {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	return len(pm.keyValMap) == 0
}

type CLI struct {
	app      *kingpin.Application
	commands map[string]*kingpin.CmdClause
	// generate command flags
	outputDir        *string
	genConfigFile    **os.File
	sampleConfigPath *string
	useTLS           *bool
	// submit command flags
	userConfigFile **os.File
	transactions   *int // transactions is the number of txs to be sent
	rate           *int // rate is the number of transaction per second to be sent
	txSize         *int // txSize is the required transaction size
	// load command flags
	loadUserConfigFile **os.File
	loadTransactions   *int
	loadRate           *string
	loadTxSize         *int
	// receive command flags
	receiveUserConfigFile   **os.File
	receiveExpectedNumOfTxs *int
	receiveOutputDir        *string
	pullFromPartyId         *int
	// createSharedConfigProto command flags
	sharedConfigYamlPath *string
	sharedConfigProtoDir *string
	// createBlock command flags
	sharedConfigYamlPathToBlock *string
	blockDir                    *string
	sampleConfigPathToBlock     *string
}

func NewCLI() *CLI {
	app := kingpin.New("armageddon", "Utility for generating Arma config material")
	cli := &CLI{app: app}
	cli.configureCommands()
	return cli
}

func (cli *CLI) configureCommands() {
	commands := make(map[string]*kingpin.CmdClause)
	gen := cli.app.Command("generate", "Generate config material")
	cli.outputDir = gen.Flag("output", "The output directory in which to place config files").Default("arma-config").String()
	cli.genConfigFile = gen.Flag("config", "The configuration template to use").File()
	cli.useTLS = gen.Flag("useTLS", "Defines if the connection between a client to a router and an assembler is a TLS one or not").Bool()
	cli.sampleConfigPath = gen.Flag("sampleConfigPath", "The path to the sample config files").String()
	commands["generate"] = gen

	showtemplate := cli.app.Command("showtemplate", "Show the default configuration template needed to build Arma config material")
	commands["showtemplate"] = showtemplate

	version := cli.app.Command("version", "Show version information")
	commands["version"] = version

	submit := cli.app.Command("submit", "Submit txs to routers and verify the submission")
	cli.userConfigFile = submit.Flag("config", "The user configuration needed to connection with routers and assemblers").File()
	cli.transactions = submit.Flag("transactions", "The number of transactions to be sent").Int()
	cli.rate = submit.Flag("rate", "The rate specify the number of transactions per second to be sent").Int()
	cli.txSize = submit.Flag("txSize", "The required transaction size in bytes").Default("512").Int()
	commands["submit"] = submit

	load := cli.app.Command("load", "Submit txs to routers and verify the routers have received the txs")
	cli.loadUserConfigFile = load.Flag("config", "The user configuration needed to connection with routers").File()
	cli.loadTransactions = load.Flag("transactions", "The number of transactions to be sent").Int()
	cli.loadRate = load.Flag("rate", "The rate specifies the number of transactions per second to be sent as one or more rate numbers separated by space").String()
	cli.loadTxSize = load.Flag("txSize", "The required transaction size in bytes").Int()
	commands["load"] = load

	receive := cli.app.Command("receive", "Pull txs from some assembler and report statistics")
	cli.receiveUserConfigFile = receive.Flag("config", "The user configuration needed to connection with assemblers").File()
	cli.receiveExpectedNumOfTxs = receive.Flag("expectedTxs", "The expected number of transactions the assembler should received").Default("-1").Int()
	cli.receiveOutputDir = receive.Flag("output", "The output directory in which to place statistics file").Default(".").String()
	cli.pullFromPartyId = receive.Flag("pullFromPartyId", "The party id of the assembler to pull blocks from").Int()
	commands["receive"] = receive

	createSharedConfigProto := cli.app.Command("createSharedConfigProto", "Create a shared config binary from a shared configuration YAML file")
	cli.sharedConfigYamlPath = createSharedConfigProto.Flag("sharedConfigYaml", "The path to the shared configuration YAML file").String()
	cli.sharedConfigProtoDir = createSharedConfigProto.Flag("output", "The output directory in which to place the shared config binary").String()
	commands["createSharedConfigProto"] = createSharedConfigProto

	createBlock := cli.app.Command("createBlock", "Create a new block with the given shared configuration")
	cli.sharedConfigYamlPathToBlock = createBlock.Flag("sharedConfigYaml", "The path to the shared configuration YAML file").String()
	cli.blockDir = createBlock.Flag("output", "The output directory in which to place the block").String()
	cli.sampleConfigPathToBlock = createBlock.Flag("sampleConfigPath", "The path to the sample config files").String()
	commands["createBlock"] = createBlock

	cli.commands = commands
}

func (cli *CLI) Run(args []string) {
	switch kingpin.MustParse(cli.app.Parse(args)) {

	// "generate" command
	case cli.commands["generate"].FullCommand():
		generateConfigAndCrypto(cli.genConfigFile, cli.outputDir, cli.sampleConfigPath)
		logger.Infof("Configuration material was created successfully in %s", *cli.outputDir)

	// "showtemplate" command
	case cli.commands["showtemplate"].FullCommand():
		showtemplate()

	// "version" command
	case cli.commands["version"].FullCommand():
		printVersion()

	// "submit" command
	case cli.commands["submit"].FullCommand():
		submit(cli.userConfigFile, cli.transactions, cli.rate, cli.txSize)

	// "load" command
	case cli.commands["load"].FullCommand():
		load(cli.loadUserConfigFile, cli.loadTransactions, cli.loadRate, cli.loadTxSize)

	// "receive" command
	case cli.commands["receive"].FullCommand():
		receive(cli.receiveUserConfigFile, cli.pullFromPartyId, cli.receiveOutputDir, cli.receiveExpectedNumOfTxs)

	// "createSharedConfigProto" command
	case cli.commands["createSharedConfigProto"].FullCommand():
		createSharedConfigProto(cli.sharedConfigYamlPath, cli.sharedConfigProtoDir)

	// "createBlock" command
	case cli.commands["createBlock"].FullCommand():
		createBlock(cli.sharedConfigYamlPathToBlock, cli.blockDir, cli.sampleConfigPathToBlock)
	}
}

func createBlock(sharedConfigYamlPath *string, outputDir *string, sampleConfigPath *string) {
	sharedConfigProto, sharedConfigYaml, err := config.LoadSharedConfig(*sharedConfigYamlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigToBlock(sharedConfigProto, sharedConfigYaml, outputDir, sampleConfigPath)
}

func createSharedConfigProto(sharedConfigYamlPath *string, outputDir *string) {
	if *outputDir == "" {
		fmt.Fprintf(os.Stderr, "Error creating block, outputDir is missing")
		os.Exit(-1)
	}

	sharedConfig, _, err := config.LoadSharedConfig(*sharedConfigYamlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigToProto(sharedConfig, outputDir)
}

func sharedConfigToProto(sharedConfig *protos.SharedConfig, outputDir *string) {
	sharedConfigBytes, err := proto.Marshal(sharedConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigBinaryPath := filepath.Join(*outputDir, "shared_config.binpb")
	err = os.WriteFile(sharedConfigBinaryPath, sharedConfigBytes, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing the shared config binary: %s", err)
		os.Exit(-1)
	}
}

func sharedConfigToBlock(sharedConfig *protos.SharedConfig, sharedConfigYaml *config.SharedConfigYaml, outputDir *string, sampleConfigPath *string) {
	sharedConfigBytes, err := proto.Marshal(sharedConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigBinaryPath := filepath.Join(*outputDir, "shared_config.bin")
	err = os.WriteFile(sharedConfigBinaryPath, sharedConfigBytes, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing the shared config binary: %s", err)
		os.Exit(-1)
	}

	_, err = genconfig.CreateGenesisBlock(*outputDir, sharedConfigYaml, sharedConfigBinaryPath, *sampleConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creation bootstrap config block: %s", err)
		os.Exit(-1)
	}
}

// generateConfigAndCrypto is generating the crypto material and the configuration files in the new format.
func generateConfigAndCrypto(genConfigFile **os.File, outputDir *string, sampleConfigPath *string) {
	if *sampleConfigPath == "" {
		if path, err := fabric.SafeGetDevConfigDir(); err == nil && path != "" {
			*sampleConfigPath = path
		} else {
			fmt.Fprintf(os.Stderr, "Configuration generation failed because the required sampleConfigPath flag was not provided, and setting a default value for it was unsuccessful: %s", err)
			os.Exit(-1)
		}
	}

	// get config file content given as argument
	networkConfig, err := getConfigFileContent(genConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// generate crypto material
	err = GenerateCryptoConfig(networkConfig, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating crypto config: %s", err)
		os.Exit(-1)
	}

	// generate local config yaml files
	networkLocalConfig, err := genconfig.CreateArmaLocalConfig(*networkConfig, *outputDir, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating local config: %s", err)
		os.Exit(-1)
	}

	// generate shared config yaml file
	sharedConfigYaml, err := genconfig.CreateArmaSharedConfig(*networkConfig, networkLocalConfig, *outputDir, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(*outputDir, "bootstrap", "shared_config.yaml"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading shared config: %s", err)
		os.Exit(-1)
	}

	blockDir := filepath.Join(*outputDir, "bootstrap")
	sharedConfigToBlock(sharedConfig, sharedConfigYaml, &blockDir, sampleConfigPath)

	// generate user config yaml file for each party
	// user will be able to connect to each of the routers and assemblers only if it receives for each router the CA that signed the certificate of that router.
	// therefore, the CA created per party must be collected for each party, to which the router is associated.
	var tlsCACertsBytesPartiesCollection [][]byte
	for _, party := range sharedConfig.PartiesConfig {
		tlsCACertsBytesPartiesCollection = append(tlsCACertsBytesPartiesCollection, party.TLSCACerts...)
	}

	for i := range sharedConfig.PartiesConfig {
		userTLSPrivateKeyPath := filepath.Join(*outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", "user-key.pem")
		userTLSCertPath := filepath.Join(*outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", "user-tls-cert.pem")

		userConfig, err := NewUserConfig(userTLSPrivateKeyPath, userTLSCertPath, tlsCACertsBytesPartiesCollection, networkConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating user config: %s", err)
			os.Exit(-1)
		}

		err = utils.WriteToYAML(userConfig, filepath.Join(*outputDir, "config", fmt.Sprintf("party%d", i+1), "user_config.yaml"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating user config yaml: %s", err)
			os.Exit(-1)
		}
	}
}

func getConfigFileContent(genConfigFile **os.File) (*genconfig.Network, error) {
	var configFileContent string
	if *genConfigFile != nil {
		data, err := io.ReadAll(*genConfigFile)
		if err != nil {
			return nil, fmt.Errorf("error reading configuration template: %s", err)
		}
		configFileContent = string(data)
	} else {
		// no configuration template has been provided, hence the default one is chosen
		configFileContent = defaultConfig
	}

	network := genconfig.Network{}
	err := yaml.Unmarshal([]byte(configFileContent), &network)
	if err != nil {
		return nil, fmt.Errorf("error Unmarshalling YAML: %s", err)
	}

	return &network, nil
}

func showtemplate() {
	fmt.Print(defaultConfig)
	os.Exit(0)
}

func printVersion() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Println(fmt.Errorf("failed to read build info"))
	}

	fmt.Printf("Armageddon version is: %+v\n", bi.Main.Version)
}

func ReadUserConfig(userConfigFile **os.File) (*UserConfig, error) {
	var configFileContent string
	if *userConfigFile != nil {
		data, err := io.ReadAll(*userConfigFile)
		if err != nil {
			return nil, fmt.Errorf("error reading configuration template: %s", err)
		}
		configFileContent = string(data)
	} else {
		// no configuration template has been provided
		fmt.Fprintf(os.Stderr, "user config yaml file is missing")
		os.Exit(1)
	}

	userConfig := UserConfig{}
	err := yaml.Unmarshal([]byte(configFileContent), &userConfig)
	if err != nil {
		return nil, fmt.Errorf("error Unmarshalling YAML: %s", err)
	}

	return &userConfig, nil
}

// submit command makes txs and sends them to all routers
// it also asks for blocks from some assembler (no matter who it is) to validate the txs appear in some block
func submit(userConfigFile **os.File, transactions *int, rate *int, txSize *int) {
	// check transaction size
	txMinimumSize := 16 + 8 + 8
	if *txSize < txMinimumSize {
		fmt.Fprintf(os.Stderr, "the required tx size: %d is less than the minimum size: %d", *txSize, txMinimumSize)
		os.Exit(3)
	}

	// get user config file content given as argument
	userConfig, err := ReadUserConfig(userConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// send txs to the routers
	start := time.Now()
	txsMap := &protectedMap{
		keyValMap: make(map[string]bool),
		mutex:     sync.Mutex{},
	}

	logger.Infof("Submit starts.....")
	var waitForTxToBeSentAndReceived sync.WaitGroup
	waitForTxToBeSentAndReceived.Add(2)
	go func() {
		sendTxToRouters(userConfig, *transactions, *rate, *txSize, txsMap)
		waitForTxToBeSentAndReceived.Done()
	}()

	// receive blocks from some assembler
	var numOfBlocks int
	var txDelayTimes float64
	go func() {
		numOfBlocks, txDelayTimes = receiveResponseFromAssembler(userConfig, txsMap, *transactions)
		waitForTxToBeSentAndReceived.Done()
	}()

	waitForTxToBeSentAndReceived.Wait()
	elapsed := time.Since(start)
	logger.Infof("Submit Finished.....")
	// report results
	reportResults(*transactions, elapsed, txDelayTimes, numOfBlocks, *txSize)
}

// load command makes txs and sends them to all routers
func load(userConfigFile **os.File, transactions *int, rate *string, txSize *int) {
	rates := strings.Fields(*rate)
	// check transaction size
	txMinimumSize := 16 + 8 + 8
	if *txSize < txMinimumSize {
		fmt.Fprintf(os.Stderr, "the required tx size: %d is less than the minimum size: %d", *txSize, txMinimumSize)
		os.Exit(3)
	}

	// get user config file content given as argument
	userConfig, err := ReadUserConfig(userConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}
	convertedRates := make([]int, len(rates))
	for i := 0; i < len(rates); i++ {
		convertedRates[i], err = strconv.Atoi(rates[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "rate is not valid: %s", err)
			os.Exit(-1)
		}
	}
	// send txs to the routers
	fmt.Printf("start to load, calling to LoadThatTolerateRoutersFaulty...\n")
	for i := 0; i < len(rates); i++ {
		start := time.Now()
		// sendTxToRouters(userConfig, *transactions, convertedRates[i], *txSize, nil)
		LoadThatTolerateRoutersFaulty(userConfig, *transactions, convertedRates[i], *txSize, nil)
		elapsed := time.Since(start)
		reportLoadResults(*transactions, elapsed, *txSize)
	}
}

// receive command pull blocks from the assembler and report statistics
func receive(userConfigFile **os.File, pullFromPartyId *int, receiveOutputDir *string, expectedNumOfTxs *int) {
	// get user config file content given as argument
	userConfig, err := ReadUserConfig(userConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s", err)
		os.Exit(-1)
	}

	// pull blocks from the assembler and report statistics to statistics.csv file
	pullBlocksFromAssemblerAndCollectStatistics(userConfig, *pullFromPartyId, *receiveOutputDir, *expectedNumOfTxs)
	logger.Infof("Receive command finished, statistics can be found in: %v\n", path.Join(*receiveOutputDir, "statistics.csv"))
}

func trimPortFromEndpoint(endpoint string) string {
	if strings.Contains(endpoint, ":") {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			panic(fmt.Sprintf("endpoint %s is not a valid host:port string: %v", endpoint, err))
		}
		return host
	}

	return endpoint
}

func nextSeekInfo(startSeq uint64) *ab.SeekInfo {
	return &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: startSeq}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT,
	}
}

func sendTxToRouters(userConfig *UserConfig, numOfTxs int, rate int, txSize int, txsMap *protectedMap) {
	var gRPCRouterClientsConn []*grpc.ClientConn
	var streams []ab.AtomicBroadcast_BroadcastClient

	// serverRootCAs := append([][]byte{}, userConfig.TLSCACerts...)

	// create gRPC clients and streams to the routers
	for i := 0; i < len(userConfig.RouterEndpoints); i++ {
		// create a gRPC connection to the router
		gRPCRouterClient := comm.ClientConfig{
			KaOpts: comm.KeepaliveOptions{
				ClientInterval: time.Hour,
				ClientTimeout:  time.Hour,
			},
			SecOpts: comm.SecureOptions{
				Key:               userConfig.TLSPrivateKey,
				Certificate:       userConfig.TLSCertificate,
				RequireClientCert: userConfig.UseTLSRouter == "mTLS",
				UseTLS:            userConfig.UseTLSRouter != "none",
				ServerRootCAs:     userConfig.TLSCACerts,
			},
			DialTimeout: time.Second * 5,
		}

		gRPCRouterClientConn, err := gRPCRouterClient.Dial(userConfig.RouterEndpoints[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create a gRPC client connection to router %d: %v", i+1, err)
			os.Exit(3)
		}

		gRPCRouterClientsConn = append(gRPCRouterClientsConn, gRPCRouterClientConn)

		// open a broadcast stream
		stream, err := ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open a broadcast stream to router %d: %v", i+1, err)
			os.Exit(3)
		}

		streams = append(streams, stream)
	}

	// open a go routine to check for acknowledgment
	var wgRecv sync.WaitGroup
	for n, s := range streams {
		wgRecv.Add(1)
		go func(n int, stream ab.AtomicBroadcast_BroadcastClient) {
			defer wgRecv.Done()
			numOfAcks := 0
			for {
				ack, err := stream.Recv()
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to receive acknowledgment from router %d: %v", n+1, err)
					os.Exit(3)
				}
				if ack.Status.String() != "SUCCESS" {
					fmt.Fprintf(os.Stderr, "failed to receive ack with success status from router %d: %v", n+1, err)
					os.Exit(3)
				}
				numOfAcks = numOfAcks + 1
				if numOfAcks == numOfTxs {
					break
				}
			}
		}(n, s)
	}

	// create a session number (16 bytes)
	sessionNumber := make([]byte, 16)
	_, err := rand.Read(sessionNumber)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a session number, %v", err)
		os.Exit(3)
	}

	// send txs to all routers, using the rate limiter bucket
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	capacity := rate / fillFrequency
	rl, err := NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter, err: %v\n", err)
		os.Exit(3)
	}
	for i := 0; i < numOfTxs; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		sendTx(txsMap, streams, i, txSize, sessionNumber)
	}
	rl.Stop()

	wgRecv.Wait()

	// close gRPC connections
	for i, conn := range gRPCRouterClientsConn {
		if err := conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close gRPC connection to router %d: %v", i+1, err)
			os.Exit(3)
		}
	}
}

func pullBlocksFromAssemblerAndCollectStatistics(userConfig *UserConfig, pullFromPartyId int, receiveOutputDir string, expectedNumOfTxs int) {
	// choose randomly the first assembler
	i := pullFromPartyId - 1

	serverRootCAs := append([][]byte{}, userConfig.TLSCACerts...)

	// create a gRPC connection to the assembler
	gRPCAssemblerClient := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               userConfig.TLSPrivateKey,
			Certificate:       userConfig.TLSCertificate,
			RequireClientCert: userConfig.UseTLSAssembler == "mTLS",
			UseTLS:            userConfig.UseTLSAssembler != "none",
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		nil,
		nextSeekInfo(0),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed create a request envelope")
		os.Exit(3)
	}

	var stream ab.AtomicBroadcast_DeliverClient
	var gRPCAssemblerClientConn *grpc.ClientConn
	endpointToPullFrom := userConfig.AssemblerEndpoints[i]

	gRPCAssemblerClientConn, err = gRPCAssemblerClient.Dial(endpointToPullFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a gRPC client connection to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver stream
	stream, err = abc.Deliver(context.TODO())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a deliver stream to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// send request envelope
	err = stream.Send(requestEnvelope)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to send a request envelope to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// pull blocks from assembler, every second pack statistics and send it to the manageStatistics
	statisticsAggregator := &StatisticsAggregator{}

	statisticChan := make(chan Statistics, 60)
	blockChan := make(chan BlockWithTime)
	stopChan := make(chan bool)

	var waitToFinish sync.WaitGroup
	waitToFinish.Add(4)

	statisticsAggregator.startTime = time.Now().UnixMilli()
	startTimeS := float64(statisticsAggregator.startTime) / 1000
	timeIntervalToSampleStat := 1 * time.Second

	// handle statistics channel messages
	go func() {
		manageStatistics(receiveOutputDir, statisticChan, stopChan, startTimeS, expectedNumOfTxs, pullFromPartyId, timeIntervalToSampleStat)
		waitToFinish.Done()
	}()

	// every second read the statistics and send it to the manageStatistics
	go func() {
		ticker := time.NewTicker(timeIntervalToSampleStat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// send the accumulated statistics to the channel
				lastStat := statisticsAggregator.ReadAndReset()
				statisticChan <- lastStat
			case <-stopChan:
				waitToFinish.Done()
				return
			}
		}
	}()

	// pull blocks from the assembler
	// if a flag is given, stop when finish receiving all txs
	go func() {
		logger.Infof("starting pulling blocks from the assembler")
		var txsTotal int
		for {
			block, err := pullBlock(stream, endpointToPullFrom, gRPCAssemblerClientConn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to pull block from assembler %d: %v", i+1, err)
				os.Exit(3)
			}

			if block.Header.Number == 0 {
				continue
			}

			blockWithTime := BlockWithTime{
				block:        block,
				acceptedTime: time.Now(),
			}
			blockChan <- blockWithTime
			txsTotal += len(blockWithTime.block.Data.Data)

			logger.Debugf("block with %d txs was pulled from the assembler, overall %d txs were received at this moment", len(blockWithTime.block.Data.Data), txsTotal)

			if expectedNumOfTxs > 0 && expectedNumOfTxs <= txsTotal {
				logger.Debugf("overall %d txs were received, finished pulling", txsTotal)
				waitToFinish.Done()
				return
			}
		}
	}()

	// parse blocks and make statistics on each block
	go func() {
		var sumOfDelayTimes float64
		var txs int
		var txsTotal int
		var sumOfTxsSize int
		for {
			blockWithTime := <-blockChan
			sumOfDelayTimes = 0.0
			sumOfTxsSize = 0
			txs = len(blockWithTime.block.Data.Data)
			txsTotal += len(blockWithTime.block.Data.Data)
			// iterate over txs in block
			for j := 0; j < txs; j++ {
				env, err := protoutil.GetEnvelopeFromBlock(blockWithTime.block.Data.Data[j])
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to get envelope from block with time: err: %v", err)
					os.Exit(3)
				}
				data, err := tx.GetDataFromEnvelope(env)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to get data from envelope: err: %v", err)
					os.Exit(3)
				}
				logger.Debugf("tx %x was received from the assembler", data)

				// extract the tx size, sending time and calculate the delay, add the delay to sumOfDelayTimes
				sumOfTxsSize += len(data)
				delay := calculateDelayOfTx(data, blockWithTime.acceptedTime)
				sumOfDelayTimes = sumOfDelayTimes + delay.Seconds()
			}
			statisticsAggregator.Add(txs, 1, sumOfDelayTimes, sumOfTxsSize)

			if expectedNumOfTxs > 0 && expectedNumOfTxs <= txsTotal {
				logger.Infof("%d txs were expected and overall %d were successfully received", expectedNumOfTxs, txsTotal)
				close(stopChan)
				waitToFinish.Done()
				return
			}
		}
	}()

	waitToFinish.Wait()
	logger.Debugf("exit pulling blocks from the assembler")
}

func calculateDelayOfTx(data []byte, acceptedTime time.Time) time.Duration {
	sendTime := tx.ExtractTimestampFromTx(data)
	delayTime := acceptedTime.Sub(sendTime)
	return delayTime
}

func pullBlock(stream ab.AtomicBroadcast_DeliverClient, endpointToPullFrom string, gRPCAssemblerClientConn *grpc.ClientConn) (*common.Block, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive a deliver response from %s", endpointToPullFrom)
	}

	block := resp.GetBlock()

	if block == nil {
		stream.CloseSend()
		gRPCAssemblerClientConn.Close()
		return nil, fmt.Errorf("received a non block message from %s: %v", endpointToPullFrom, resp)
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		stream.CloseSend()
		gRPCAssemblerClientConn.Close()
		return nil, fmt.Errorf("received empty block from %s", endpointToPullFrom)
	}

	return block, nil
}

func sendTx(txsMap *protectedMap, streams []ab.AtomicBroadcast_BroadcastClient, i int, txSize int, sessionNumber []byte) {
	data := tx.PrepareTxWithTimestamp(i, txSize, sessionNumber)
	if txsMap != nil {
		logger.Debugf("Add tx %x to the map", data)
		txsMap.Add(string(data))
	}
	for j := 0; j < len(streams); j++ {
		err := streams[j].Send(tx.CreateStructuredEnvelope(data))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to send tx to router %d: %v", j+1, err)
			os.Exit(3)
		}
	}
}

func reportResults(transactions int, elapsed time.Duration, txDelayTimesResult float64, numOfBlocksResult int, txSize int) {
	avgTxRate := float64(transactions) / elapsed.Seconds()
	avgTxDelay := txDelayTimesResult / float64(transactions)
	avgBlockRate := float64(numOfBlocksResult) / elapsed.Seconds()
	avgBlockSize := transactions / numOfBlocksResult
	logger.Infof("SUCCESS: number of txs: %d, tx size: %d bytes, elapsed time: %v, avg. tx rate: %.2f, avg. tx delay: %vs, num of blocks: %d, avg. block rate: %v, avg. block size: %v txs\n", transactions, txSize, elapsed, avgTxRate, avgTxDelay, numOfBlocksResult, avgBlockRate, avgBlockSize)
}

func reportLoadResults(transactions int, elapsed time.Duration, txSize int) {
	avgTxSendingRate := float64(transactions) / elapsed.Seconds()
	logger.Infof("Load command finished, sent %d TXs in %v seconds, TX size %d, avg. tx sending rate: %.2f\n", transactions, elapsed, txSize, avgTxSendingRate)
}

// manageStatistics manages a statistics queue and every hour writes the queue to a CSV file
func manageStatistics(receiveOutputDir string, statisticChan <-chan Statistics, stopChan <-chan bool, startTime float64, expectedTxs int, pullFrom int, timeIntervalToSampleStat time.Duration) {
	filePath := path.Join(receiveOutputDir, "statistics.csv")
	logger.Infof("Statistics are written to: %v\n", filePath)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open a csv file: %v", err)
		os.Exit(3)
	}
	defer file.Close()

	// write CSV header if file is new
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to stat the file: %v", err)
		os.Exit(3)
	}
	if fileInfo.Size() == 0 {
		writer := csv.NewWriter(file)
		// Write the description of the experiment
		desc := "Experiment Description: " + fmt.Sprintf("Time: %.2fs, ", startTime) + fmt.Sprintf("Receiver from assembler%d, ", pullFrom)
		if expectedTxs >= 0 {
			desc = desc + fmt.Sprintf("Expected number of txs: %d", expectedTxs)
		}

		writer.Write([]string{desc, "", "", "", "", "", "", "", ""})
		writer.Write([]string{""})
		writer.Write([]string{"Time Since Start (s)", "Number of txs", "Number of blocks", "Avg. tx rate", "Sum of txs size", "Avg. tx size (byte)", "Sum of txs delay (s)", "Avg. tx delay (s)", "Avg. block rate", "Avg. block size (byte)", "Avg. number of txs in block"})
		writer.Flush()
	}

	for {
		select {
		case statistic := <-statisticChan:
			writeStatisticsToCSV(file, statistic, timeIntervalToSampleStat)

		case <-stopChan:
			for {
				select {
				case statistic := <-statisticChan:
					writeStatisticsToCSV(file, statistic, timeIntervalToSampleStat)
				default:
					return
				}
			}
		}
	}
}

func writeStatisticsToCSV(file *os.File, statistic Statistics, timeIntervalToSampleStat time.Duration) {
	writer := csv.NewWriter(file)
	defer writer.Flush()
	defer file.Sync()

	var avgTxSize int
	var avgTxDelay float64
	var avgBlockSize int
	var avgNumOfTxsInBlock int
	if statistic.numOfTxs != 0 {
		avgTxSize = statistic.sumOfTxsSize / statistic.numOfTxs
		avgTxDelay = statistic.sumOfTxsDelay / float64(statistic.numOfTxs)
		avgBlockSize = statistic.sumOfTxsSize / statistic.numOfBlocks
		avgNumOfTxsInBlock = statistic.numOfTxs / statistic.numOfBlocks
	}

	err := writer.Write([]string{
		fmt.Sprintf("%.f", statistic.timeStamp),
		fmt.Sprintf("%d", statistic.numOfTxs),
		fmt.Sprintf("%d", statistic.numOfBlocks),
		fmt.Sprintf("%.2f", float64(statistic.numOfTxs)/timeIntervalToSampleStat.Seconds()),
		fmt.Sprintf("%d", statistic.sumOfTxsSize),
		fmt.Sprintf("%d", avgTxSize),
		fmt.Sprintf("%.2f", statistic.sumOfTxsDelay),
		fmt.Sprintf("%.2f", avgTxDelay),
		fmt.Sprintf("%.2f", float64(statistic.numOfBlocks)/timeIntervalToSampleStat.Seconds()),
		fmt.Sprintf("%d", avgBlockSize),
		fmt.Sprintf("%d", avgNumOfTxsInBlock),
	})
	if err != nil {
		logger.Errorf("failed to write to CSV: %v", err)
	}
}

func receiveResponseFromAssembler(userConfig *UserConfig, txsMap *protectedMap, expectedNumOfTxs int) (int, float64) {
	// choose randomly the first assembler
	i := 0
	serverRootCAs := append([][]byte{}, userConfig.TLSCACerts...)

	// create a gRPC connection to the assembler
	gRPCAssemblerClient := comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               userConfig.TLSPrivateKey,
			Certificate:       userConfig.TLSCertificate,
			RequireClientCert: userConfig.UseTLSAssembler == "mTLS",
			UseTLS:            userConfig.UseTLSAssembler != "none",
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}

	// prepare request envelope
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		nil,
		nextSeekInfo(0),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed create a request envelope")
		os.Exit(3)
	}

	var stream ab.AtomicBroadcast_DeliverClient
	var gRPCAssemblerClientConn *grpc.ClientConn
	endpointToPullFrom := userConfig.AssemblerEndpoints[i]

	gRPCAssemblerClientConn, err = gRPCAssemblerClient.Dial(endpointToPullFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a gRPC client connection to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

	// create a deliver stream
	stream, err = abc.Deliver(context.TODO())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create a deliver stream to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// send request envelope
	err = stream.Send(requestEnvelope)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to send a request envelope to assembler %d: %v", i+1, err)
		os.Exit(3)
	}

	// pull blocks from assembler
	numOfBlocksCalculated := 0
	numOfTxsCalculated := 0
	var sumOfDelayTimes float64
	for {
		block, err := pullBlock(stream, endpointToPullFrom, gRPCAssemblerClientConn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to pull block from assembler %d: %v", i+1, err)
			os.Exit(3)
		}

		if block.Header.Number == 0 {
			continue
		}

		currentTime := time.Now()
		numOfBlocksCalculated += 1

		// iterate over txs in block
		for j := 0; j < len(block.Data.Data); j++ {
			numOfTxsCalculated += 1
			env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[j])
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get envelope from block: %v", err)
				os.Exit(3)
			}

			// 1. extract the sending time and calculate the delay, add the delay to sumOfDelayTimes
			data, err := tx.GetDataFromEnvelope(env)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get data from envelope: err: %v", err)
				os.Exit(3)
			}
			sendTime := tx.ExtractTimestampFromTx(data)
			delayTime := currentTime.Sub(sendTime)
			sumOfDelayTimes = sumOfDelayTimes + delayTime.Seconds()

			// 2. delete the tx from the map
			if txsMap != nil {
				logger.Debugf("remove tx %x from the map", data)
				txsMap.Remove(string(data))
			}
		}

		// if the map is empty it means we received all txs, then we stop asking for blocks from the assembler
		// NOTE: the map is relevant when using the submit command. Load and receive commands don't maintain a map.
		if expectedNumOfTxs < 0 {
			continue
		}

		if (txsMap != nil && txsMap.IsEmpty()) && numOfTxsCalculated >= expectedNumOfTxs {
			break
		}
	}

	return numOfBlocksCalculated, sumOfDelayTimes
}
