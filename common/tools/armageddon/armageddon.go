/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	armaSharedConfigFileName = "shared_config.binpb"
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
	outputDir                           *string
	genConfigFile                       **os.File
	sampleConfigPath                    *string
	useTLS                              *bool
	clientSignatureVerificationRequired *bool
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
	loadSignedMode     *string
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
	baseDir                     *string
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
	cli.clientSignatureVerificationRequired = gen.Flag("clientSignatureVerificationRequired", "Specify if client signature verification is required").Bool()
	commands["generate"] = gen

	showtemplate := cli.app.Command("showtemplate", "Show the default configuration template needed to build Arma config material")
	commands["showtemplate"] = showtemplate

	version := cli.app.Command("version", "Show version information")
	commands["version"] = version

	submit := cli.app.Command("submit", "Submit txs to routers and verify the submission")
	cli.userConfigFile = submit.Flag("config", "The user configuration needed to connect with routers and assemblers").File()
	cli.transactions = submit.Flag("transactions", "The number of transactions to be sent").Int()
	cli.rate = submit.Flag("rate", "The rate specifies the number of transactions per second to be sent").Int()
	cli.txSize = submit.Flag("txSize", "The required transaction size in bytes").Default("512").Int()
	commands["submit"] = submit

	load := cli.app.Command("load", "Submit txs to routers and verify the routers have received the txs")
	cli.loadUserConfigFile = load.Flag("config", "The user configuration needed to connect with routers").File()
	cli.loadTransactions = load.Flag("transactions", "The number of transactions to be sent").Int()
	cli.loadRate = load.Flag("rate", "The rate specifies the number of transactions per second to be sent as one or more rate numbers separated by space").String()
	cli.loadTxSize = load.Flag("txSize", "The required transaction size in bytes").Int()
	cli.loadSignedMode = load.Flag("signedMode", "Signing mode has three option: none = unsigned, full = sign including the full certificate, short = sign using known certificate").String()
	commands["load"] = load

	receive := cli.app.Command("receive", "Pull txs from some assembler and report statistics")
	cli.receiveUserConfigFile = receive.Flag("config", "The user configuration needed to connect with assemblers").File()
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
	cli.blockDir = createBlock.Flag("blockOutput", "The output directory in which to place the block").String()
	cli.baseDir = createBlock.Flag("baseDir", "The directory in which all crypto and config material is saved").String()
	cli.sampleConfigPathToBlock = createBlock.Flag("sampleConfigPath", "The path to the sample config files").String()
	commands["createBlock"] = createBlock

	cli.commands = commands
}

func (cli *CLI) Run(args []string) {
	switch kingpin.MustParse(cli.app.Parse(args)) {

	// "generate" command
	case cli.commands["generate"].FullCommand():
		generateConfigAndCrypto(cli.genConfigFile, cli.outputDir, cli.sampleConfigPath, cli.clientSignatureVerificationRequired)
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
		load(cli.loadUserConfigFile, cli.loadTransactions, cli.loadRate, cli.loadTxSize, cli.loadSignedMode)

	// "receive" command
	case cli.commands["receive"].FullCommand():
		receive(cli.receiveUserConfigFile, cli.pullFromPartyId, cli.receiveOutputDir, cli.receiveExpectedNumOfTxs)

	// "createSharedConfigProto" command
	case cli.commands["createSharedConfigProto"].FullCommand():
		createSharedConfigProto(cli.sharedConfigYamlPath, cli.sharedConfigProtoDir)

	// "createBlock" command
	case cli.commands["createBlock"].FullCommand():
		createBlock(cli.sharedConfigYamlPathToBlock, cli.blockDir, cli.baseDir, cli.sampleConfigPathToBlock)
	}
}

func createBlock(sharedConfigYamlPath *string, blockDir *string, baseDir *string, sampleConfigPath *string) {
	sharedConfigProto, sharedConfigYaml, err := config.LoadSharedConfig(*sharedConfigYamlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigToBlock(sharedConfigProto, sharedConfigYaml, blockDir, baseDir, sampleConfigPath)
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

func sharedConfigToProto(sharedConfig *ordererpb.SharedConfig, outputDir *string) {
	sharedConfigBytes, err := proto.Marshal(sharedConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigBinaryPath := filepath.Join(*outputDir, armaSharedConfigFileName)
	err = os.WriteFile(sharedConfigBinaryPath, sharedConfigBytes, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing the shared config binary: %s", err)
		os.Exit(-1)
	}
}

func sharedConfigToBlock(sharedConfig *ordererpb.SharedConfig, sharedConfigYaml *config.SharedConfigYaml, blockDir *string, baseDir *string, sampleConfigPath *string) {
	sharedConfigBytes, err := proto.Marshal(sharedConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfigBinaryPath := filepath.Join(*blockDir, "shared_config.bin")
	err = os.WriteFile(sharedConfigBinaryPath, sharedConfigBytes, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing the shared config binary: %s", err)
		os.Exit(-1)
	}

	_, err = genconfig.CreateGenesisBlock(*blockDir, *baseDir, sharedConfigYaml, sharedConfigBinaryPath, *sampleConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creation bootstrap config block: %s", err)
		os.Exit(-1)
	}
}

// generateConfigAndCrypto is generating the crypto material and the configuration files in the new format.
func generateConfigAndCrypto(genConfigFile **os.File, outputDir *string, sampleConfigPath *string, clientSignatureVerificationRequired *bool) {
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

	// generate crypto material and profile for the config block
	profile, err := GenerateCryptoConfigWithProfile(networkConfig, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating crypto config: %s", err)
		os.Exit(-1)
	}

	// generate local config yaml files
	networkLocalConfig, err := genconfig.CreateArmaLocalConfig(*networkConfig, *outputDir, *outputDir, *clientSignatureVerificationRequired)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating local config: %s", err)
		os.Exit(-1)
	}

	// generate shared config yaml file
	_, err = genconfig.CreateArmaSharedConfig(*networkConfig, networkLocalConfig, *outputDir, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(*outputDir, "bootstrap", "shared_config.yaml"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading shared config: %s", err)
		os.Exit(-1)
	}

	err = createConfigBlockWithArmaSharedConfig(profile, sharedConfig, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error updating config block with new meta: %s", err)
		os.Exit(-1)
	}

	// generate user config yaml file for each party
	// user will be able to connect to each of the routers and assemblers only if it receives for each router the CA that signed the certificate of that router.
	// therefore, the CA created per party must be collected for each party, to which the router is associated.
	var tlsCACertsBytesPartiesCollection [][]byte
	for _, party := range sharedConfig.PartiesConfig {
		tlsCACertsBytesPartiesCollection = append(tlsCACertsBytesPartiesCollection, party.TLSCACerts...)
	}

	for i := range sharedConfig.PartiesConfig {
		userTLSPrivateKeyPath := filepath.Join(*outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", fmt.Sprintf("client@org%d", i+1), "tls", "client.key")
		userTLSCertPath := filepath.Join(*outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", fmt.Sprintf("client@org%d", i+1), "tls", "client.crt")
		mspDir := filepath.Join(*outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", fmt.Sprintf("client@org%d", i+1), "msp")

		userConfig, err := NewUserConfig(mspDir, userTLSPrivateKeyPath, userTLSCertPath, tlsCACertsBytesPartiesCollection, networkConfig)
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

	// generate user config yaml file for peers
	for _, peer := range profile.Application.Organizations {
		userTLSPrivateKeyPath := filepath.Join(*outputDir, "crypto", "peerOrganizations", peer.Name, "users", fmt.Sprintf("client@%s", peer.Name), "tls", "client.key")
		userTLSCertPath := filepath.Join(*outputDir, "crypto", "peerOrganizations", peer.Name, "users", fmt.Sprintf("client@%s", peer.Name), "tls", "client.crt")

		userMSPDir := filepath.Join(*outputDir, "crypto", "peerOrganizations", peer.Name, "users", fmt.Sprintf("client@%s", peer.Name), "msp")
		userConfig, err := NewUserConfig(userMSPDir, userTLSPrivateKeyPath, userTLSCertPath, tlsCACertsBytesPartiesCollection, networkConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating user config: %s", err)
			os.Exit(-1)
		}

		peerConfigDir := filepath.Join(*outputDir, "config", peer.Name)
		if err := os.MkdirAll(peerConfigDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating peer config dir: %s", err)
			os.Exit(-1)
		}

		err = utils.WriteToYAML(userConfig, filepath.Join(peerConfigDir, "user_config.yaml"))
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

	// the map is empty by the time Wait returns, so every tx arrived
	logger.Infof("Verification passed, all %d txs were received", *transactions)

	verifyAssemblerHeights(userConfig)

	// report results
	reportResults(*transactions, elapsed, txDelayTimes, numOfBlocks, *txSize)
}

// load command makes txs and sends them to all routers
func load(userConfigFile **os.File, transactions *int, rate *string, txSize *int, signedMode *string) {
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

	logger.Infof("Load command uses signed transactions mode: %s", *signedMode)

	convertedRates := make([]int, len(rates))
	for i := 0; i < len(rates); i++ {
		convertedRates[i], err = strconv.Atoi(rates[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "rate is not valid: %s", err)
			os.Exit(-1)
		}
	}
	// send txs to the routers
	for i := 0; i < len(rates); i++ {
		start := time.Now()
		SendTxsToAllAvailableRouters(userConfig, *transactions, convertedRates[i], *txSize, nil, *signedMode)
		elapsed := time.Since(start)
		reportLoadResults(*transactions, elapsed, *txSize)
	}
}

func SendTxsToAllAvailableRouters(userConfig *UserConfig, numOfTxs int, rate int, txSize int, txsMap *protectedMap, signedMode string) {
	broadcastClient := NewBroadcastTxClient(userConfig)
	err := broadcastClient.InitStreams()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init streams between client and router %v", err)
		os.Exit(3)
	}

	// create a session number (16 bytes)
	sessionNumber := make([]byte, 16)
	_, err = rand.Read(sessionNumber)
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

	// open go routines for receive response from the router
	for _, streamInfo := range broadcastClient.streamsToRouters {
		go ReceiveResponseFromRouter(userConfig, streamInfo)
	}

	var signer *crypto.ECDSASigner
	var certBytes []byte
	var org string
	var env *common.Envelope

	if signedMode == "full" || signedMode == "short" {
		org, err = signutil.GetMspIDfromDir(userConfig.MSPDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get mspID from user msp dir: %v", err)
			os.Exit(3)
		}

		signer, certBytes, err = signutil.LoadCryptoMaterialForSigner(userConfig.MSPDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load crypto materials: %v", err)
			os.Exit(3)
		}
	}

	switch signedMode {
	case "full":
		for i := 0; i < numOfTxs; i++ {
			env = tx.PrepareSignedEnvelopeWithCertificate(i, txSize, sessionNumber, signer, certBytes, org)
			if env == nil {
				fmt.Fprintf(os.Stderr, "failed to prepare envelope %d in signed mode %s", i+1, signedMode)
				os.Exit(3)
			}
			status := rl.GetToken()
			if !status {
				fmt.Fprintf(os.Stderr, "failed to send tx %d in signed mode %s", i+1, signedMode)
				os.Exit(3)
			}
			broadcastClient.SendTxToAllRouters(env)
		}
	case "short":
		for i := 0; i < numOfTxs; i++ {
			env = tx.PrepareSignedEnvelopeWithCertificateID(i, txSize, sessionNumber, signer, certBytes, org)
			if env == nil {
				fmt.Fprintf(os.Stderr, "failed to prepare envelope %d in signed mode %s", i+1, signedMode)
				os.Exit(3)
			}
			status := rl.GetToken()
			if !status {
				fmt.Fprintf(os.Stderr, "failed to send tx %d in signed mode %s", i+1, signedMode)
				os.Exit(3)
			}
			broadcastClient.SendTxToAllRouters(env)
		}
	default:
		for i := 0; i < numOfTxs; i++ {
			env = tx.PrepareUnsignedEnvelope(i, txSize, sessionNumber)
			status := rl.GetToken()
			if !status {
				fmt.Fprintf(os.Stderr, "failed to send tx %d in unsigned mode", i+1)
				os.Exit(3)
			}
			broadcastClient.SendTxToAllRouters(env)
		}
	}
	rl.Stop()

	err = broadcastClient.Stop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop broadcast client, err: %v", err)
		os.Exit(3)
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

	// pull blocks from the assembler and report statistics to a timestamped CSV file
	pullBlocksFromAssemblerAndCollectStatistics(userConfig, *pullFromPartyId, *receiveOutputDir, *expectedNumOfTxs)
	logger.Infof("Receive command finished, statistics can be found in the output directory: %v\n", *receiveOutputDir)
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
	// the broadcast client keeps sending to the routers that are up, and reconnects to the rest
	broadcastClient := NewBroadcastTxClient(userConfig)
	err := broadcastClient.InitStreams()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init streams between client and router %v", err)
		os.Exit(3)
	}

	// open go routines for receive response from the router
	for _, streamInfo := range broadcastClient.streamsToRouters {
		go receiveAckFromRouter(userConfig, streamInfo)
	}

	// create a session number (16 bytes)
	sessionNumber := make([]byte, 16)
	_, err = rand.Read(sessionNumber)
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
		sendTx(txsMap, broadcastClient, i, txSize, sessionNumber)
	}
	rl.Stop()

	logger.Infof("all %d txs were sent to the routers", numOfTxs)

	err = broadcastClient.Stop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop broadcast client, err: %v", err)
		os.Exit(3)
	}
}

// receiveAckFromRouter is submit's variant of ReceiveResponseFromRouter that checks the ack status.
// TODO: it covers only the initial streams, since TryReconnect restarts ReceiveResponseFromRouter.
func receiveAckFromRouter(userConfig *UserConfig, streamInfo *StreamInfo) {
	for {
		select {
		case <-streamInfo.stopChan:
			streamInfo.logger.Infof("Stop receiveAckFromRouter go routine")
			return
		default:
			response, err := streamInfo.stream.Recv()
			if err != nil {
				streamInfo.logger.Infof("Failed to receive response from router, close receive go routine, mark router %s as broken and start reconnection, err: %v", streamInfo.endpoint, err)
				streamInfo.SetIsBroken(true)
				streamInfo.TryReconnect(userConfig)
				return
			}

			if response.GetStatus() != common.Status_SUCCESS {
				streamInfo.logger.Warnf("Router %s rejected a tx with status %v", streamInfo.endpoint, response.GetStatus())
			}
		}
	}
}

func pullBlocksFromAssemblerAndCollectStatistics(userConfig *UserConfig, pullFromPartyId int, receiveOutputDir string, expectedNumOfTxs int) {
	puller := newAssemblerBlockPuller(userConfig, pullFromPartyId)
	if err := puller.connect(0); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
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

	// every second read the statistics and send it to manageStatistics
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

	// pull blocks from the assembler with reconnect logic on failure
	go func() {
		logger.Infof("starting pulling blocks from the assembler")
		var txsTotal int

		for {
			blockWithTime := BlockWithTime{
				block:        puller.next(),
				acceptedTime: time.Now(),
			}
			blockChan <- blockWithTime
			txsTotal += len(blockWithTime.block.Data.Data)

			logger.Debugf("block with %d txs was pulled from the assembler, overall %d txs were received at this moment", len(blockWithTime.block.Data.Data), txsTotal)

			if expectedNumOfTxs > 0 && expectedNumOfTxs <= txsTotal {
				logger.Infof("overall %d txs were received, finished pulling", txsTotal)
				puller.close()
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
				sumOfTxsSize += len(protoutil.MarshalOrPanic(env))
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

// assemblerHeightTimeout must exceed how long an assembler can be down and then catching up.
const assemblerHeightTimeout = 3 * time.Minute

// verifyAssemblerHeights polls, because assemblers commit asynchronously.
func verifyAssemblerHeights(userConfig *UserConfig) {
	numOfAssemblers := len(userConfig.AssemblerEndpoints)
	deadline := time.Now().Add(assemblerHeightTimeout)

	for {
		heights := make([]uint64, 0, numOfAssemblers)
		for partyID := 1; partyID <= numOfAssemblers; partyID++ {
			height, err := newestBlockNumber(userConfig, partyID)
			if err != nil {
				logger.Warnf("failed to read the block height of assembler %d: %v", partyID, err)
				break
			}

			heights = append(heights, height)
		}

		if numOfAssemblers > 0 && len(heights) == numOfAssemblers && slices.Min(heights) == slices.Max(heights) {
			logger.Infof("all %d assemblers reached block %d", numOfAssemblers, heights[0])
			return
		}

		if time.Now().After(deadline) {
			logger.Warnf("assemblers are not at the same block height after %v, per party: %v", assemblerHeightTimeout, heights)
			return
		}

		time.Sleep(time.Second)
	}
}

func newestBlockNumber(userConfig *UserConfig, partyID int) (uint64, error) {
	requestEnvelope, err := createDeliverRequest(userConfig, newestSeekInfo())
	if err != nil {
		return 0, fmt.Errorf("failed to create a request envelope: %w", err)
	}

	endpoint := userConfig.AssemblerEndpoints[partyID-1]

	conn, err := assemblerClientConfig(userConfig).Dial(endpoint)
	if err != nil {
		return 0, fmt.Errorf("failed to create a gRPC client connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := ab.NewAtomicBroadcastClient(conn).Deliver(context.TODO())
	if err != nil {
		return 0, fmt.Errorf("failed to create a deliver stream: %w", err)
	}
	defer func() { _ = stream.CloseSend() }()

	if err := stream.Send(requestEnvelope); err != nil {
		return 0, fmt.Errorf("failed to send a request envelope: %w", err)
	}

	block, err := pullBlock(stream, endpoint)
	if err != nil {
		return 0, err
	}

	return block.GetHeader().GetNumber(), nil
}

func assemblerClientConfig(userConfig *UserConfig) comm.ClientConfig {
	return comm.ClientConfig{
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               userConfig.TLSPrivateKey,
			Certificate:       userConfig.TLSCertificate,
			RequireClientCert: userConfig.UseTLSAssembler == "mTLS",
			UseTLS:            userConfig.UseTLSAssembler != "none",
			ServerRootCAs:     append([][]byte{}, userConfig.TLSCACerts...),
		},
		DialTimeout: time.Second * 5,
	}
}

// assemblerBlockPuller keeps a deliver stream to one assembler alive across its restarts.
type assemblerBlockPuller struct {
	userConfig   *UserConfig
	clientConfig comm.ClientConfig
	partyID      int
	endpoint     string
	conn         *grpc.ClientConn
	stream       ab.AtomicBroadcast_DeliverClient
	lastBlockNum uint64
}

func newAssemblerBlockPuller(userConfig *UserConfig, partyID int) *assemblerBlockPuller {
	return &assemblerBlockPuller{
		userConfig:   userConfig,
		clientConfig: assemblerClientConfig(userConfig),
		partyID:      partyID,
		endpoint:     userConfig.AssemblerEndpoints[partyID-1],
	}
}

func (p *assemblerBlockPuller) connect(startSeq uint64) error {
	requestEnvelope, err := createDeliverRequestWithSeekInfo(p.userConfig, startSeq)
	if err != nil {
		return fmt.Errorf("failed to create a request envelope: %w", err)
	}

	conn, err := p.clientConfig.Dial(p.endpoint)
	if err != nil {
		return fmt.Errorf("failed to create a gRPC client connection to assembler %d: %w", p.partyID, err)
	}

	stream, err := ab.NewAtomicBroadcastClient(conn).Deliver(context.TODO())
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to create a deliver stream to assembler %d: %w", p.partyID, err)
	}

	if err := stream.Send(requestEnvelope); err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to send a request envelope to assembler %d: %w", p.partyID, err)
	}

	p.conn, p.stream = conn, stream

	return nil
}

// next never returns nil, it retries until the assembler delivers a usable block.
func (p *assemblerBlockPuller) next() *common.Block {
	for {
		block, err := pullBlock(p.stream, p.endpoint)
		if err != nil {
			if errors.Is(err, errStreamBroken) {
				logger.Warnf("lost connection to assembler %d: %v — will retry in 1s", p.partyID, err)
				p.close()
				p.reconnect()
			} else {
				logger.Warnf("skipping an unusable response from assembler %d: %v", p.partyID, err)
			}

			continue
		}

		if num := block.GetHeader().GetNumber(); num > 0 {
			p.lastBlockNum = num

			return block
		}
	}
}

func (p *assemblerBlockPuller) reconnect() {
	for {
		time.Sleep(1 * time.Second)

		if err := p.connect(p.lastBlockNum + 1); err != nil {
			logger.Warnf("reconnect to assembler %d failed: %v — retrying", p.partyID, err)
			continue
		}

		logger.Infof("reconnected to assembler %d successfully, resuming from block %d", p.partyID, p.lastBlockNum+1)

		return
	}
}

func (p *assemblerBlockPuller) close() {
	if p.conn == nil {
		return
	}

	_ = p.stream.CloseSend()
	_ = p.conn.Close()
}

// errStreamBroken marks a pullBlock error that means the stream is unusable, not just the block.
var errStreamBroken = errors.New("deliver stream broken")

func pullBlock(stream ab.AtomicBroadcast_DeliverClient, endpointToPullFrom string) (*common.Block, error) {
	resp, err := stream.Recv()
	if err != nil {
		err = fmt.Errorf("failed to receive a deliver response from %s: %w", endpointToPullFrom, err)
		return nil, errors.Mark(err, errStreamBroken)
	}

	block := resp.GetBlock()

	if block == nil {
		return nil, fmt.Errorf("received a non-block message from %s: %v", endpointToPullFrom, resp)
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, fmt.Errorf("received empty block from %s", endpointToPullFrom)
	}

	return block, nil
}

func calculateDelayOfTx(data []byte, acceptedTime time.Time) time.Duration {
	sendTime := tx.ExtractTimestampFromTx(data)
	delayTime := acceptedTime.Sub(sendTime)
	return delayTime
}

func sendTx(txsMap *protectedMap, broadcastClient *BroadcastTxClient, i int, txSize int, sessionNumber []byte) {
	env := tx.PrepareUnsignedEnvelope(i, txSize, sessionNumber)
	data, _ := tx.GetDataFromEnvelope(env)
	if txsMap != nil {
		logger.Debugf("Add tx %x to the map", data)
		txsMap.Add(string(data))
	}
	broadcastClient.SendTxToAllRouters(env)
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

// manageStatistics manages a statistics queue and writes statistics to a timestamped CSV file for this run
func manageStatistics(receiveOutputDir string, statisticChan <-chan Statistics, stopChan <-chan bool, startTime float64, expectedTxs int, pullFrom int, timeIntervalToSampleStat time.Duration) {
	filePath := path.Join(receiveOutputDir, "statistics.csv")
	logger.Infof("Statistics are written to: %v\n", filePath)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
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

func newestSeekInfo() *ab.SeekInfo {
	return &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT,
	}
}

func createDeliverRequestWithSeekInfo(userConfig *UserConfig, startSeq uint64) (*common.Envelope, error) {
	return createDeliverRequest(userConfig, nextSeekInfo(startSeq))
}

func createDeliverRequest(userConfig *UserConfig, seekInfo *ab.SeekInfo) (*common.Envelope, error) {
	signer, err := signutil.CreateSignerForUser(userConfig.MSPDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create signer for user: %v", err)
	}

	var tlsCertHash []byte
	if userConfig.UseTLSAssembler == "mTLS" {
		block, _ := pem.Decode(userConfig.TLSCertificate)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("failed to decode PEM certificate")
		}
		tlsCertHash = util.ComputeSHA256(block.Bytes)
	}

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"arma",
		signer,
		seekInfo,
		int32(0),
		uint64(0),
		tlsCertHash,
	)

	return requestEnvelope, err
}

// receiveResponseFromAssembler returns only when every tx sent has been seen, so the caller sets any deadline.
func receiveResponseFromAssembler(userConfig *UserConfig, txsMap *protectedMap, expectedNumOfTxs int) (int, float64) {
	// arbitrarily choose the first assembler to pull blocks from
	puller := newAssemblerBlockPuller(userConfig, 1)
	if err := puller.connect(0); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(3)
	}
	defer puller.close()

	// pull blocks from assembler
	numOfBlocksCalculated := 0
	numOfTxsCalculated := 0
	var sumOfDelayTimes float64
	for {
		block := puller.next()

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

// createConfigBlockWithArmaSharedConfig creates a new config block with the updated shared config and writes it to the output directory.
func createConfigBlockWithArmaSharedConfig(profile *configtxgen.Profile, sharedConfig *ordererpb.SharedConfig, outputDir string) error {
	// Convert the shared config to its protobuf representation and write it to the output directory.
	sharedConfigProtoDir := filepath.Join(outputDir, "bootstrap")
	sharedConfigToProto(sharedConfig, &sharedConfigProtoDir)

	profile.Orderer.Arma.Path = filepath.Join(sharedConfigProtoDir, armaSharedConfigFileName)

	block, err := configtxgen.GetOutputBlock(profile, "arma")
	if err != nil {
		return errors.Wrap(err, "failed to get output block")
	}

	err = configtxgen.WriteOutputBlock(block, filepath.Join(outputDir, "bootstrap", "bootstrap.block"))
	if err != nil {
		return errors.Wrap(err, "error writing output block")
	}

	return nil
}
