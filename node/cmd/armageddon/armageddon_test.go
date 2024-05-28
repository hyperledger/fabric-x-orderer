package armageddon

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"github.ibm.com/Yacov-Manevich/ARMA/node"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon to create config files in a folder structure
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Make a tx, send to all the routers and pull block from assemblers to observe the block have been committed
func TestArmageddon(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/Yacov-Manevich/ARMA/node/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	runArmaNodes(t, dir, armaBinaryPath, readyChan)
	startTimeout := time.After(10 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}

	// 4.
	//send a tx to the routers
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		sendTxToRouters(t, dir, &wg)
	}()
	wg.Wait()

	// receive block from the assembler
	receiveResponseFromAssemblers(t, dir)
}

func runArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan struct{}) {
	nodes := map[string][]string{
		"router":    []string{"router_node_config.yaml"},
		"batcher":   []string{"batcher_node_1_config.yaml", "batcher_node_2_config.yaml"},
		"consensus": []string{"consenter_node_config.yaml"},
		"assembler": []string{"assembler_node_config.yaml"},
	}

	for i := 0; i < 4; i++ {
		partyDir := path.Join(dir, fmt.Sprintf("Party%d", i+1))
		for name, files := range nodes {
			for j := 0; j < len(files); j++ {
				nodeConfigPath := path.Join(partyDir, files[j])
				editDirectoryInNodeConfigYAML(t, name, nodeConfigPath)
				go runNode(t, name, armaBinaryPath, nodeConfigPath, readyChan)
			}
		}
	}
}

func runNode(t *testing.T, name string, armaBinaryPath string, nodeConfigPath string, readyChan chan struct{}) {
	cmd := exec.Command(armaBinaryPath, name, "--config", nodeConfigPath)
	require.NotNil(t, cmd)

	sess, err := gexec.Start(cmd, nil, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		match, err := gbytes.Say("listening on").Match(sess.Err)
		require.NoError(t, err)
		return match
	}, 10*time.Second, 10*time.Millisecond)

	readyChan <- struct{}{}
	return
}

// generateInputConfigFileForArmageddon create a config.yaml file which is the input to armageddon generate command.
// the configuration includes 4 parties and 2 batchers for each party.
func generateInputConfigFileForArmageddon(t *testing.T, path string) {
	var parties []Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := getAvailablePort(t)
		consenterPort, llc := getAvailablePort(t)
		routerPort, llr := getAvailablePort(t)
		batcher1Port, llb1 := getAvailablePort(t)
		batcher2Port, llb2 := getAvailablePort(t)

		party := Party{
			ID:                uint16(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
	}

	network := Network{
		Parties: parties,
	}

	err := writeToYAML(network, path)
	require.NoError(t, err)

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}
}

func getAvailablePort(t *testing.T) (port string, ll net.Listener) {
	ll, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := ll.Addr().String()
	_, portS, err := net.SplitHostPort(endpoint)
	require.NoError(t, err)
	return portS, ll
}

func writeToYAML(config interface{}, path string) error {
	rnc, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, rnc, 0644)
	if err != nil {
		return err
	}

	return nil
}

func readRouterNodeConfigFromYaml(t *testing.T, path string) *node.RouterNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	routerConfig := node.RouterNodeConfig{}
	err = yaml.Unmarshal(configBytes, &routerConfig)
	require.NoError(t, err)
	return &routerConfig
}

func readAssemblerNodeConfigFromYaml(t *testing.T, path string) *node.AssemblerNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	assemblerConfig := node.AssemblerNodeConfig{}
	err = yaml.Unmarshal(configBytes, &assemblerConfig)
	require.NoError(t, err)
	return &assemblerConfig
}

func readConsenterNodeConfigFromYaml(t *testing.T, path string) *node.ConsenterNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	consenterConfig := node.ConsenterNodeConfig{}
	err = yaml.Unmarshal(configBytes, &consenterConfig)
	require.NoError(t, err)
	return &consenterConfig
}

func readBatcherNodeConfigFromYaml(t *testing.T, path string) *node.BatcherNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	batcherConfig := node.BatcherNodeConfig{}
	err = yaml.Unmarshal(configBytes, &batcherConfig)
	require.NoError(t, err)
	return &batcherConfig
}

// editDirectoryInNodeConfigYAML fill the Directory field in all relevant config structures. This must be done before running Arma nodes
func editDirectoryInNodeConfigYAML(t *testing.T, name string, path string) {
	dir, err := os.MkdirTemp("", "Directory_"+fmt.Sprintf(name))
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	switch name {
	case "batcher":
		batcherConfig := readBatcherNodeConfigFromYaml(t, path)
		batcherConfig.Directory = dir
		node.NodeConfigToYAML(batcherConfig, path)
	case "consensus":
		consenterConfig := readConsenterNodeConfigFromYaml(t, path)
		consenterConfig.Directory = dir
		node.NodeConfigToYAML(consenterConfig, path)
	case "assembler":
		assemblerConfig := readAssemblerNodeConfigFromYaml(t, path)
		assemblerConfig.Directory = dir
		node.NodeConfigToYAML(assemblerConfig, path)
	}
}

func nextSeekInfo(startSeq uint64) *ab.SeekInfo {
	return &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: startSeq}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT,
	}
}

func sendTxToRouter(t *testing.T, dir string, wg *sync.WaitGroup, routerNum int) {
	// read router config
	routerConfig := readRouterNodeConfigFromYaml(t, path.Join(dir, fmt.Sprintf("Party%d", routerNum+1), "router_node_config.yaml"))
	var serverRootCAs [][]byte
	for _, rawBytes := range routerConfig.Shards[0].Batchers[routerNum].TLSCACerts {
		byteSlice := []byte(rawBytes)
		serverRootCAs = append(serverRootCAs, byteSlice)
	}

	// create a gRPC connection to the router
	gRPCRouterClient := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               routerConfig.TLSPrivateKeyFile,
			Certificate:       routerConfig.TLSCertificateFile,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     serverRootCAs,
		},
		DialTimeout: time.Second * 5,
	}
	gRPCRouterClientConn, err := gRPCRouterClient.Dial("127.0.0.1:" + trimHostFromEndpoint(routerConfig.ListenAddress))
	require.NoError(t, err)

	// open a broadcast stream
	var stream ab.AtomicBroadcast_BroadcastClient
	stream, err = ab.NewAtomicBroadcastClient(gRPCRouterClientConn).Broadcast(context.TODO())
	require.NoError(t, err)

	// send tx
	err = stream.Send(&common.Envelope{
		Payload: []byte("data transaction")})
	require.NoError(t, err)
	t.Logf("tx was sent to router: %v", routerNum+1)

	// check for acknowledgment
	ack, err := stream.Recv()
	require.Equal(t, ack.Status.String(), "SUCCESS")
	t.Logf("tx was received, router: %v sent ack: %v", routerNum+1, ack)

	wg.Done()
	gRPCRouterClientConn.Close()
	stream.CloseSend()
}

func sendTxToRouters(t *testing.T, dir string, wg *sync.WaitGroup) {
	for i := 0; i < 4; i++ {
		sendTxToRouter(t, dir, wg, i)
	}
}

func receiveResponseFromAssemblers(t *testing.T, dir string) {
	for i := 0; i < 4; i++ {
		// read assembler config
		assemblerConfig := readAssemblerNodeConfigFromYaml(t, path.Join(dir, fmt.Sprintf("Party%d", i+1), "assembler_node_config.yaml"))
		var serverRootCAs [][]byte
		for _, rawBytes := range assemblerConfig.Shards[0].Batchers[i].TLSCACerts {
			byteSlice := []byte(rawBytes)
			serverRootCAs = append(serverRootCAs, byteSlice)
		}

		// create a gRPC connection to the assembler
		gRPCAssemblerClient := comm.ClientConfig{
			AsyncConnect: true,
			KaOpts: comm.KeepaliveOptions{
				ClientInterval: time.Hour,
				ClientTimeout:  time.Hour,
			},
			SecOpts: comm.SecureOptions{
				Key:               assemblerConfig.TLSPrivateKeyFile,
				Certificate:       assemblerConfig.TLSCertificateFile,
				RequireClientCert: true,
				UseTLS:            true,
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
		require.NoError(t, err)

		var stream ab.AtomicBroadcast_DeliverClient
		var gRPCAssemblerClientConn *grpc.ClientConn
		endpointToPullFrom := "127.0.0.1:" + trimHostFromEndpoint(assemblerConfig.ListenAddress)

		// create a delivery stream and ask for pulling blocks, try again in case of an error
		// do it until the overall timeout of 10 seconds is reached
		deliveryTimeout := time.After(10 * time.Second)
	ForLoop:
		for {
			select {
			case <-deliveryTimeout:
				require.Fail(t, fmt.Sprintf("timeout has occurred, assembler %v failed to send block in time", i+1))
			default:
				gRPCAssemblerClientConn, err = gRPCAssemblerClient.Dial(endpointToPullFrom)
				if err != nil {
					t.Logf("failed connecting to %s: %v, try again", endpointToPullFrom, err)
					time.Sleep(2 * time.Second)
					continue
				}

				abc := ab.NewAtomicBroadcastClient(gRPCAssemblerClientConn)

				stream, err = abc.Deliver(context.TODO())
				if err != nil {
					t.Logf("failed creating Deliver stream to %s: %v, try again", endpointToPullFrom, err)
					gRPCAssemblerClientConn.Close()
					time.Sleep(2 * time.Second)
					continue
				}

				err = stream.Send(requestEnvelope)
				if err != nil {
					t.Logf("failed sending request envelope to %s: %v, try again", endpointToPullFrom, err)
					stream.CloseSend()
					gRPCAssemblerClientConn.Close()
					time.Sleep(2 * time.Second)
					continue
				}

				t.Logf("request envelope was sent to assembler %v", i+1)

				block, err := pullBlock(stream, endpointToPullFrom, gRPCAssemblerClientConn)
				if err != nil {
					t.Logf("err: %v, send request envelope to assembler %v again", err, i+1)
					time.Sleep(2 * time.Second)
					continue
				}
				require.NotNil(t, block)
				env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[0])
				require.NoError(t, err)
				require.Equal(t, env.Payload, []byte("data transaction"))
				t.Logf("block was pulled successfully from assembler %v", i+1)
				break ForLoop
			}
		}
	}
}

func pullBlock(stream ab.AtomicBroadcast_DeliverClient, endpointToPullFrom string, gRPCAssemblerClientConn *grpc.ClientConn) (*common.Block, error) {
	resp, err := stream.Recv()
	if err != nil {
		stream.CloseSend()
		gRPCAssemblerClientConn.Close()
		return nil, fmt.Errorf("failed receiving block for %s from %s: %v", "arma", endpointToPullFrom, err)
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
