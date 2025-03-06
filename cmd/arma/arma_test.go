package arma

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"

	"github.ibm.com/decentralized-trust-research/arma/common/utils"

	"github.com/hyperledger/fabric/protoutil"

	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	"github.ibm.com/decentralized-trust-research/arma/node/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRouter(t *testing.T) {
	testLogger = flogging.MustGetLogger("arma")
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	err = config.NodeConfigToYAML(config.RouterNodeConfig{
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		PartyID:            1,
		Shards:             []config.ShardInfo{{ShardId: 1, Batchers: []config.BatcherInfo{{Endpoint: "127.0.0.1:80", PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}}}}},
		UseTLS:             true,
	}, configPath)
	require.NoError(t, err)

	originalLogger := testLogger
	defer func() {
		testLogger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Router listening on [::]:6022" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"router", "--config", configPath})
	wg.Wait()
}

func TestAssembler(t *testing.T) {
	testLogger = flogging.MustGetLogger("arma")
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	err = config.NodeConfigToYAML(config.AssemblerNodeConfig{
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Shards:             []config.ShardInfo{{ShardId: 1, Batchers: []config.BatcherInfo{{PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}}}}},
	}, configPath)
	require.NoError(t, err)

	originalLogger := testLogger
	defer func() {
		testLogger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Assembler listening on [::]:6023" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"assembler", "--config", configPath})
	wg.Wait()
}

func TestAssemblerWithBlock(t *testing.T) {
	testLogger = flogging.MustGetLogger("arma")
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	err = config.NodeConfigToYAML(config.AssemblerNodeConfig{
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Shards:             []config.ShardInfo{{ShardId: 1, Batchers: []config.BatcherInfo{{PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}}}}},
		ListenAddress:      "127.0.0.1:6043",
	}, configPath)
	require.NoError(t, err)

	block := utils.EmptyGenesisBlock("arma")
	blockBytes := protoutil.MarshalOrPanic(block)
	blockPath := filepath.Join(dir, "genesis.block")
	err = os.WriteFile(blockPath, blockBytes, 0o600)
	require.NoError(t, err)

	originalLogger := testLogger
	defer func() {
		testLogger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Assembler listening on 127.0.0.1:6043" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"assembler", "--config", configPath})
	wg.Wait()
}

func TestBatcher(t *testing.T) {
	testLogger = flogging.MustGetLogger("arma")
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	err = config.NodeConfigToYAML(config.BatcherNodeConfig{
		SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: keyBytes}),
		ShardId:            1,
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Consenters: []config.ConsenterInfo{
			{PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}, Endpoint: "noroute:80"}, {PartyID: 2, TLSCACerts: []config.RawBytes{ca.CertBytes()}, Endpoint: "noroute:80"},
		},
		Shards: []config.ShardInfo{
			{ShardId: 1, Batchers: []config.BatcherInfo{
				{PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert, Endpoint: "127.0.0.1:80"},
				{PartyID: 2, TLSCACerts: []config.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert, Endpoint: "127.0.0.1:80"},
			}},
		},
	}, configPath)
	require.NoError(t, err)

	originalLogger := testLogger
	defer func() {
		testLogger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Batcher listening on [::]:6024" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"batcher", "--config", configPath})
	wg.Wait()
}

func TestConsensus(t *testing.T) {
	testLogger = flogging.MustGetLogger("arma")
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	pkBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)

	err = config.NodeConfigToYAML(config.ConsenterNodeConfig{
		SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: keyBytes}),
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Consenters: []config.ConsenterInfo{
			{PartyID: 1, PublicKey: pem.EncodeToMemory(&pem.Block{Bytes: pkBytes})},
		},
		Shards: []config.ShardInfo{
			{ShardId: 1, Batchers: []config.BatcherInfo{
				{PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert, PublicKey: pem.EncodeToMemory(&pem.Block{Bytes: pkBytes})},
			}},
		},
	}, configPath)
	require.NoError(t, err)

	originalLogger := testLogger
	defer func() {
		testLogger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Consensus listening on [::]:6025" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"consensus", "--config", configPath})
	wg.Wait()
}

func TestConsensusWithBlock(t *testing.T) {
	testLogger = flogging.MustGetLogger("arma")
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	pkBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)

	err = config.NodeConfigToYAML(config.ConsenterNodeConfig{
		SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: keyBytes}),
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Consenters: []config.ConsenterInfo{
			{PartyID: 1, PublicKey: pem.EncodeToMemory(&pem.Block{Bytes: pkBytes})},
		},
		Shards: []config.ShardInfo{
			{ShardId: 1, Batchers: []config.BatcherInfo{
				{PartyID: 1, TLSCACerts: []config.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert, PublicKey: pem.EncodeToMemory(&pem.Block{Bytes: pkBytes})},
			}},
		},
		ListenAddress: "127.0.0.1:6045",
	}, configPath)
	require.NoError(t, err)

	block := utils.EmptyGenesisBlock("arma")
	blockBytes := protoutil.MarshalOrPanic(block)
	blockPath := filepath.Join(dir, "genesis.block")
	err = os.WriteFile(blockPath, blockBytes, 0o600)
	require.NoError(t, err)

	originalLogger := testLogger
	defer func() {
		testLogger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Consensus listening on 127.0.0.1:6045" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"consensus", "--config", configPath})
	wg.Wait()
}

func TestCLI(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	fPath := filepath.Join(dir, "cli-test")

	err = os.WriteFile(fPath, []byte("the little fox jumped over the lazy dog"), 0o600)
	require.NoError(t, err)

	cli := NewCLI()
	cli.Command("test", "run a router node", func(configFile *os.File) {
		stat, err := configFile.Stat()
		require.NoError(t, err)

		content := make([]byte, stat.Size())
		_, err = io.ReadFull(configFile, content)
		require.NoError(t, err)

		assert.Equal(t, []byte("the little fox jumped over the lazy dog"), content)
	})
	cli.Run([]string{"test", "--config", fPath})
}
