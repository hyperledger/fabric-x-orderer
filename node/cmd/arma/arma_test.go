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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.ibm.com/Yacov-Manevich/ARMA/node"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRouter(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	err = node.NodeConfigToYAML(node.RouterNodeConfig{
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		PartyID:            1,
		Shards:             []node.ShardInfo{{ShardId: 1, Batchers: []node.BatcherInfo{{PartyID: 1, TLSCACerts: []node.RawBytes{ca.CertBytes()}}}}},
	}, configPath)
	require.NoError(t, err)

	originalLogger := logger
	defer func() {
		logger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
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
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	err = node.NodeConfigToYAML(node.AssemblerNodeConfig{
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Shards:             []node.ShardInfo{{ShardId: 1, Batchers: []node.BatcherInfo{{PartyID: 1, TLSCACerts: []node.RawBytes{ca.CertBytes()}}}}},
	}, configPath)
	require.NoError(t, err)

	originalLogger := logger
	defer func() {
		logger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Assembler listening on [::]:6023" {
			wg.Done()
		}
		return nil
	}))

	cli := NewCLI()
	cli.Run([]string{"assembler", "--config", configPath})
	wg.Wait()
}

func TestBatcher(t *testing.T) {
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

	err = node.NodeConfigToYAML(node.BatcherNodeConfig{
		SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: keyBytes}),
		ShardId:            1,
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Consenters: []node.ConsenterInfo{
			{PartyID: 1, TLSCACerts: []node.RawBytes{ca.CertBytes()}, Endpoint: "noroute:80"}, {PartyID: 2, TLSCACerts: []node.RawBytes{ca.CertBytes()}, Endpoint: "noroute:80"},
		},
		Shards: []node.ShardInfo{
			{ShardId: 1, Batchers: []node.BatcherInfo{
				{PartyID: 1, TLSCACerts: []node.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert},
				{PartyID: 2, TLSCACerts: []node.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert},
			}}},
	}, configPath)
	require.NoError(t, err)

	originalLogger := logger
	defer func() {
		logger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
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

	err = node.NodeConfigToYAML(node.ConsenterNodeConfig{
		SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: keyBytes}),
		PartyId:            1,
		Directory:          dir,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Consenters: []node.ConsenterInfo{
			{PartyID: 1, PublicKey: pem.EncodeToMemory(&pem.Block{Bytes: pkBytes})},
		},
		Shards: []node.ShardInfo{
			{ShardId: 1, Batchers: []node.BatcherInfo{
				{PartyID: 1, TLSCACerts: []node.RawBytes{ca.CertBytes()}, TLSCert: ckp.Cert, PublicKey: pem.EncodeToMemory(&pem.Block{Bytes: pkBytes})},
			}}},
	}, configPath)
	require.NoError(t, err)

	originalLogger := logger
	defer func() {
		logger = originalLogger
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "Consensus listening on [::]:6025" {
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

	err = os.WriteFile(fPath, []byte("the little fox jumped over the lazy dog"), 0600)
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
