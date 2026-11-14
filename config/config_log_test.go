/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

func TestReadConfigLogsConfigurationForEveryRole(t *testing.T) {
	const hsmPIN = "test-pin"
	e := newRejoinTestEnv(t)

	for _, role := range []struct{ name, file string }{
		{config.RouterStr, "local_config_router.yaml"},
		{config.BatcherStr, "local_config_batcher1.yaml"},
		{config.ConsensusStr, "local_config_consenter.yaml"},
		{config.AssemblerStr, "local_config_assembler.yaml"},
	} {
		t.Run(role.name, func(t *testing.T) {
			path := filepath.Join(e.dir, "config", "party1", role.file)
			testutil.EditDirectoryInNodeConfigYAML(t, path, filepath.Join(e.dir, "storage", role.name), "", 0)

			// Add a PIN to verify that it is not logged.
			node := testutil.ReadNodeConfigFromYaml(t, path)
			section := fmt.Sprintf("PKCS11: {library: /nonexistent/libsofthsm2.so, label: l, pin: %s}", hsmPIN)
			require.NoError(t, yaml.Unmarshal([]byte(section), node.GeneralConfig.BCCSP))
			require.NoError(t, utils.WriteToYAML(node, path))

			logger, logged := recordLogs()
			conf, block, err := config.ReadConfig(path, logger)
			require.NoError(t, err)
			general := conf.LocalConfig.NodeLocalConfig.GeneralConfig
			if pkcs11 := reflect.ValueOf(general.BCCSP).Elem().FieldByName("PKCS11"); pkcs11.IsValid() {
				require.Equal(t, hsmPIN, pkcs11.Elem().FieldByName("Pin").String(), "the PIN was not loaded")
			}

			entries := logged()
			local, shared := configEntries(t, entries)
			localTitle, localYAML, _ := strings.Cut(local, "\n")
			sharedTitle, sharedYAML, _ := strings.Cut(shared, "\n")
			require.Equal(t, fmt.Sprintf("Local config (%s):", role.name), localTitle)
			require.Equal(t, fmt.Sprintf("Shared config (block %d):", block.Header.Number), sharedTitle)
			for _, content := range []string{localYAML, sharedYAML} {
				require.NoError(t, yaml.Unmarshal([]byte(content), new(map[string]any)))
			}
			require.Contains(t, localYAML, fmt.Sprintf("ListenPort: %d", general.ListenPort))
			require.Contains(t, localYAML, "LocalMSPID: "+general.LocalMSPID)
			require.Contains(t, sharedYAML, fmt.Sprintf("MaxPartyID: %d", conf.SharedConfig.MaxPartyID))

			base64DER := func(pemFile string) string {
				data, err := os.ReadFile(pemFile)
				require.NoError(t, err)
				der, _ := pem.Decode(data)
				require.NotNil(t, der)
				return base64.StdEncoding.EncodeToString(der.Bytes)
			}
			require.Contains(t, sharedYAML, base64DER(general.TLSConfig.Certificate))

			privateKey := base64DER(general.TLSConfig.PrivateKey)
			for _, entry := range entries {
				require.NotContains(t, entry.Message, hsmPIN)
				require.NotContains(t, entry.Message, "-----BEGIN")
				require.NotContains(t, entry.Message, privateKey)
			}
		})
	}
}

func recordLogs() (*flogging.FabricLogger, func() []zapcore.Entry) {
	var mu sync.Mutex
	var entries []zapcore.Entry
	record := zap.Hooks(func(entry zapcore.Entry) error {
		mu.Lock()
		defer mu.Unlock()
		entries = append(entries, entry)
		return nil
	})
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), zapcore.AddSync(io.Discard), zapcore.DebugLevel)
	return flogging.NewFabricLogger(zap.New(core, record)), func() []zapcore.Entry {
		mu.Lock()
		defer mu.Unlock()
		return append([]zapcore.Entry(nil), entries...)
	}
}

func configEntries(t *testing.T, entries []zapcore.Entry) (local, shared string) {
	var messages []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Message, "Local config (") || strings.HasPrefix(entry.Message, "Shared config (") {
			require.Equal(t, zapcore.InfoLevel, entry.Level, entry.Message)
			messages = append(messages, entry.Message)
		}
	}
	require.Len(t, messages, 2)
	return messages[0], messages[1]
}
