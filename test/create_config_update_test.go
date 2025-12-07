/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

func TestCreateConfigBlockUpdate(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, 1, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// Create config update
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	configUpdatePbData := CreateConfigUpdate(t, dir, genesisBlockPath)
	require.NotEmpty(t, configUpdatePbData)

	// Create a dummy config update envelope
	configUpdateEnvelope := tx.CreateStructuredConfigUpdateEnvelope(configUpdatePbData)
	require.NotNil(t, configUpdateEnvelope)
}

// TODO: enable update options
func CreateConfigUpdate(t *testing.T, dir string, genesisBlockPath string) []byte {
	// Compile configtxlator tool
	configtxlatorPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-common/cmd/configtxlator", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, configtxlatorPath)
	defer gexec.CleanupBuildArtifacts()

	// Decode the genesis block from proto to json representation
	jsonPath := filepath.Join(dir, "config_block.json")
	cmd := exec.Command(configtxlatorPath, "proto_decode", "--input", genesisBlockPath, "--type", "common.Block", "--output", jsonPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, jsonPath)

	// Read the block in the json representation
	jsonData, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	require.NotEmpty(t, jsonData)

	data := make(map[string]any)
	err = json.Unmarshal(jsonData, &data)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Change some value
	configData := getNestedJSONValue(data, "data", "data", "payload", "data", "config")
	require.NotNil(t, configData)

	jsonConfigData, err := json.Marshal(configData)
	require.NoError(t, err)

	jsonConfigPath := filepath.Join(dir, "config.json")
	err = os.WriteFile(jsonConfigPath, jsonConfigData, 0o644)
	require.NoError(t, err)

	overwriteNestedJSONValue(configData.(map[string]any), 5050, "channel_group", "groups", "Orderer", "values", "BatchSize", "value", "max_message_count")
	maxMessageCount := getNestedJSONValue(configData.(map[string]any), "channel_group", "groups", "Orderer", "values", "BatchSize", "value", "max_message_count")
	require.Equal(t, 5050, maxMessageCount.(int))

	modifiedJsonConfigData, err := json.Marshal(configData)
	require.NoError(t, err)

	modifiedJsonConfigPath := filepath.Join(dir, "modified_config.json")
	err = os.WriteFile(modifiedJsonConfigPath, modifiedJsonConfigData, 0o644)
	require.NoError(t, err)

	pbConfigPath := filepath.Join(dir, "config.pb")
	cmd = exec.Command(configtxlatorPath, "proto_encode", "--input", jsonConfigPath, "--type", "common.Config", "--output", pbConfigPath)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, pbConfigPath)

	modifiedPbConfigPath := filepath.Join(dir, "modified_config.pb")
	cmd = exec.Command(configtxlatorPath, "proto_encode", "--input", modifiedJsonConfigPath, "--type", "common.Config", "--output", modifiedPbConfigPath)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, modifiedPbConfigPath)

	// Compute update
	configUpdatePbPath := filepath.Join(dir, "config_update.pb")
	cmd = exec.Command(configtxlatorPath, "compute_update", "--channel_id", "arma", "--original", pbConfigPath, "--updated", modifiedPbConfigPath, "--output", configUpdatePbPath)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, configUpdatePbPath)

	configUpdatePbData, err := os.ReadFile(configUpdatePbPath)
	require.NoError(t, err)
	require.NotEmpty(t, configUpdatePbData)

	return configUpdatePbData
}

func getNestedJSONValue(data map[string]any, path ...string) any {
	curr := any(data)
	for i := 0; i < len(path); {
		p := path[i]
		switch node := curr.(type) {
		case map[string]any:
			v, ok := node[p]
			if !ok {
				return nil
			}
			curr = v
			i++
		case []any:
			if len(node) == 0 {
				return nil
			}
			curr = node[0]
		default:
			return nil
		}
	}
	return curr
}

func overwriteNestedJSONValue(data map[string]any, value any, path ...string) {
	curr := any(data)
	for i := 0; i < len(path); {
		p := path[i]
		switch node := curr.(type) {
		case map[string]any:
			if i == len(path)-1 {
				node[p] = value
				return
			}
			v, ok := node[p]
			if !ok {
				return
			}
			curr = v
			i++
		case []any:
			if len(node) == 0 {
				return
			}
			if i == len(path)-1 {
				node[0] = value
				return
			}
			curr = node[0]
		default:
			return
		}
	}
}
