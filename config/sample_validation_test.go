/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSampleLocalConfigs(t *testing.T) {
	err := filepath.WalkDir("sample", func(path string, d os.DirEntry, err error) error {
		require.NoError(t, err)

		if d.IsDir() {
			return nil
		}

		matched, err := filepath.Match("local_config*.yaml", d.Name())
		require.NoError(t, err)

		if !matched {
			return nil
		}

		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			var node yaml.Node
			err = yaml.Unmarshal(data, &node)
			require.NoError(t, err)

			// Skip PKCS11 in non-pkcs11 builds.
			removePKCS11Config(&node)

			data, err = yaml.Marshal(&node)
			require.NoError(t, err)

			decoder := yaml.NewDecoder(bytes.NewReader(data))
			decoder.KnownFields(true)

			localConfig := &config.NodeLocalConfig{}
			err = decoder.Decode(localConfig)
			require.NoError(t, err)
		})

		return nil
	})
	require.NoError(t, err)
}

func removePKCS11Config(node *yaml.Node) {
	if len(node.Content) == 0 {
		return
	}

	general := yamlMappingValue(node.Content[0], "General")
	bccsp := yamlMappingValue(general, "BCCSP")
	if bccsp == nil || bccsp.Kind != yaml.MappingNode {
		return
	}

	for i := 0; i < len(bccsp.Content); i += 2 {
		if bccsp.Content[i].Value == "PKCS11" {
			bccsp.Content = append(bccsp.Content[:i], bccsp.Content[i+2:]...)
			return
		}
	}
}

func yamlMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}

	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}

	return nil
}
