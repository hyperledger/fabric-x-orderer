/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/config/generate"

	"github.com/stretchr/testify/require"
)

func TestSharedConfigLoading(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")
	err = armageddon.GenerateCryptoConfig(&networkConfig, dir)
	require.NoError(t, err)

	// 2.
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)
	// check that all nodes know the same boostrap file
	bootstrapPath := networkLocalConfig.PartiesLocalConfig[0].AssemblerLocalConfig.GeneralConfig.Bootstrap.File
	for _, party := range networkLocalConfig.PartiesLocalConfig {
		require.Equal(t, bootstrapPath, party.RouterLocalConfig.GeneralConfig.Bootstrap.File)
		for _, batcher := range party.BatchersLocalConfig {
			require.Equal(t, bootstrapPath, batcher.GeneralConfig.Bootstrap.File)
		}
		require.Equal(t, bootstrapPath, party.ConsenterLocalConfig.GeneralConfig.Bootstrap.File)
		require.Equal(t, bootstrapPath, party.AssemblerLocalConfig.GeneralConfig.Bootstrap.File)
	}

	// 3.
	networkSharedConfig, err := generate.CreateArmaSharedConfig(networkConfig, networkLocalConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkSharedConfig)

	// 4.
	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	require.NoError(t, err)
	require.NotNil(t, sharedConfig)
	require.NotNil(t, sharedConfig.BatchingConfig)
	require.NotNil(t, sharedConfig.ConsensusConfig)
	require.NotNil(t, sharedConfig.PartiesConfig)
	require.Equal(t, len(sharedConfig.PartiesConfig), len(networkConfig.Parties))

	// check that all certificates are valid x509 certificates
	for _, partyConfig := range sharedConfig.PartiesConfig {
		cert, err := parsex509Cert(partyConfig.RouterConfig.TlsCert)
		require.NotNil(t, cert)
		require.NoError(t, err)

		for _, batcher := range partyConfig.BatchersConfig {
			cert, err = parsex509Cert(batcher.TlsCert)
			require.NotNil(t, cert)
			require.NoError(t, err)
			cert, err = parsex509Cert(batcher.SignCert)
			require.NotNil(t, cert)
			require.NoError(t, err)
		}

		cert, err = parsex509Cert(partyConfig.ConsenterConfig.TlsCert)
		require.NotNil(t, cert)
		require.NoError(t, err)
		cert, err = parsex509Cert(partyConfig.ConsenterConfig.SignCert)
		require.NotNil(t, cert)
		require.NoError(t, err)

		cert, err = parsex509Cert(partyConfig.AssemblerConfig.TlsCert)
		require.NotNil(t, cert)
		require.NoError(t, err)
	}
}

func parsex509Cert(certBytes []byte) (*x509.Certificate, error) {
	pbl, _ := pem.Decode(certBytes)
	if pbl == nil || pbl.Bytes == nil {
		return nil, errors.Errorf("no pem content for cert")
	}
	if pbl.Type != "CERTIFICATE" && pbl.Type != "PRIVATE KEY" {
		return nil, errors.Errorf("unexpected pem type, got a %s", strings.ToLower(pbl.Type))
	}

	cert, err := x509.ParseCertificate(pbl.Bytes)
	return cert, err
}
