/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
)

func TestValidateNewConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	numOfShards := 1

	configPath := filepath.Join(dir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	genesisBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
	blockBytes, err := os.ReadFile(genesisBlockPath)
	require.NoError(t, err)

	block := protoutil.UnmarshalBlockOrPanic(blockBytes)
	env, err := protoutil.ExtractEnvelope(block, 0)
	require.NoError(t, err)

	or := verify.DefaultOrdererRules{}
	require.NoError(t, or.ValidateNewConfig(env))
}

func TestValidateNewConfig_InvalidTimeout(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	numOfShards := 1

	configPath := filepath.Join(dir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	genesisBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
	blockBytes, err := os.ReadFile(genesisBlockPath)
	require.NoError(t, err)

	block := protoutil.UnmarshalBlockOrPanic(blockBytes)
	env, err := protoutil.ExtractEnvelope(block, 0)
	require.NoError(t, err)

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	require.NoError(t, err)

	cfgEnv := &common.ConfigEnvelope{}
	require.NoError(t, proto.Unmarshal(payload.Data, cfgEnv))

	consensusVal := cfgEnv.Config.ChannelGroup.Groups["Orderer"].Values["ConsensusType"]
	ct := &orderer.ConsensusType{}
	require.NoError(t, proto.Unmarshal(consensusVal.Value, ct))

	shared := &config_protos.SharedConfig{}
	err = proto.Unmarshal(ct.Metadata, shared)
	require.NoError(t, err)

	// set an invalid value
	shared.BatchingConfig.BatchTimeouts.BatchCreationTimeout = "0s"

	ct.Metadata, err = proto.Marshal(shared)
	require.NoError(t, err)

	consensusVal.Value, err = proto.Marshal(ct)
	require.NoError(t, err)

	payload.Data, err = proto.Marshal(cfgEnv)
	require.NoError(t, err)

	env.Payload, err = proto.Marshal(payload)
	require.NoError(t, err)

	or := verify.DefaultOrdererRules{}
	err = or.ValidateNewConfig(env)

	require.Error(t, err)
	require.Contains(t, err.Error(), "batch creation timeout")
}
