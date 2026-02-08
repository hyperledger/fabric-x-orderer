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

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric/protoutil"
)

func TestValidateNewConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	env := createConfigEnvTest(t, dir, numOfParties)

	or := verify.DefaultOrdererRules{}
	require.NoError(t, or.ValidateNewConfig(env))
}

func TestValidateNewConfig_InvalidTimeout(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	env := createConfigEnvTest(t, dir, numOfParties)

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

func TestValidateTransition_RemoveAndAddSameParty(t *testing.T) {
	or := verify.DefaultOrdererRules{}

	// create a config env with 3 parties
	env := createConfigEnvTest(t, t.TempDir(), 3)

	// create new bundle from env
	currBundle, err := channelconfig.NewBundleFromEnvelope(env, factory.GetDefault())
	require.NoError(t, err)

	// create a new config env by removing partyID=3, MaxPartyID is still 3
	env2 := proto.Clone(env).(*common.Envelope)
	payload, err := protoutil.UnmarshalPayload(env2.Payload)
	require.NoError(t, err)

	cfgEnv := &common.ConfigEnvelope{}
	require.NoError(t, proto.Unmarshal(payload.Data, cfgEnv))

	ctVal := cfgEnv.Config.ChannelGroup.Groups["Orderer"].Values["ConsensusType"]
	ct := &orderer.ConsensusType{}
	require.NoError(t, proto.Unmarshal(ctVal.Value, ct))

	shared := &config_protos.SharedConfig{}
	require.NoError(t, proto.Unmarshal(ct.Metadata, shared))

	newParties := shared.PartiesConfig[:0]
	for _, p := range shared.PartiesConfig {
		if p.PartyID != 3 {
			newParties = append(newParties, p)
		}
	}
	shared.PartiesConfig = newParties

	ct.Metadata, err = proto.Marshal(shared)
	require.NoError(t, err)
	ctVal.Value, err = proto.Marshal(ct)
	require.NoError(t, err)

	payload.Data, err = proto.Marshal(cfgEnv)
	require.NoError(t, err)
	env2.Payload, err = proto.Marshal(payload)
	require.NoError(t, err)

	// validate the transition after the removal
	require.NoError(t, or.ValidateTransition(currBundle, env2))

	// try to add partyID=3 again
	currBundle, err = channelconfig.NewBundleFromEnvelope(env2, factory.GetDefault())
	require.NoError(t, err)

	err = or.ValidateTransition(currBundle, env)
	require.Error(t, err)
	require.ErrorContains(t, err, "proposed party ID 3 must be greater than previous MaxPartyID 3")
}

func createConfigEnvTest(t *testing.T, baseDir string, numOfParties int) *common.Envelope {
	numOfShards := 1
	configPath := filepath.Join(baseDir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", baseDir})
	blockBytes, err := os.ReadFile(filepath.Join(baseDir, "bootstrap", "bootstrap.block"))
	require.NoError(t, err)

	block := protoutil.UnmarshalBlockOrPanic(blockBytes)
	env, err := protoutil.ExtractEnvelope(block, 0)
	require.NoError(t, err)

	return env
}
