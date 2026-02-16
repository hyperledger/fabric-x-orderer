/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	mockSigner "github.com/hyperledger/fabric-x-common/protoutil/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	mocksVerifier "github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
)

func TestValidateNewConfig(t *testing.T) {
	numOfParties := 1
	_, env, _, _, _, _, _, cleanup := setupOrdererRulesTest(t, numOfParties)
	defer cleanup()

	or := verify.DefaultOrdererRules{}
	require.NoError(t, or.ValidateNewConfig(env, factory.GetDefault()))
}

func TestValidateNewConfig_InvalidTimeout(t *testing.T) {
	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 1)
	defer cleanup()

	// update the batch timeout to an invalid value
	updatePb := builder.UpdateBatchTimeouts(t, configutil.NewBatchTimeoutsConfig(configutil.BatchTimeoutsConfigName.BatchCreationTimeout, "0s"))
	require.NotEmpty(t, updatePb)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	env := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	or := verify.DefaultOrdererRules{}
	err = or.ValidateNewConfig(env, factory.GetDefault())

	require.Error(t, err)
	require.Contains(t, err.Error(), "batch creation timeout")
}

func TestValidateTransition_RemoveAndAddSameParty(t *testing.T) {
	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()

	// create a config with 3 parties
	dir, currEnv, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 3)
	defer cleanup()

	// remove partyID=3, MaxPartyID is still 3
	updatePb := builder.RemoveParty(t, 3)
	require.NotEmpty(t, updatePb)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// validate the transition after the removal
	err = or.ValidateTransition(currBundle, nextEnv, bccsp)
	require.NoError(t, err)

	// try to add partyID=3 again, should fail
	nextBundle, err := channelconfig.NewBundleFromEnvelope(nextEnv, bccsp)
	require.NoError(t, err)

	err = or.ValidateTransition(nextBundle, currEnv, bccsp)
	require.Error(t, err)
	require.ErrorContains(t, err, "proposed party ID 3 must be greater than previous MaxPartyID 3")
}

func TestValidateTransition_FailedRemoveTwoParties(t *testing.T) {
	or := verify.DefaultOrdererRules{}

	// create a config with 5 parties
	dir, _, bundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 5)
	defer cleanup()

	// remove two parties
	builder.RemoveParty(t, 5)
	builder.RemoveParty(t, 4)
	updatePb := builder.ConfigUpdatePBData(t)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, bundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{Payload: nextCfgEnv.Payload, Signature: nextCfgEnv.Signature}

	// should fail because more than one party is removed
	err = or.ValidateTransition(bundle, nextEnv, factory.GetDefault())
	require.Error(t, err)
	require.ErrorContains(t, err, "more than one party removed in config tx")
}

func TestValidateTransition_FailedAddTwoParties(t *testing.T) {
	or := verify.DefaultOrdererRules{}

	// create a config with 3 parties
	dir, _, bundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 3)
	defer cleanup()

	// add 2 parties
	builder.AddNewParty(t, &protos.PartyConfig{
		CACerts:    [][]byte{[]byte("newCACert-1")},
		TLSCACerts: [][]byte{[]byte("newTLSCACert-1")},
		ConsenterConfig: &protos.ConsenterNodeConfig{
			Host: "localhost", Port: 7050, TlsCert: []byte("consenterNewCert-1"),
		},
		RouterConfig: &protos.RouterNodeConfig{
			Host: "localhost", Port: 8050, TlsCert: []byte("routerNewCert-1"),
		},
		AssemblerConfig: &protos.AssemblerNodeConfig{
			Host: "localhost", Port: 9050, TlsCert: []byte("assemblerNewCert-1"),
		},
		BatchersConfig: []*protos.BatcherNodeConfig{
			{ShardID: 1, Host: "localhost", Port: 10050, TlsCert: []byte("batcherNewCert-1")},
		},
	})

	builder.AddNewParty(t, &protos.PartyConfig{
		CACerts:    [][]byte{[]byte("newCACert-2")},
		TLSCACerts: [][]byte{[]byte("newTLSCACert-2")},
		ConsenterConfig: &protos.ConsenterNodeConfig{
			Host: "localhost", Port: 7051, TlsCert: []byte("consenterNewCert-2"),
		},
		RouterConfig: &protos.RouterNodeConfig{
			Host: "localhost", Port: 8051, TlsCert: []byte("routerNewCert-2"),
		},
		AssemblerConfig: &protos.AssemblerNodeConfig{
			Host: "localhost", Port: 9051, TlsCert: []byte("assemblerNewCert-2"),
		},
		BatchersConfig: []*protos.BatcherNodeConfig{
			{ShardID: 1, Host: "localhost", Port: 10051, TlsCert: []byte("batcherNewCert-2")},
		},
	})

	updatePb := builder.ConfigUpdatePBData(t)
	require.NotEmpty(t, updatePb)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, bundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{Payload: nextCfgEnv.Payload, Signature: nextCfgEnv.Signature}

	err = or.ValidateTransition(bundle, nextEnv, factory.GetDefault())
	require.Error(t, err)
	require.ErrorContains(t, err, "more than one party added in config tx")
}

func setupOrdererRulesTest(t *testing.T, parties int) (string, *common.Envelope, channelconfig.Resources, *configutil.ConfigUpdateBuilder, *policy.DefaultConfigUpdateProposer, identity.SignerSerializer, *requestfilter.RulesVerifier, func()) {
	t.Helper()
	dir := t.TempDir()

	configPath := filepath.Join(dir, "config.yaml")
	testutil.CreateNetwork(t, configPath, parties, 1, "TLS", "none")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	genesisBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
	blockBytes, err := os.ReadFile(genesisBlockPath)
	require.NoError(t, err)

	block := protoutil.UnmarshalBlockOrPanic(blockBytes)
	env, err := protoutil.ExtractEnvelope(block, 0)
	require.NoError(t, err)

	bundle, err := channelconfig.NewBundleFromEnvelope(env, factory.GetDefault())
	require.NoError(t, err)

	builder, cleanup := configutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)

	proposer := &policy.DefaultConfigUpdateProposer{}

	fakeSigner := &mockSigner.SignerSerializer{}
	fakeSigner.SignReturns([]byte("signature"), nil)
	fakeSigner.SerializeReturns([]byte("identity"), nil)

	verifier := requestfilter.NewRulesVerifier(nil)
	sr := &mocksVerifier.FakeStructureRule{}
	sr.VerifyAndClassifyReturns(common.HeaderType_CONFIG, nil)
	verifier.AddStructureRule(sr)

	return dir, env, bundle, builder, proposer, fakeSigner, verifier, cleanup
}
