/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	mockSigner "github.com/hyperledger/fabric-x-common/protoutil/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	mocksVerifier "github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestValidateNewConfig(t *testing.T) {
	numOfParties := 1
	_, env, _, _, _, _, _, cleanup := setupOrdererRulesTest(t, numOfParties)
	defer cleanup()

	or := verify.DefaultOrdererRules{}
	require.NoError(t, or.ValidateNewConfig(env, factory.GetDefault(), types.PartyID(1)))
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
	err = or.ValidateNewConfig(env, factory.GetDefault(), types.PartyID(1))

	require.Error(t, err)
	require.Contains(t, err.Error(), "batch creation timeout")
}

func TestValidateNewConfig_BFTParams(t *testing.T) {
	dir, env, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 1)
	defer cleanup()

	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()
	partyID := types.PartyID(1)

	// validate the initial config
	require.NoError(t, or.ValidateNewConfig(env, bccsp, partyID))

	// set an invalid smartbft config value
	updatePb := builder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.LeaderHeartbeatCount, "0"))
	require.NotEmpty(t, updatePb)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	env = &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// validate the new config, expect error
	err = or.ValidateNewConfig(env, bccsp, partyID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "smartbft config validation failed")
}

func TestValidateNewConfig_InvalidRequestMaxBytes(t *testing.T) {
	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 1)
	defer cleanup()

	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()

	// set smartbft RequestMaxBytes to a value smaller than BatchingConfig RequestMaxBytes
	updatePb := builder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.RequestMaxBytes, "1"))
	require.NotEmpty(t, updatePb)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, 1, updatePb)
	req := &comm.Request{
		Payload:   updateEnv.Payload,
		Signature: updateEnv.Signature,
	}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	env := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	err = or.ValidateNewConfig(env, bccsp, types.PartyID(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "smartbft RequestMaxBytes must be equal or greater than BatchingConfig RequestMaxBytes")
}

func TestValidateNewConfig_InvalidOrdererEndpoint(t *testing.T) {
	_, env, _, _, _, _, _, cleanup := setupOrdererRulesTest(t, 1)
	defer cleanup()

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	require.NoError(t, err)

	cfgEnv := &common.ConfigEnvelope{}
	require.NoError(t, proto.Unmarshal(payload.Data, cfgEnv))

	endpointsVal := cfgEnv.Config.ChannelGroup.Groups["Orderer"].Groups["org1"].Values["Endpoints"]

	oa := &common.OrdererAddresses{}
	require.NoError(t, proto.Unmarshal(endpointsVal.Value, oa))

	// remove broadcast
	var addresses []string
	for _, a := range oa.Addresses {
		if !strings.Contains(a, ",broadcast,") {
			addresses = append(addresses, a)
		}
	}
	oa.Addresses = addresses
	endpointsVal.Value, err = proto.Marshal(oa)
	require.NoError(t, err)

	payload.Data, err = proto.Marshal(cfgEnv)
	require.NoError(t, err)

	env.Payload, err = proto.Marshal(payload)
	require.NoError(t, err)

	or := verify.DefaultOrdererRules{}
	err = or.ValidateNewConfig(env, factory.GetDefault(), types.PartyID(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing broadcast endpoint")
}

func TestValidateNewConfig_ConsenterConsistency(t *testing.T) {
	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 1)
	defer cleanup()

	// create a valid config update first
	cert := []byte("fake1-tls-cert")
	updatePb := builder.UpdateConsensusTLSCert(t, types.PartyID(1), cert)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	env := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// change TLSCert in consenter_mapping to a different value
	payload := &common.Payload{}
	err = proto.Unmarshal(env.Payload, payload)
	require.NoError(t, err)

	cfgEnv := &common.ConfigEnvelope{}
	require.NoError(t, proto.Unmarshal(payload.Data, cfgEnv))

	orderersVal := cfgEnv.Config.ChannelGroup.Groups["Orderer"].Values["Orderers"]
	orderers := &common.Orderers{}
	require.NoError(t, proto.Unmarshal(orderersVal.Value, orderers))

	// change the TLS cert in consenter_mapping to create mismatch
	orderers.ConsenterMapping[0].ServerTlsCert = []byte("fake2-tls-cert")
	orderers.ConsenterMapping[0].ClientTlsCert = []byte("fake2-tls-cert")

	orderersVal.Value, err = proto.Marshal(orderers)
	require.NoError(t, err)

	payload.Data, err = proto.Marshal(cfgEnv)
	require.NoError(t, err)

	env.Payload, err = proto.Marshal(payload)
	require.NoError(t, err)

	or := verify.DefaultOrdererRules{}
	err = or.ValidateNewConfig(env, factory.GetDefault(), types.PartyID(1))

	require.Error(t, err)
	require.Contains(t, err.Error(), "TLS certificate mismatch for party 1")
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

	// add two parties
	_, netInfo1 := builder.PrepareAndAddNewParty(t, dir)
	defer func() {
		for _, ni := range netInfo1 {
			if ni != nil {
				ni.Close()
			}
		}
	}()
	_, netInfo2 := builder.PrepareAndAddNewParty(t, dir)
	defer func() {
		for _, ni := range netInfo2 {
			if ni != nil {
				ni.Close()
			}
		}
	}()

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

func TestValidateTransition_ModifyOneParty(t *testing.T) {
	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()

	// create a config with 2 parties
	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 2)
	defer cleanup()

	// modify party 1 TLS cert
	newCert := generateConsenterTLSCert(t, dir, 1)
	updatePb := builder.UpdateConsensusTLSCert(t, types.PartyID(1), newCert)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// should succeed, one party modified
	err = or.ValidateTransition(currBundle, nextEnv, bccsp)
	require.NoError(t, err)
}

func TestValidateTransition_FailedModifyTwoParties(t *testing.T) {
	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()
	// create a config with 3 parties

	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 3)
	defer cleanup()

	// modify two parties TLS certs
	builder.UpdateConsensusTLSCert(t, types.PartyID(1), generateConsenterTLSCert(t, dir, 1))
	builder.UpdateConsensusTLSCert(t, types.PartyID(2), generateConsenterTLSCert(t, dir, 2))
	updatePb := builder.ConfigUpdatePBData(t)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// should fail, more than one party modified
	err = or.ValidateTransition(currBundle, nextEnv, bccsp)
	require.Error(t, err)
	require.ErrorContains(t, err, "more than one party modified in config tx")
}

func TestValidateTransition_FailedAddAndModify(t *testing.T) {
	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()

	// create a config with 2 parties
	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 2)
	defer cleanup()

	// add a new party
	_, netInfo := builder.PrepareAndAddNewParty(t, dir)
	defer func() {
		for _, ni := range netInfo {
			if ni != nil {
				ni.Close()
			}
		}
	}()

	// also modify an existing party
	newCert := generateConsenterTLSCert(t, dir, 1)
	builder.UpdateConsensusTLSCert(t, types.PartyID(1), newCert)
	updatePb := builder.ConfigUpdatePBData(t)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// should fail, add and modify in same config tx
	err = or.ValidateTransition(currBundle, nextEnv, bccsp)
	require.Error(t, err)
	require.ErrorContains(t, err, "only one party can be changed in a config tx")
}

func TestValidateTransition_FailedRemoveAndModify(t *testing.T) {
	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()

	// create a config with 3 parties
	dir, _, currBundle, builder, proposer, signer, verifier, cleanup := setupOrdererRulesTest(t, 3)
	defer cleanup()

	// remove party 3
	builder.RemoveParty(t, 3)

	// also modify party 1
	newCert := generateConsenterTLSCert(t, dir, 1)
	builder.UpdateConsensusTLSCert(t, types.PartyID(1), newCert)
	updatePb := builder.ConfigUpdatePBData(t)

	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier)
	require.NoError(t, err)

	nextEnv := &common.Envelope{
		Payload:   nextCfgEnv.Payload,
		Signature: nextCfgEnv.Signature,
	}

	// should fail, remove and modify in same config tx
	err = or.ValidateTransition(currBundle, nextEnv, bccsp)
	require.Error(t, err)
	require.ErrorContains(t, err, "only one party can be changed in a config tx")
}

func TestValidateTransition_ChannelID(t *testing.T) {
	or := verify.DefaultOrdererRules{}
	bccsp := factory.GetDefault()

	_, env, currBundle, _, _, _, _, cleanup := setupOrdererRulesTest(t, 2)
	defer cleanup()

	payload := &common.Payload{}
	require.NoError(t, proto.Unmarshal(env.Payload, payload))

	ch := &common.ChannelHeader{}
	require.NoError(t, proto.Unmarshal(payload.Header.ChannelHeader, ch))

	// change channel ID
	ch.ChannelId = "different-channel"
	payload.Header.ChannelHeader = protoutil.MarshalOrPanic(ch)
	nextEnv := &common.Envelope{
		Payload:   protoutil.MarshalOrPanic(payload),
		Signature: env.Signature,
	}

	err := or.ValidateTransition(currBundle, nextEnv, bccsp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "channel ID cannot change")
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

func generateConsenterTLSCert(t *testing.T, dir string, partyID int) []byte {
	t.Helper()

	org := fmt.Sprintf("org%d", partyID)

	tlsCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", org, "tlsca", "tlsca-cert.pem")
	tlsCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", org, "tlsca", "priv_sk")

	tlsDir := filepath.Join(dir, "crypto", "ordererOrganizations", org, "orderers", fmt.Sprintf("party%d", partyID), "consenter", "tls")
	newKeyPath := filepath.Join(tlsDir, "key.pem")

	newCert, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", tlsDir, newKeyPath, []string{"127.0.0.1"})
	require.NoError(t, err)

	return newCert
}
