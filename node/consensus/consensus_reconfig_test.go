/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	configrequestMocks "github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

func TestSubmitConfigConsensusNode(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	// prepare setup
	genesisBlock := utils.EmptyGenesisBlock("arma")
	setup := setupConsensusTest(t, ca, 1, genesisBlock)

	// update consensus config update proposer to return a dummy config request with header = common.HeaderType_CONFIG
	payloadBytes := []byte{1}
	configRequestEnvelope := tx.CreateStructuredConfigEnvelope(payloadBytes)
	configRequest := &protos.Request{
		Payload:   configRequestEnvelope.Payload,
		Signature: configRequestEnvelope.Signature,
	}
	mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	mockConfigUpdateProposer.ProposeConfigUpdateReturns(configRequest, nil)
	setup.consensusNodes[0].ConfigUpdateProposer = mockConfigUpdateProposer
	mockConfigRequestValidator := &configrequestMocks.FakeConfigRequestValidator{}
	mockConfigRequestValidator.ValidateConfigRequestReturns(nil)
	setup.consensusNodes[0].ConfigRequestValidator = mockConfigRequestValidator
	setup.consensusNodes[0].ConfigApplier = &NoOpDefaultConfigApplier{}

	// update consensus router config
	routerCert, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, routerCert)

	consensusNode := setup.consensusNodes[0]
	consensusNode.Config.Router = config.RouterInfo{
		PartyID:    1,
		Endpoint:   "127.0.0.1:5030",
		TLSCACerts: nil,
		TLSCert:    routerCert.Cert,
	}

	// update setup configs such that consenterNodeConfig.router will be the above routerInfo (for consistency)
	setup.configs[0].Router = consensusNode.Config.Router

	// Submit request (decision 1)
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 1)
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)

	// Submit config request (decision 2)
	_, err = createAndSubmitConfigRequest(setup.consensusNodes[0], routerCert.TLSCert, payloadBytes)
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)

	// check decision appended to the consensus ledger is a config decision, i.e. it has a config block as last block in AvailableCommonBlocks
	proposal, _, err := state.BytesToDecision(b.Data.Data[0])
	require.NotNil(t, proposal)
	require.NoError(t, err)

	header := &state.Header{}
	err = header.Deserialize(proposal.Header)
	require.NoError(t, err)

	block := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
	require.True(t, protoutil.IsConfigBlock(block))
	require.True(t, header.Num == header.DecisionNumOfLastConfigBlock)

	setup.consensusNodes[0].Stop()

	err = recoverNode(t, setup, 0, ca, block)
	require.NoError(t, err)

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.SequenceReturns(1)
	bundle.ConfigtxValidatorReturns(configtxValidator)
	setup.consensusNodes[0].Config.Bundle = bundle

	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 1)
	require.ErrorContains(t, err, "mismatch config sequence")

	err = createAndSubmitRequestWithConfigSeq(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 1, 1)
	require.NoError(t, err)

	setup.consensusNodes[0].Stop()
}

func TestSubmitConfigConsensusMultiNodes(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	// prepare setup
	genesisBlock := utils.EmptyGenesisBlock("arma")
	parties := 4
	setup := setupConsensusTest(t, ca, parties, genesisBlock)

	// update consensus config update proposer to return a dummy config request with header = common.HeaderType_CONFIG
	payloadBytes := []byte{1}
	configRequestEnvelope := tx.CreateStructuredConfigEnvelope(payloadBytes)
	configRequest := &protos.Request{
		Payload:   configRequestEnvelope.Payload,
		Signature: configRequestEnvelope.Signature,
	}
	mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	mockConfigUpdateProposer.ProposeConfigUpdateReturns(configRequest, nil)
	mockConfigRequestValidator := &configrequestMocks.FakeConfigRequestValidator{}
	mockConfigRequestValidator.ValidateConfigRequestReturns(nil)
	for i := 0; i < parties; i++ {
		setup.consensusNodes[i].ConfigUpdateProposer = mockConfigUpdateProposer
		setup.consensusNodes[i].ConfigRequestValidator = mockConfigRequestValidator
		setup.consensusNodes[i].ConfigApplier = &NoOpDefaultConfigApplier{}
	}

	// update consensus router config
	routerCert, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, routerCert)

	consensusNode := setup.consensusNodes[0]
	consensusNode.Config.Router = config.RouterInfo{
		PartyID:    1,
		Endpoint:   "127.0.0.1:5030",
		TLSCACerts: nil,
		TLSCert:    routerCert.Cert,
	}

	// update setup configs such that consenterNodeConfig.router will be the above routerInfo (for consistency)
	setup.configs[0].Router = consensusNode.Config.Router

	// Submit request (decision 1)
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[0].sk, 1, 1, digest124, 1, 1)
	require.NoError(t, err)

	for i := 0; i < parties; i++ {
		b := <-setup.listeners[i].c
		require.Equal(t, uint64(1), b.Header.Number)
	}

	// Submit config request (decision 2) and data request
	err = createAndSubmitRequest(setup.consensusNodes[0], setup.batcherNodes[1].sk, 2, 1, digest124, 1, 1)
	require.NoError(t, err)
	_, err = createAndSubmitConfigRequest(setup.consensusNodes[0], routerCert.TLSCert, payloadBytes)
	require.NoError(t, err)

	for i := 0; i < parties; i++ {
		b := <-setup.listeners[i].c
		require.Equal(t, uint64(2), b.Header.Number)

		// check decision appended to the consensus ledger is a config decision, i.e. it has a config block as last block in AvailableCommonBlocks
		proposal, _, err := state.BytesToDecision(b.Data.Data[0])
		require.NotNil(t, proposal)
		require.NoError(t, err)

		header := &state.Header{}
		err = header.Deserialize(proposal.Header)
		require.NoError(t, err)

		require.Len(t, header.AvailableCommonBlocks, 2)
		dataBlock := header.AvailableCommonBlocks[0]
		require.Equal(t, digest124, dataBlock.Header.DataHash)

		configBlock := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
		require.True(t, protoutil.IsConfigBlock(configBlock))
		require.True(t, header.Num == header.DecisionNumOfLastConfigBlock)
	}

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}

type NoOpDefaultConfigApplier struct{}

func (ca *NoOpDefaultConfigApplier) ApplyConfigToState(state *state.State, configRequest *state.ConfigRequest) (*state.State, error) {
	return state, nil
}
