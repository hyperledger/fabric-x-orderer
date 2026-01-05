/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configutil

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func createSigner(privateKeyPath string) (*crypto.ECDSASigner, error) {
	// Read the private key
	keyBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key, err: %v", err)
	}

	// Create a ECDSA Singer
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create ECDSA Signer, err: %v", err)
	}

	return (*crypto.ECDSASigner)(privateKey), nil
}

func CreateConfigUpdate(t *testing.T, dir string, genesisBlockPath string) []byte {
	// Compile configtxlator tool
	configtxlatorPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-common/cmd/configtxlator", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, configtxlatorPath)

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

func CreateConfigTX(t *testing.T, dir string, numOfParties int, genesisBlockPath string, submittingParty int) *common.Envelope {
	// Create ConfigUpdateBytes
	configUpdatePbBytes := CreateConfigUpdate(t, dir, genesisBlockPath)
	require.NotNil(t, configUpdatePbBytes)

	// create ConfigUpdateEnvelope
	configUpdateEnvelope := &common.ConfigUpdateEnvelope{
		ConfigUpdate: configUpdatePbBytes,
		Signatures:   []*common.ConfigSignature{},
	}

	// sign with majority admins (for 4 parties the majority is 3)
	for i := 0; i < (numOfParties/2)+1; i++ {
		// Read admin of organization i
		adminSigner, adminCertBytes, err := createCertAndSigner(dir, i+1)
		require.NoError(t, err)
		require.NotNil(t, adminSigner)
		require.NotNil(t, adminCertBytes)

		sId := &msp.SerializedIdentity{
			Mspid:   fmt.Sprintf("org%d", i+1),
			IdBytes: adminCertBytes,
		}

		sigHeader, err := protoutil.NewSignatureHeader(adminSigner)
		require.NoError(t, err)

		sigHeader.Creator = protoutil.MarshalOrPanic(sId)

		configSig := &common.ConfigSignature{
			SignatureHeader: protoutil.MarshalOrPanic(sigHeader),
		}
		configSig.Signature, err = adminSigner.Sign(util.ConcatenateBytes(configSig.SignatureHeader, configUpdateEnvelope.ConfigUpdate))
		require.NoError(t, err)

		configUpdateEnvelope.Signatures = append(configUpdateEnvelope.Signatures, configSig)
	}

	configUpdateEnvelopeBytes, err := proto.Marshal(configUpdateEnvelope)
	require.NoError(t, err)

	// Wrap the ConfigUpdateEnvelope with an Envelope signed by the admin of the submitting party
	submittingAdminSigner, submittingAdminCert, err := createCertAndSigner(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, submittingAdminSigner)
	require.NotNil(t, submittingAdminCert)

	payload := tx.CreatePayloadWithConfigUpdate(configUpdateEnvelopeBytes, submittingAdminCert, fmt.Sprintf("org%d", submittingParty))
	require.NotNil(t, payload)
	env, err := tx.CreateSignedEnvelope(payload, submittingAdminSigner)
	require.NoError(t, err)
	require.NotNil(t, env)

	return env
}

func createCertAndSigner(dir string, submittingParty int) (*crypto.ECDSASigner, []byte, error) {
	keyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", submittingParty), "users", "admin", "msp", "keystore", "priv_sk")
	submittingAdminSigner, err := createSigner(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating a signer, err: %s", err)
	}

	certPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", submittingParty), "users", "admin", "msp", "signcerts", fmt.Sprintf("Admin@Org%d-cert.pem", submittingParty))
	submittingAdminCert, err := os.ReadFile(certPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating a certificate, err: %s", err)
	}

	return submittingAdminSigner, submittingAdminCert, nil
}
