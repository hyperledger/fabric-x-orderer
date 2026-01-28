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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var partiesConfigPath = []string{"channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata", "PartiesConfig"}

type (
	batchSizeConfigName     string
	batchTimeoutsConfigName string
	smartBFTConfigName      string
)

var BatchSizeConfigName = struct {
	MaxMessageCount   batchSizeConfigName
	AbsoluteMaxBytes  batchSizeConfigName
	PreferredMaxBytes batchSizeConfigName
}{
	MaxMessageCount:   batchSizeConfigName("MaxMessageCount"),
	AbsoluteMaxBytes:  batchSizeConfigName("AbsoluteMaxBytes"),
	PreferredMaxBytes: batchSizeConfigName("PreferredMaxBytes"),
}

var BatchTimeoutsConfigName = struct {
	AutoRemoveTimeout     batchTimeoutsConfigName
	BatchCreationTimeout  batchTimeoutsConfigName
	FirstStrikeThreshold  batchTimeoutsConfigName
	SecondStrikeThreshold batchTimeoutsConfigName
}{
	AutoRemoveTimeout:     batchTimeoutsConfigName("AutoRemoveTimeout"),
	BatchCreationTimeout:  batchTimeoutsConfigName("BatchCreationTimeout"),
	FirstStrikeThreshold:  batchTimeoutsConfigName("FirstStrikeThreshold"),
	SecondStrikeThreshold: batchTimeoutsConfigName("SecondStrikeThreshold"),
}

var SmartBFTConfigName = struct {
	DecisionsPerLeader            smartBFTConfigName
	IncomingMessageBufferSize     smartBFTConfigName
	LeaderHeartbeatCount          smartBFTConfigName
	LeaderHeartbeatTimeout        smartBFTConfigName
	LeaderRotation                smartBFTConfigName
	NumOfTicksBehindBeforeSyncing smartBFTConfigName
	RequestAutoRemoveTimeout      smartBFTConfigName
	RequestBatchMaxBytes          smartBFTConfigName
	RequestBatchMaxCount          smartBFTConfigName
	RequestBatchMaxInterval       smartBFTConfigName
	RequestComplainTimeout        smartBFTConfigName
	RequestForwardTimeout         smartBFTConfigName
	RequestMaxBytes               smartBFTConfigName
	RequestPoolSize               smartBFTConfigName
	RequestPoolSubmitTimeout      smartBFTConfigName
	SpeedUpViewChange             smartBFTConfigName
	SyncOnStart                   smartBFTConfigName
	ViewChangeResendInterval      smartBFTConfigName
	ViewChangeTimeout             smartBFTConfigName
}{
	DecisionsPerLeader:            smartBFTConfigName("DecisionsPerLeader"),
	IncomingMessageBufferSize:     smartBFTConfigName("IncomingMessageBufferSize"),
	LeaderHeartbeatCount:          smartBFTConfigName("LeaderHeartbeatCount"),
	LeaderHeartbeatTimeout:        smartBFTConfigName("LeaderHeartbeatTimeout"),
	LeaderRotation:                smartBFTConfigName("LeaderRotation"),
	NumOfTicksBehindBeforeSyncing: smartBFTConfigName("NumOfTicksBehindBeforeSyncing"),
	RequestAutoRemoveTimeout:      smartBFTConfigName("RequestAutoRemoveTimeout"),
	RequestBatchMaxBytes:          smartBFTConfigName("RequestBatchMaxBytes"),
	RequestBatchMaxCount:          smartBFTConfigName("RequestBatchMaxCount"),
	RequestBatchMaxInterval:       smartBFTConfigName("RequestBatchMaxInterval"),
	RequestComplainTimeout:        smartBFTConfigName("RequestComplainTimeout"),
	RequestForwardTimeout:         smartBFTConfigName("RequestForwardTimeout"),
	RequestMaxBytes:               smartBFTConfigName("RequestMaxBytes"),
	RequestPoolSize:               smartBFTConfigName("RequestPoolSize"),
	RequestPoolSubmitTimeout:      smartBFTConfigName("RequestPoolSubmitTimeout"),
	SpeedUpViewChange:             smartBFTConfigName("SpeedUpViewChange"),
	SyncOnStart:                   smartBFTConfigName("SyncOnStart"),
	ViewChangeResendInterval:      smartBFTConfigName("ViewChangeResendInterval"),
	ViewChangeTimeout:             smartBFTConfigName("ViewChangeTimeout"),
}

type batchSizeConfig struct {
	name  batchSizeConfigName
	value int
}

func (b batchSizeConfig) Value() int {
	return b.value
}

func (b batchSizeConfig) Name() string {
	return string(b.name)
}

func NewBatchSizeConfig(name batchSizeConfigName, value int) *batchSizeConfig {
	return &batchSizeConfig{name: name, value: value}
}

func (b batchTimeoutsConfig) Name() string {
	return string(b.name)
}

func (b batchTimeoutsConfig) Value() string {
	return b.value
}

type batchTimeoutsConfig struct {
	name  batchTimeoutsConfigName
	value string
}

func NewBatchTimeoutsConfig(name batchTimeoutsConfigName, value string) *batchTimeoutsConfig {
	return &batchTimeoutsConfig{name: name, value: value}
}

type smartBFTConfig struct {
	name  smartBFTConfigName
	value any
}

func (s smartBFTConfig) Name() string {
	return string(s.name)
}

func (s smartBFTConfig) Value() any {
	return s.value
}

func NewSmartBFTConfig[T string | bool](name smartBFTConfigName, value T) *smartBFTConfig {
	return &smartBFTConfig{name: name, value: value}
}

func createSigner(privateKeyPath string) (*crypto.ECDSASigner, error) {
	// Read the private key
	keyBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key, err: %v", err)
	}

	// Create a ECDSA Signer
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create ECDSA Signer, err: %v", err)
	}

	return (*crypto.ECDSASigner)(privateKey), nil
}

type ConfigUpdateBuilder struct {
	configtxlatorPath string
	configDir         string
	jsonConfigPath    string
	configData        map[string]any
	maxPartiesNum     int
}

func NewConfigUpdateBuilder(t *testing.T, configDir string, lastConfigBlockPath string) (*ConfigUpdateBuilder, func()) {
	// Compile configtxlator tool
	configtxlatorPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/testutil/configtxlator", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, configtxlatorPath)

	// Get the config data in json representation
	configBlockData := getJSONConfigBlockData(t, configDir, configtxlatorPath, lastConfigBlockPath)

	configDataPath := []string{"data", "data[0]", "payload", "data", "config"}
	configData := getNestedJSONValue(t, configBlockData, configDataPath...)

	jsonConfigData, err := json.Marshal(configData)
	require.NoError(t, err)

	jsonConfigPath := filepath.Join(configDir, "config.json")
	err = os.WriteFile(jsonConfigPath, jsonConfigData, 0o644)
	require.NoError(t, err)

	return &ConfigUpdateBuilder{
		configtxlatorPath: configtxlatorPath,
		configDir:         configDir,
		jsonConfigPath:    jsonConfigPath,
		configData:        configData.(map[string]any),
	}, gexec.CleanupBuildArtifacts
}

func getJSONConfigBlockData(t *testing.T, dir string, configtxlatorPath string, genesisBlockPath string) map[string]any {
	// Decode the genesis block from proto to json representation
	jsonPath := filepath.Join(dir, "config_block.json")
	cmd := exec.Command(configtxlatorPath, "proto_decode", "--input", genesisBlockPath, "--type", "common.Block", "--output", jsonPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))

	// configtxlator.DecodeProto("common.Block", MustOpen(genesisBlockPath), MustCreate(jsonPath))
	require.FileExists(t, jsonPath)

	// Read the block in the json representation
	jsonData, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	require.NotEmpty(t, jsonData)

	data := make(map[string]any)
	err = json.Unmarshal(jsonData, &data)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	return data
}

func (c *ConfigUpdateBuilder) createConfigUpdate(t *testing.T, modifiedJSONConfigData map[string]any) []byte {
	modifiedConfigData, err := json.Marshal(modifiedJSONConfigData)
	require.NoError(t, err)

	modifiedJsonConfigPath := filepath.Join(c.configDir, "modified_config.json")
	err = os.WriteFile(modifiedJsonConfigPath, modifiedConfigData, 0o644)
	require.NoError(t, err)

	pbConfigPath := filepath.Join(c.configDir, "config.pb")
	cmd := exec.Command(c.configtxlatorPath, "proto_encode", "--input", c.jsonConfigPath, "--type", "common.Config", "--output", pbConfigPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, pbConfigPath)

	modifiedPbConfigPath := filepath.Join(c.configDir, "modified_config.pb")
	cmd = exec.Command(c.configtxlatorPath, "proto_encode", "--input", modifiedJsonConfigPath, "--type", "common.Config", "--output", modifiedPbConfigPath)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, modifiedPbConfigPath)

	// Compute update
	configUpdatePbPath := filepath.Join(c.configDir, "config_update.pb")
	cmd = exec.Command(c.configtxlatorPath, "compute_update", "--channel_id", "arma", "--original", pbConfigPath, "--updated", modifiedPbConfigPath, "--output", configUpdatePbPath)
	output, err = cmd.CombinedOutput()
	require.NoError(t, err, "Command failed with output: %s", string(output))
	require.FileExists(t, configUpdatePbPath)

	configUpdatePbData, err := os.ReadFile(configUpdatePbPath)
	require.NoError(t, err)
	require.NotEmpty(t, configUpdatePbData)

	return configUpdatePbData
}

func (c *ConfigUpdateBuilder) UpdateBatchSizeConfig(t *testing.T, batchSizeConfig *batchSizeConfig) []byte {
	overwriteNestedJSONValue(t, c.configData, batchSizeConfig.Value(), "channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata", "BatchingConfig", "BatchSize", batchSizeConfig.Name())
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateBatchTimeouts(t *testing.T, batchTimeoutsConfig *batchTimeoutsConfig) []byte {
	overwriteNestedJSONValue(t, c.configData, batchTimeoutsConfig.Value(), "channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata", "BatchingConfig", "BatchTimeouts", batchTimeoutsConfig.Name())
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateSmartBFTConfig(t *testing.T, smartBFTConfig *smartBFTConfig) []byte {
	overwriteNestedJSONValue(t, c.configData, smartBFTConfig.Value(), "channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata", "ConsensusConfig", "SmartBFTConfig", smartBFTConfig.Name())
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateBatcherSignCert(t *testing.T, partyID types.PartyID, shardID types.ShardID, cert []byte) []byte {
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			batchersConfig := partyMap["BatchersConfig"].([]any)
			for _, bc := range batchersConfig {
				bcMap := bc.(map[string]any)
				if uint32(shardID) == uint32(bcMap["shardID"].(float64)) {
					bcMap["sign_cert"] = cert
					found = true
					break
				}
			}
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateConsenterSignCert(t *testing.T, partyID types.PartyID, cert []byte) []byte {
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			consenterConfig := partyMap["ConsenterConfig"].(map[string]any)
			consenterConfig["sign_cert"] = cert
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdatePartyTLSCACerts(t *testing.T, partyID types.PartyID, tlsCACerts [][]byte) []byte {
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			partyMap["TLSCACerts"] = tlsCACerts
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdatePartyCACerts(t *testing.T, partyID types.PartyID, caCerts [][]byte) []byte {
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			partyMap["CACerts"] = caCerts
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateOrderingEndpoint(t *testing.T, partyID types.PartyID, host string, port int) []byte {
	// Change the endpoint value for the given partyID
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			consenterConfig := partyMap["ConsenterConfig"].(map[string]any)
			consenterConfig["host"] = host
			consenterConfig["port"] = (float64)(port)
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)

	consenterMapping := getNestedJSONValue(t, c.configData, "channel_group", "groups", "Orderer", "values", "Orderers", "value", "consenter_mapping")
	mappingList := consenterMapping.([]any)

	found = false
	for _, mapping := range mappingList {
		mappingMap := mapping.(map[string]any)
		if uint32(partyID) == uint32(mappingMap["id"].(float64)) {
			mappingMap["host"] = host
			mappingMap["port"] = (float64)(port)
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in consenter_mapping", partyID)

	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateRouterEndpoint(t *testing.T, partyID types.PartyID, host string, port int) []byte {
	// Change the endpoint value for the given partyID
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			routerConfig := partyMap["RouterConfig"].(map[string]any)
			routerConfig["host"] = host
			routerConfig["port"] = (float64)(port)
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)

	c.UpdateOrgEndpoints(t, partyID)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateAssemblerEndpoint(t *testing.T, partyID types.PartyID, host string, port int) []byte {
	// Change the endpoint value for the given partyID
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	found := false
	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			assemblerConfig := partyMap["AssemblerConfig"].(map[string]any)
			assemblerConfig["host"] = host
			assemblerConfig["port"] = (float64)(port)
			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)

	c.UpdateOrgEndpoints(t, partyID)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateBatcherEndpoint(t *testing.T, partyID types.PartyID, shardID types.ShardID, host string, port int) []byte {
	// Change the endpoint value for the given partyID
	found := false
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			batchersConfig := partyMap["BatchersConfig"].([]any)
			for _, bc := range batchersConfig {
				bcMap := bc.(map[string]any)
				if uint32(shardID) == uint32(bcMap["shardID"].(float64)) {
					bcMap["host"] = host
					bcMap["port"] = (float64)(port)
					found = true
					break
				}
			}
		}
	}

	require.True(t, found, "PartyID %d or ShardID %d not found in PartiesConfig", partyID, shardID)

	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) UpdateOrgEndpoints(t *testing.T, partyID types.PartyID) []byte {
	found := false
	var (
		broadcastHost string
		broadcastPort int
		deliverHost   string
		deliverPort   int
	)

	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	for _, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			assemblerConfig := partyMap["AssemblerConfig"].(map[string]any)
			deliverHost = assemblerConfig["host"].(string)
			deliverPort = int(assemblerConfig["port"].(float64))

			routerConfig := partyMap["RouterConfig"].(map[string]any)
			broadcastHost = routerConfig["host"].(string)
			broadcastPort = int(routerConfig["port"].(float64))

			found = true
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)
	// Update OrdererAddresses
	org := fmt.Sprintf("org%d", partyID)
	addresses := []string{
		fmt.Sprintf("id=%d,msp-id=%s,api=broadcast,host=%s,port=%d", partyID, org, broadcastHost, broadcastPort),
		fmt.Sprintf("id=%d,msp-id=%s,api=deliver,host=%s,port=%d", partyID, org, deliverHost, deliverPort),
	}

	overwriteNestedJSONValue(t, c.configData, addresses, "channel_group", "groups", "Orderer", "groups", org, "values", "Endpoints", "value", "addresses")
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) AddNewParty(t *testing.T, newParty *protos.PartyConfig) []byte {
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	if c.maxPartiesNum == 0 {
		c.maxPartiesNum = len(partiesConfigList)
	}

	c.maxPartiesNum++
	newPartyID := c.maxPartiesNum

	batchersConfig := []map[string]any{}
	for _, bc := range newParty.BatchersConfig {
		batchersConfig = append(batchersConfig,
			map[string]any{
				"shardID":   bc.ShardID,
				"host":      bc.Host,
				"port":      bc.Port,
				"sign_cert": bc.SignCert,
				"tls_cert":  bc.TlsCert,
			})
	}

	partiesConfigList = append(partiesConfigList,
		map[string]any{
			"PartyID":    float64(newPartyID),
			"CACerts":    newParty.CACerts,
			"TLSCACerts": newParty.TLSCACerts,

			"ConsenterConfig": map[string]any{
				"host":      newParty.ConsenterConfig.Host,
				"port":      newParty.ConsenterConfig.Port,
				"sign_cert": newParty.ConsenterConfig.SignCert,
				"tls_cert":  newParty.ConsenterConfig.TlsCert,
			},
			"RouterConfig": map[string]any{
				"host":     newParty.RouterConfig.Host,
				"port":     newParty.RouterConfig.Port,
				"tls_cert": newParty.RouterConfig.TlsCert,
			},
			"AssemblerConfig": map[string]any{
				"host":     newParty.AssemblerConfig.Host,
				"port":     newParty.AssemblerConfig.Port,
				"tls_cert": newParty.AssemblerConfig.TlsCert,
			},
			"BatchersConfig": batchersConfig,
		})

	overwriteNestedJSONValue(t, c.configData, partiesConfigList, partiesConfigPath...)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) RemoveParty(t *testing.T, partyID types.PartyID) []byte {
	partiesConfig := getNestedJSONValue(t, c.configData, partiesConfigPath...)
	partiesConfigList := partiesConfig.([]any)

	if c.maxPartiesNum == 0 {
		c.maxPartiesNum = len(partiesConfigList)
	}

	found := false
	for i, party := range partiesConfigList {
		partyMap := party.(map[string]any)
		if uint32(partyID) == uint32(partyMap["PartyID"].(float64)) {
			found = true
			partiesConfigList = append(partiesConfigList[:i], partiesConfigList[i+1:]...)
			break
		}
	}

	require.True(t, found, "PartyID %d not found in PartiesConfig", partyID)

	overwriteNestedJSONValue(t, c.configData, partiesConfigList, partiesConfigPath...)
	return c.createConfigUpdate(t, c.configData)
}

func (c *ConfigUpdateBuilder) ConfigUpdatePBData(t *testing.T) []byte {
	return c.createConfigUpdate(t, c.configData)
}

// CreateConfigTX creates a config transaction signed by the specified administrators.
// To satisfy majority requirement, the signingParties list must explicitly include the IDs of all participating parties necessary to reach that majority.
func CreateConfigTX(t *testing.T, dir string, signingParties []types.PartyID, submittingParty int, configUpdateBytes []byte) *common.Envelope {
	// Create ConfigUpdateBytes
	require.NotNil(t, configUpdateBytes)

	// create ConfigUpdateEnvelope
	configUpdateEnvelope := &common.ConfigUpdateEnvelope{
		ConfigUpdate: configUpdateBytes,
		Signatures:   []*common.ConfigSignature{},
	}

	// sign with admins
	for _, partyID := range signingParties {
		adminSigner, adminCertBytes, err := createAdminCertAndSigner(dir, int(partyID))
		require.NoError(t, err)
		require.NotNil(t, adminSigner)
		require.NotNil(t, adminCertBytes)

		sId := &msp.SerializedIdentity{
			Mspid:   fmt.Sprintf("org%d", partyID),
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
	submittingAdminSigner, submittingAdminCert, err := createAdminCertAndSigner(dir, submittingParty)
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

func createAdminCertAndSigner(dir string, submittingParty int) (*crypto.ECDSASigner, []byte, error) {
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

func ReadConfigEnvelopeFromConfigBlock(configBlock *common.Block) (*common.ConfigEnvelope, error) {
	envelope, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return nil, err
	}

	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling envelope to payload")
	}

	configEnvelope, _ := configtx.UnmarshalConfigEnvelope(payload.Data)

	return configEnvelope, nil
}

func GetPartyConfig(t *testing.T, configEnvelope *common.ConfigEnvelope, partyID types.PartyID) *protos.PartyConfig {
	require.NotNil(t, configEnvelope)

	require.NotNil(t, configEnvelope.Config.GetChannelGroup().Groups["Orderer"].Values["ConsensusType"].GetValue())

	consensusType := orderer.ConsensusType{}
	err := proto.Unmarshal(configEnvelope.Config.GetChannelGroup().Groups["Orderer"].Values["ConsensusType"].GetValue(), &consensusType)
	require.NoError(t, err)

	sharedConfig := protos.SharedConfig{}
	err = proto.Unmarshal(consensusType.GetMetadata(), &sharedConfig)
	require.NoError(t, err)

	partiesConfig := sharedConfig.GetPartiesConfig()
	require.NotNil(t, partiesConfig)

	for _, partyConfig := range partiesConfig {
		if partyConfig.PartyID == uint32(partyID) {
			return partyConfig
		}
	}

	return nil
}

func getNestedJSONValue(t *testing.T, data map[string]any, path ...string) any {
	index := 0

	curr := any(data)
	for i := 0; i < len(path); {
		p := path[i]
		switch node := curr.(type) {
		case map[string]any:
			subStrings := trimAndSplit(p)
			p = subStrings[0]
			v, ok := node[p]
			require.True(t, ok, "key %s not found at path %v", p, path[:i])
			curr = v
			if len(subStrings) == 1 {
				i++
				continue
			}
			_, err := fmt.Sscanf(subStrings[1], "%d", &index)
			require.True(t, err == nil && index >= 0 && index < len(node), "index parse error: %v", err)
		case []any:
			require.True(t, len(node) > 0, "empty array encountered at path %v", path[:i])
			curr = node[index]
			i++
		case nil:
			require.FailNow(t, fmt.Sprintf("nil value encountered at path %v", path[:i]))
		default:
			require.FailNow(t, fmt.Sprintf("unexpected type encountered at path %v", path[:i]))
		}
	}
	return curr
}

func trimAndSplit(s string) []string {
	s = strings.TrimSpace(s)
	subStrings := strings.Split(s, "[")

	if len(subStrings) > 1 {
		indexPart := strings.TrimRight(subStrings[1], "]")
		indexPart = strings.TrimSpace(indexPart)
		if indexPart == "" {
			return []string{strings.TrimSpace(subStrings[0])}
		}
		return []string{strings.TrimSpace(subStrings[0]), indexPart}
	}
	return []string{s}
}

func overwriteNestedJSONValue(t *testing.T, data map[string]any, value any, path ...string) {
	index := 0

	curr := any(data)
	for i := 0; i < len(path); {
		p := path[i]
		switch node := curr.(type) {
		case map[string]any:
			subStrings := trimAndSplit(p)
			p = subStrings[0]
			if i == len(path)-1 {
				node[p] = value
				return
			}
			v, ok := node[p]
			require.True(t, ok, "key %s not found at path %v", p, path[:i])

			curr = v
			if len(subStrings) == 1 {
				i++
				continue
			}
			_, err := fmt.Sscanf(subStrings[1], "%d", &index)
			require.True(t, err == nil && index >= 0 && index < len(node), "index parse error: %v", err)
		case []any:
			require.True(t, len(node) > 0, "empty array encountered at path %v", path[:i])
			if i == len(path)-1 {
				node[index] = value
				return
			}
			curr = node[index]
		default:
			require.FailNow(t, fmt.Sprintf("unexpected type encountered at path %v", path[:i]))
		}
	}
}
