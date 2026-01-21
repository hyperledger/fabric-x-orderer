/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cryptogen

import (
	"fmt"
	"maps"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"

	"github.com/hyperledger/fabric-x-common/api/types"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/hyperledger/fabric-x-common/core/config"
	"github.com/hyperledger/fabric-x-common/sampleconfig"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
)

// ConfigBlockParameters represents the configuration of the config block.
type ConfigBlockParameters struct {
	TargetPath    string
	BaseProfile   string
	ChannelID     string
	Organizations []OrganizationParameters
	ArmaMetaBytes []byte
}

// OrganizationParameters represents the properties of an organization.
// The Name field will also be used for MspID and organization ID.
type OrganizationParameters struct {
	Name             string
	Domain           string
	OrdererEndpoints []*types.OrdererEndpoint
	ConsenterNodes   []Node
	OrdererNodes     []Node
	PeerNodes        []Node
}

// Node describe an organization node.
type Node struct {
	CommonName string
	Hostname   string
	SANS       []string
	// Fabric-X supports multiple parties per organizations.
	// Thus, in such case, we can create multiple Orderer's nodes
	// for each organization.
	// We organize them such that each party's nodes will be under
	// a dedicated party folder.
	// This folder name is inffered from PartyName, if given.
	// Otherwise, a default name will be used.
	// If it is not set, and we have only one party for the organization,
	// the folder structure will collapse one step down.
	// If it is not set, and we have multiple parties for the organization,
	// The party assigned named will be party-<party-ID>.
	PartyName string
}

// file names.
const (
	ConfigBlockFileName  = "config-block.pb.bin"
	ArmaSharedConfigFile = "arma.pb.bin"
)

// LoadSampleConfig returns the orderer/application config combination that corresponds to
// a given profile.
func LoadSampleConfig(profile string) (*configtxgen.Profile, error) {
	config := viperutil.New()
	err := config.ReadConfig(strings.NewReader(sampleconfig.DefaultYaml))
	if err != nil {
		return nil, errors.Wrap(err, "failed to read config")
	}

	conf := &configtxgen.TopLevel{}
	err = config.EnhancedExactUnmarshal(conf)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshalling config into struct")
	}

	result, ok := conf.Profiles[profile]
	if !ok {
		return nil, errors.Errorf("could not find profile: %s", profile)
	}
	return result, nil
}

// CreateOrExtendConfigBlockWithCrypto creates a config block with default values and a crypto material.
// It uses the first orderer organization as a template and creates the given organizations.
// It uses the same organizations for the orderer and the application.
func CreateOrExtendConfigBlockWithCrypto(conf ConfigBlockParameters) (*common.Block, error) {
	profile, err := CreateOrExtendProfileWithCrypto(&conf)
	if err != nil {
		return nil, err
	}

	profile.Orderer.Arma.Path = ArmaSharedConfigFile
	config.TranslatePathInPlace(conf.TargetPath, &profile.Orderer.Arma.Path)

	err = os.WriteFile(profile.Orderer.Arma.Path, conf.ArmaMetaBytes, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to write ARMA data file")
	}

	block, err := configtxgen.GetOutputBlock(profile, conf.ChannelID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get output block")
	}
	err = configtxgen.WriteOutputBlock(block, path.Join(conf.TargetPath, ConfigBlockFileName))
	return block, errors.Wrap(err, "failed to write block")
}

// CreateOrExtendProfileWithCrypto creates a profile with default values and a crypto material.
// It uses the first orderer organization as a template and creates the given organizations.
// It uses the same organizations for the orderer and the application.
func CreateOrExtendProfileWithCrypto(conf *ConfigBlockParameters) (*configtxgen.Profile, error) {
	initConfigDefault(conf)
	profile, loadErr := LoadSampleConfig(conf.BaseProfile)
	if loadErr != nil {
		return nil, loadErr
	}

	if len(profile.Orderer.Organizations) < 1 {
		return nil, errors.Errorf("no orderer organizations in selected profile: %s", conf.BaseProfile)
	}

	sourceOrg := *profile.Orderer.Organizations[0]

	profile.Consortiums = nil
	profile.Orderer.ConsenterMapping = make([]*configtxgen.Consenter, 0, len(conf.Organizations))
	profile.Orderer.Organizations = make([]*configtxgen.Organization, 0, len(conf.Organizations))
	profile.Application.Organizations = make([]*configtxgen.Organization, 0, len(conf.Organizations))
	cryptoConf := &Config{}

	allOrdererIDs := make(map[uint32]any)
	for _, o := range conf.Organizations {
		org, orgOrdererIDs := createOrg(sourceOrg, &o)
		for _, id := range orgOrdererIDs {
			if _, ok := allOrdererIDs[id]; ok {
				return nil, errors.Errorf("duplicate party id [%d] found in org %s", id, o.Name)
			}
			allOrdererIDs[id] = nil
		}
		allConsenters, err := createConsenter(&o, orgOrdererIDs)
		if err != nil {
			return nil, err
		}
		profile.Orderer.ConsenterMapping = append(profile.Orderer.ConsenterMapping, allConsenters...)

		spec := createOrgSpec(&o)
		switch orgOU(&o) {
		case PeerOU:
			profile.Application.Organizations = append(profile.Application.Organizations, org)
			cryptoConf.PeerOrgs = append(cryptoConf.PeerOrgs, spec)
		case OrdererOU:
			profile.Orderer.Organizations = append(profile.Orderer.Organizations, org)
			cryptoConf.OrdererOrgs = append(cryptoConf.OrdererOrgs, spec)
		default:
			profile.Application.Organizations = append(profile.Application.Organizations, org)
			profile.Orderer.Organizations = append(profile.Orderer.Organizations, org)
			cryptoConf.GenericOrgs = append(cryptoConf.GenericOrgs, spec)
		}
	}

	profile.CompleteInitialization(conf.TargetPath)

	return profile, Extend(conf.TargetPath, cryptoConf)
}

func initConfigDefault(conf *ConfigBlockParameters) {
	if conf.BaseProfile == "" {
		conf.BaseProfile = configtxgen.SampleFabricX
	}
	if conf.ChannelID == "" {
		conf.ChannelID = "chan"
	}
}

func orgOU(o *OrganizationParameters) string {
	ordererNodeCount := len(o.ConsenterNodes) + len(o.OrdererNodes)
	peerNodeCount := len(o.PeerNodes)
	switch {
	case ordererNodeCount > 0 && peerNodeCount == 0:
		return OrdererOU
	case ordererNodeCount == 0 && peerNodeCount > 0:
		return PeerOU
	default:
		return "all"
	}
}

func createOrgSpec(o *OrganizationParameters) OrgSpec {
	ordererNodeCount := len(o.ConsenterNodes) + len(o.OrdererNodes)
	peerNodeCount := len(o.PeerNodes)
	nodeSpecs := make([]NodeSpec, 0, ordererNodeCount+peerNodeCount)
	for _, n := range o.ConsenterNodes {
		nodeSpecs = append(nodeSpecs, NodeSpec{
			CommonName:         n.CommonName,
			Hostname:           n.Hostname,
			SANS:               n.SANS,
			Party:              n.PartyName,
			OrganizationalUnit: OrdererOU,
		})
	}
	for _, n := range o.OrdererNodes {
		nodeSpecs = append(nodeSpecs, NodeSpec{
			CommonName:         n.CommonName,
			Hostname:           n.Hostname,
			SANS:               n.SANS,
			Party:              n.PartyName,
			OrganizationalUnit: OrdererOU,
		})
	}
	for _, n := range o.PeerNodes {
		nodeSpecs = append(nodeSpecs, NodeSpec{
			CommonName:         n.CommonName,
			Hostname:           n.Hostname,
			SANS:               n.SANS,
			Party:              n.PartyName,
			OrganizationalUnit: PeerOU,
		})
	}

	return OrgSpec{
		Name:   o.Name,
		Domain: o.Domain,
		CA: NodeSpec{
			Hostname:   "ca." + o.Domain,
			CommonName: o.Name + "-CA",
		},
		Users: UsersSpec{
			Specs: []UserSpec{
				{Name: "client"},
			},
		},
		Specs: nodeSpecs,
	}
}

func createOrg(
	sourceOrg configtxgen.Organization, o *OrganizationParameters,
) (*configtxgen.Organization, []uint32) {
	org := sourceOrg
	org.ID = o.Name
	org.Name = o.Name
	org.MSPDir = path.Join(getOrgPath(o), MSPDir)
	org.OrdererEndpoints = o.OrdererEndpoints
	allOrdererIDsMap := make(map[uint32]any)
	for _, ep := range org.OrdererEndpoints {
		ep.MspID = o.Name
		allOrdererIDsMap[ep.ID] = nil
	}
	org.Policies = make(map[string]*configtxgen.Policy)
	for k, p := range sourceOrg.Policies {
		org.Policies[k] = &configtxgen.Policy{
			Type: p.Type,
			Rule: strings.ReplaceAll(p.Rule, sourceOrg.Name, o.Name),
		}
	}
	allOrdererIDs := slices.Collect(maps.Keys(allOrdererIDsMap))
	// We sort the IDs for deterministic output.
	slices.Sort(allOrdererIDs)
	return &org, allOrdererIDs
}

func createConsenter(o *OrganizationParameters, ids []uint32) ([]*configtxgen.Consenter, error) {
	if len(ids) != len(o.ConsenterNodes) {
		return nil, errors.Errorf("number of consenters doesn't match number of parties in org: %s", o.Name)
	}
	consenter := make([]*configtxgen.Consenter, len(o.ConsenterNodes))
	for i, n := range o.ConsenterNodes {
		id := ids[i]
		if len(n.PartyName) == 0 && len(ids) > 1 {
			n.PartyName = fmt.Sprintf("party-%d", id)
		}
		host, port, err := parseEndpoint(n.Hostname)
		if err != nil {
			return nil, err
		}
		identity := path.Join(getOrgPath(o), OrdererNodesDir, n.PartyName, n.CommonName,
			MSPDir, SignCertsDir, n.CommonName+CertSuffix)
		tlsIdentity := path.Join(getOrgPath(o), OrdererNodesDir, n.PartyName, n.CommonName,
			TLSDir, ServerPrefix+".crt")
		consenter[i] = &configtxgen.Consenter{
			ID:            id,
			Host:          host,
			Port:          port,
			MSPID:         o.Name,
			Identity:      identity,
			ClientTLSCert: tlsIdentity,
			ServerTLSCert: tlsIdentity,
		}
	}
	return consenter, nil
}

const defaultEndpointPort = 8080

func parseEndpoint(endpoint string) (string, uint32, error) {
	if !strings.Contains(endpoint, ":") {
		return endpoint, defaultEndpointPort, nil
	}
	host, portS, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", 0, errors.Errorf("endpoint %q is invalid: %w", endpoint, err)
	}
	if host == "" {
		return "", 0, errors.Errorf("endpoint %q must include a host", endpoint)
	}
	port, err := strconv.ParseUint(portS, 10, 16) // bit-size 16 rejects ports > 65535
	if err != nil {
		return "", 0, errors.Errorf("endpoint %q has an invalid port %q: %w", endpoint, portS, err)
	}
	if port == 0 {
		return "", 0, errors.Errorf("endpoint %q: port must be greater than 0", endpoint)
	}
	return host, uint32(port), nil
}

func getOrgPath(o *OrganizationParameters) string {
	switch orgOU(o) {
	case PeerOU:
		return path.Join(PeerOrganizationsDir, o.Domain)
	case OrdererOU:
		return path.Join(OrdererOrganizationsDir, o.Domain)
	default:
		return path.Join(GenericOrganizationsDir, o.Domain)
	}
}
