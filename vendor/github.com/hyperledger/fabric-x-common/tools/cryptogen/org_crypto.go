/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cryptogen

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// orgCryptoTree represents a cryptogen's organization tree structure.
type orgCryptoTree struct {
	*mspTree
	OrgSpec       *OrgSpec
	CA            string
	Users         string
	TLSCa         string
	OrderingNodes string
	PeerNodes     string
}

// cryptoTree collects all the generated crypto material.
type cryptoTree struct {
	OrdererOrgs []*orgCryptoTree
	PeerOrgs    []*orgCryptoTree
	GenericOrgs []*orgCryptoTree
}

const (
	userBaseName            = "User"
	adminBaseName           = "Admin"
	defaultHostnameTemplate = "{{.Prefix}}{{.Index}}"
	defaultCNTemplate       = "{{.Hostname}}.{{.Domain}}"
)

// Tree names.
const (
	CaDir                   = "ca"
	UsersDir                = "users"
	TLSCaDir                = "tlsca"
	PeerNodesDir            = "peers"
	OrdererNodesDir         = "orderers"
	OrdererOrganizationsDir = "ordererOrganizations"
	PeerOrganizationsDir    = "peerOrganizations"
	GenericOrganizationsDir = "organizations"

	TLSCaPrefix = "tls"

	DefaultCaHostname = "ca"
)

// Generate generates crypto in the given directory using the given config.
func Generate(rootDir string, config *Config) error {
	c, err := prepareAllCryptoSpecs(rootDir, config)
	if err != nil {
		return err
	}
	wg, _ := errgroup.WithContext(context.Background())
	for _, orgTree := range allTrees(c) {
		wg.Go(func() error {
			return orgTree.generateOrg()
		})
	}
	return wg.Wait()
}

// Extend extends a crypto in the given directory using the given config.
func Extend(rootDir string, config *Config) error {
	c, err := prepareAllCryptoSpecs(rootDir, config)
	if err != nil {
		return err
	}
	wg, _ := errgroup.WithContext(context.Background())
	for _, orgTree := range allTrees(c) {
		wg.Go(func() error {
			return orgTree.extendOrg()
		})
	}
	return wg.Wait()
}

func prepareAllCryptoSpecs(rootDir string, config *Config) (*cryptoTree, error) {
	c := &cryptoTree{
		OrdererOrgs: make([]*orgCryptoTree, len(config.OrdererOrgs)),
		PeerOrgs:    make([]*orgCryptoTree, len(config.PeerOrgs)),
		GenericOrgs: make([]*orgCryptoTree, len(config.GenericOrgs)),
	}
	for i := range config.OrdererOrgs {
		s := &config.OrdererOrgs[i]
		err := renderOrgSpecForOrgUnitWithTemplate(s, OrdererOU)
		if err != nil {
			return nil, err
		}
		c.OrdererOrgs[i] = newOrgCryptoTree(path.Join(rootDir, OrdererOrganizationsDir), s)
	}
	for i := range config.PeerOrgs {
		s := &config.PeerOrgs[i]
		err := renderOrgSpecForOrgUnitWithTemplate(s, PeerOU)
		if err != nil {
			return nil, err
		}
		c.PeerOrgs[i] = newOrgCryptoTree(path.Join(rootDir, PeerOrganizationsDir), &config.PeerOrgs[i])
	}
	for i := range config.GenericOrgs {
		s := &config.GenericOrgs[i]
		// We do not render templates for generic organizations.
		err := renderOrgSpec(s)
		if err != nil {
			return nil, err
		}
		c.GenericOrgs[i] = newOrgCryptoTree(path.Join(rootDir, GenericOrganizationsDir), s)
	}
	return c, nil
}

func allTrees(c *cryptoTree) []*orgCryptoTree {
	return slices.Concat(c.OrdererOrgs, c.PeerOrgs, c.GenericOrgs)
}

// newOrgCryptoTree creates a new organization tree.
func newOrgCryptoTree(root string, org *OrgSpec) *orgCryptoTree {
	root = filepath.Join(root, org.Domain)
	return &orgCryptoTree{
		mspTree:       newMspTree(root),
		OrgSpec:       org,
		CA:            filepath.Join(root, CaDir),
		Users:         filepath.Join(root, UsersDir),
		TLSCa:         filepath.Join(root, TLSCaDir),
		OrderingNodes: filepath.Join(root, OrdererNodesDir),
		PeerNodes:     filepath.Join(root, PeerNodesDir),
	}
}

// subUser returns a sub MSP tree of a specific user.
func (c *orgCryptoTree) subUser(name string) *mspTree {
	return newMspTree(filepath.Join(c.Users, name))
}

// subNode returns a sub MSP tree of a specific node.
func (c *orgCryptoTree) subNode(party, name, nodeOU string) *mspTree {
	var nodeDir string
	switch nodeOU {
	case OrdererOU:
		nodeDir = c.OrderingNodes
	case PeerOU:
		nodeDir = c.PeerNodes
	default: // AdminOU, ClientOU
		nodeDir = c.Users
	}
	return newMspTree(filepath.Join(nodeDir, party, name))
}

// subNodeFromSpec returns a sub MSP tree of a specific node.
func (c *orgCryptoTree) subNodeFromSpec(s *NodeSpec) *mspTree {
	return c.subNode(s.Party, s.CommonName, s.OrganizationalUnit)
}

// generateOrg generate the organization's crypto.
func (c *orgCryptoTree) generateOrg() error {
	s := c.OrgSpec
	orgName := s.Domain

	// generate signing CA
	signCA, err := caFromSpec(c.CA, orgName, "", &s.CA)
	if err != nil {
		return err
	}
	// generate TLS CA
	tlsCA, err := caFromSpec(c.TLSCa, orgName, TLSCaPrefix, &s.CA)
	if err != nil {
		return err
	}

	p := nodeParameters{
		SignCa:    signCA,
		TLSCa:     tlsCA,
		EnableOUs: s.EnableNodeOUs,
		KeyAlg:    s.CA.PublicKeyAlgorithm,
	}
	err = c.generateVerifyingMSP(p)
	if err != nil {
		return err
	}

	err = c.generateNodes(s.Specs, p)
	if err != nil {
		return err
	}

	// generate users with the admin user.
	users := append(c.generateUsers(), adminUser(orgName))
	err = c.generateNodes(users, p)
	if err != nil {
		return err
	}

	// copy the admin certs to the org's and nodes' MSP admincerts.
	return c.syncAdminCerts(users)
}

// extendOrg extends the organization's crypto.
func (c *orgCryptoTree) extendOrg() error {
	if !c.isExist() {
		return c.generateOrg()
	}

	s := c.OrgSpec
	signCA, err := loadCA(c.CA, s, s.CA.CommonName)
	if err != nil {
		return err
	}
	tlsCA, err := loadCA(c.TLSCa, s, TLSCaPrefix+s.CA.CommonName)
	if err != nil {
		return err
	}

	p := nodeParameters{
		SignCa:    signCA,
		TLSCa:     tlsCA,
		EnableOUs: s.EnableNodeOUs,
		KeyAlg:    s.CA.PublicKeyAlgorithm,
	}
	err = c.generateNodes(s.Specs, p)
	if err != nil {
		return err
	}

	users := append(c.generateUsers(), adminUser(s.Domain))
	err = c.generateNodes(users, p)
	if err != nil {
		return err
	}

	return c.syncAdminCerts(users)
}

func (c *orgCryptoTree) generateUsers() []NodeSpec {
	s := c.OrgSpec
	orgName := s.Domain
	users := make([]NodeSpec, 0, len(s.Users.Specs)+s.Users.Count)
	publicKeyAlg := getPublicKeyAlg(s.Users.PublicKeyAlgorithm)
	for _, spec := range s.Users.Specs {
		ou := ClientOU
		if spec.Admin {
			ou = AdminOU
		}
		users = append(users, NodeSpec{
			CommonName: fmt.Sprintf("%s@%s", spec.Name, orgName),
			// the user's own algorithm wins over the org-wide one.
			PublicKeyAlgorithm: cmp.Or(spec.PublicKeyAlgorithm, publicKeyAlg),
			OrganizationalUnit: ou,
		})
	}
	for j := range s.Users.Count {
		users = append(users, NodeSpec{
			CommonName:         fmt.Sprintf("%s%d@%s", userBaseName, j+1, orgName),
			PublicKeyAlgorithm: publicKeyAlg,
			OrganizationalUnit: ClientOU,
		})
	}
	return users
}

// syncAdminCerts copies every admin user's cert into the org's and each node's MSP admincerts.
// It is a no-op when node OUs are enabled, since admin authority is then conveyed by the OU
// classification instead of admincerts membership.
func (c *orgCryptoTree) syncAdminCerts(users []NodeSpec) error {
	if c.OrgSpec.EnableNodeOUs {
		return nil
	}

	// find all admin users so that we can sync their signcerts under the org admincerts/ folder
	names := adminNames(users)
	err := c.overwriteAdminCerts(c.AdminCerts, names...)
	if err != nil {
		return err
	}

	return c.overwriteNodesAdminCert(names...)
}

// adminNames returns the common names of every admin user among the given specs.
func adminNames(users []NodeSpec) []string {
	names := make([]string, 0, len(users))
	for _, u := range users {
		if u.OrganizationalUnit == AdminOU {
			names = append(names, u.CommonName)
		}
	}

	return names
}

// overwriteNodesAdminCert overwrites the admin certs on each node with the org's MSP admincerts.
func (c *orgCryptoTree) overwriteNodesAdminCert(adminUserNames ...string) error {
	for _, spec := range c.OrgSpec.Specs {
		err := c.overwriteAdminCerts(c.subNodeFromSpec(&spec).AdminCerts, adminUserNames...)
		if err != nil {
			return err
		}
	}
	return nil
}

// overwriteAdminCerts rebuilds the given admincerts directory from scratch, so that admins
// dropped from the config lose their authority instead of lingering in an existing directory.
func (c *orgCryptoTree) overwriteAdminCerts(adminCertsDir string, adminUserNames ...string) error {
	// delete the contents of admincerts
	err := os.RemoveAll(adminCertsDir)
	if err != nil {
		return errors.Wrapf(err, "error removing admin cert directory %s", adminCertsDir)
	}
	// recreate the admincerts directory
	err = os.MkdirAll(adminCertsDir, 0o750)
	if err != nil {
		return errors.Wrapf(err, "error creating admin cert directory %s", adminCertsDir)
	}
	for _, adminUserName := range adminUserNames {
		src := filepath.Join(c.subUser(adminUserName).SignCerts, adminUserName+"-cert.pem")
		dst := filepath.Join(adminCertsDir, adminUserName+"-cert.pem")
		err = copyFile(src, dst)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *orgCryptoTree) generateNodes(nodes []NodeSpec, p nodeParameters) error {
	for i := range nodes {
		node := &nodes[i]
		tree := c.subNodeFromSpec(node)
		if tree.isExist() {
			continue
		}
		curParams := p
		curParams.OU = node.OrganizationalUnit
		if node.OrganizationalUnit == AdminOU && !p.EnableOUs {
			curParams.OU = ClientOU
		}
		curParams.Name = node.CommonName
		curParams.TLSSans = node.SANS
		curParams.KeyAlg = node.PublicKeyAlgorithm
		err := tree.generateLocalMSP(curParams)
		if err != nil {
			return err
		}

		// Add certificate to the organization's known certs.
		srcCertPath := path.Join(tree.SignCerts, node.CommonName+"-cert.pem")
		targetCertPath := path.Join(c.KnownCerts, node.CommonName+"-cert.pem")
		err = copyFile(srcCertPath, targetCertPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	content, err := os.ReadFile(src)
	if err != nil {
		return errors.Wrapf(err, "error reading source file %s", src)
	}
	err = os.WriteFile(dst, content, 0o650)
	if err != nil {
		return errors.Wrapf(err, "error writing destination file %s", dst)
	}
	return nil
}

func adminUser(orgName string) NodeSpec {
	return NodeSpec{
		CommonName:         adminUserName(orgName),
		PublicKeyAlgorithm: ECDSA,
		OrganizationalUnit: AdminOU,
	}
}

func adminUserName(orgName string) string {
	return fmt.Sprintf("%s@%s", adminBaseName, orgName)
}

func getPublicKeyAlg(pubAlgFromConfig string) (publicKeyAlg string) {
	if pubAlgFromConfig == "" {
		return ECDSA
	}
	return pubAlgFromConfig
}
