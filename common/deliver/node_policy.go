/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"fmt"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// NodeRole is the service a node runs.
type NodeRole string

const (
	RoleRouter    NodeRole = "router"
	RoleBatcher   NodeRole = "batcher"
	RoleConsenter NodeRole = "consenter"
	RoleAssembler NodeRole = "assembler"
)

// NodeIdentity names the node a signing certificate belongs to.
type NodeIdentity struct {
	PartyID types.PartyID
	Role    NodeRole
	// ShardID is meaningful only when Role is RoleBatcher.
	ShardID types.ShardID
}

func (n NodeIdentity) String() string {
	if n.Role == RoleBatcher {
		return fmt.Sprintf("batcher of party %d in shard %d", n.PartyID, n.ShardID)
	}
	return fmt.Sprintf("%s of party %d", n.Role, n.PartyID)
}

// NodeVerifier authorizes a request one node sends to another: it resolves the signer by the signing
// certificate the request carries, verifies the signature against it, and refuses a role that does
// not connect to the service. Certificates are public, so only the signature proves possession.
type NodeVerifier struct {
	nodes        map[string]node
	allowedRoles map[NodeRole]struct{}
	// shardOfBatchers restricts batchers to one shard. Nil accepts the batchers of every shard.
	shardOfBatchers *types.ShardID
}

type node struct {
	identity  NodeIdentity
	publicKey *ecdsa.PublicKey
}

// NewBatcherDeliverVerifier builds the access control of the batcher deliver service of a shard.
// Only the batchers of that shard and the assemblers pull its batches.
func NewBatcherDeliverVerifier(bundle channelconfig.Resources, shardID types.ShardID) (*NodeVerifier, error) {
	return newNodeVerifier(bundle, &shardID, RoleBatcher, RoleAssembler)
}

// NewConsenterDeliverVerifier builds the access control of a consenter deliver service. Every role
// follows the stream of decisions, so all four are served.
func NewConsenterDeliverVerifier(bundle channelconfig.Resources) (*NodeVerifier, error) {
	return newNodeVerifier(bundle, nil, RoleRouter, RoleBatcher, RoleConsenter, RoleAssembler)
}

// NewNodeVerifier collects the signing certificate of every node of the shared configuration the
// bundle carries, and accepts only a node whose role is in rolesThatConnect. A node with no
// certificate is left out; an unusable certificate is an error.
func NewNodeVerifier(bundle channelconfig.Resources, rolesThatConnect ...NodeRole) (*NodeVerifier, error) {
	return newNodeVerifier(bundle, nil, rolesThatConnect...)
}

// newNodeVerifier accepts a batcher only from shardOfBatchers, when it is set. The restriction is
// kept apart from the index so that a batcher aimed at the wrong shard is told so.
func newNodeVerifier(bundle channelconfig.Resources, shardOfBatchers *types.ShardID, rolesThatConnect ...NodeRole) (*NodeVerifier, error) {
	if len(rolesThatConnect) == 0 {
		return nil, errors.New("no role connects to the service, so every request would be refused")
	}

	v := &NodeVerifier{
		nodes:           make(map[string]node),
		allowedRoles:    make(map[NodeRole]struct{}, len(rolesThatConnect)),
		shardOfBatchers: shardOfBatchers,
	}

	for _, role := range rolesThatConnect {
		switch role {
		case RoleRouter, RoleBatcher, RoleConsenter, RoleAssembler:
			v.allowedRoles[role] = struct{}{}
		default:
			return nil, errors.Errorf("%q is not a node role", role)
		}
	}

	parties, err := partiesOfBundle(bundle)
	if err != nil {
		return nil, err
	}

	for _, party := range parties {
		for _, node := range nodesOfParty(party) {
			if err := v.addNode(node.signCert, node.identity); err != nil {
				return nil, err
			}
		}
	}

	if len(v.nodes) == 0 {
		return nil, errors.New("the shared configuration holds no node signing certificates")
	}

	return v, nil
}

// partiesOfBundle returns the parties of the shared configuration, which a bundle carries as the
// consensus metadata of its orderer configuration, so a configuration update refreshes them.
func partiesOfBundle(bundle channelconfig.Resources) ([]*ordererpb.PartyConfig, error) {
	if bundle == nil {
		return nil, errors.New("no configuration bundle to take the shared configuration from")
	}

	ordererConfig, exists := bundle.OrdererConfig()
	if !exists {
		return nil, errors.New("the configuration bundle holds no orderer configuration")
	}

	sharedConfig := &ordererpb.SharedConfig{}
	if err := proto.Unmarshal(ordererConfig.ConsensusMetadata(), sharedConfig); err != nil {
		return nil, errors.Wrap(err, "failed unmarshaling the shared configuration from the consensus metadata")
	}

	return sharedConfig.GetPartiesConfig(), nil
}

// signCertOfNode is the signing certificate the shared configuration holds for one node of a party.
type signCertOfNode struct {
	identity NodeIdentity
	signCert []byte
}

// nodesOfParty returns every node a party runs with the signing certificate held for it: one router,
// one batcher per shard, one consenter and one assembler.
func nodesOfParty(party *ordererpb.PartyConfig) []signCertOfNode {
	partyID := types.PartyID(party.GetPartyID())

	nodes := make([]signCertOfNode, 0, len(party.GetBatchersConfig())+3)
	nodes = append(nodes, signCertOfNode{
		identity: NodeIdentity{PartyID: partyID, Role: RoleRouter},
		signCert: party.GetRouterConfig().GetSignCert(),
	})

	for _, batcher := range party.GetBatchersConfig() {
		nodes = append(nodes, signCertOfNode{
			identity: NodeIdentity{
				PartyID: partyID,
				Role:    RoleBatcher,
				ShardID: types.ShardID(batcher.GetShardID()),
			},
			signCert: batcher.GetSignCert(),
		})
	}

	nodes = append(nodes, signCertOfNode{
		identity: NodeIdentity{PartyID: partyID, Role: RoleConsenter},
		signCert: party.GetConsenterConfig().GetSignCert(),
	})

	return append(nodes, signCertOfNode{
		identity: NodeIdentity{PartyID: partyID, Role: RoleAssembler},
		signCert: party.GetAssemblerConfig().GetSignCert(),
	})
}

// addNode indexes a node by the DER of the signed part of its signing certificate, so that neither
// the PEM encoding nor a re-encoded certificate signature matters: an MSP rewrites a high-S ECDSA
// signature to low-S, which leaves the certificate equivalent but changes its bytes.
func (v *NodeVerifier) addNode(signCert []byte, identity NodeIdentity) error {
	if len(signCert) == 0 {
		return nil
	}

	cert, err := utils.Parsex509Cert(signCert)
	if err != nil {
		return errors.Wrapf(err, "failed parsing the signing certificate of the %s", identity)
	}

	publicKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return errors.Errorf("the signing certificate of the %s holds a %T public key, expected ECDSA",
			identity, cert.PublicKey)
	}

	// A request is verified over a SHA-256 digest, so another curve indexes a node whose every
	// request would fail to verify.
	if publicKey.Curve != elliptic.P256() {
		return errors.Errorf("the signing certificate of the %s holds a %s public key, expected P-256",
			identity, publicKey.Curve.Params().Name)
	}

	// A certificate resolves to a single identity, so nodes that share one cannot be told apart.
	// That matters only when one of them connects to this service; a request of a role that does
	// not is refused whichever of the two it resolves to.
	if existing, taken := v.nodes[string(cert.RawTBSCertificate)]; taken {
		if !v.serves(existing.identity.Role) && !v.serves(identity.Role) {
			return nil
		}

		return errors.Errorf("the %s and the %s share a signing certificate", existing.identity, identity)
	}

	v.nodes[string(cert.RawTBSCertificate)] = node{identity: identity, publicKey: publicKey}

	return nil
}

// serves says whether a role connects to the service this verifier guards.
func (v *NodeVerifier) serves(role NodeRole) bool {
	_, allowed := v.allowedRoles[role]

	return allowed
}

// CheckPolicy refuses a request that no node of the shared configuration signed, that a role which
// does not connect to this service signed, or that a batcher of another shard signed. The channel is
// not part of the decision.
func (v *NodeVerifier) CheckPolicy(envelope *cb.Envelope, channelID string) error {
	requester, err := v.VerifyRequest(envelope)
	if err != nil {
		return err
	}

	if !v.serves(requester.Role) {
		return errors.Errorf("the %s does not connect to this service", requester)
	}

	if requester.Role == RoleBatcher && v.shardOfBatchers != nil && requester.ShardID != *v.shardOfBatchers {
		return errors.Errorf("the %s does not connect to the service of shard %d",
			requester, *v.shardOfBatchers)
	}

	return nil
}

// VerifyRequest returns the node that signed the request, or an error if no node of the shared
// configuration signed it.
func (v *NodeVerifier) VerifyRequest(envelope *cb.Envelope) (NodeIdentity, error) {
	signedData, err := protoutil.EnvelopeAsSignedData(envelope)
	if err != nil {
		return NodeIdentity{}, errors.Wrap(err, "could not convert the request to signed data")
	}
	if len(signedData) != 1 {
		return NodeIdentity{}, errors.Errorf("expected a single signature over the request, got %d",
			len(signedData))
	}

	certPEM := signedData[0].Identity.GetCertificate()
	if len(certPEM) == 0 {
		return NodeIdentity{}, errors.New("the request carries no signing certificate")
	}

	cert, err := utils.Parsex509Cert(certPEM)
	if err != nil {
		return NodeIdentity{}, errors.Wrap(err, "failed parsing the signing certificate the request carries")
	}

	signer, known := v.nodes[string(cert.RawTBSCertificate)]
	if !known {
		return NodeIdentity{}, errors.New(
			"the request carries a signing certificate that is not in the shared configuration",
		)
	}

	digest := sha256.Sum256(signedData[0].Data)
	if !ecdsa.VerifyASN1(signer.publicKey, digest[:], signedData[0].Signature) {
		return NodeIdentity{}, errors.Errorf(
			"the signature over the request does not verify against the signing certificate of the %s",
			signer.identity,
		)
	}

	return signer.identity, nil
}
