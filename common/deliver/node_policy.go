/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"slices"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// NodeVerifier authorizes a request one node sends to another: it resolves the signer by the signing
// certificate the request carries among the nodes that connect to the service, and verifies the
// signature against it. Certificates are public, so only the signature proves possession.
type NodeVerifier struct {
	// permittedNodes are the nodes that connect to the service, by the signed part of their signing
	// certificate.
	permittedNodes map[string]node
}

type node struct {
	identity  types.NodeIdentity
	publicKey *ecdsa.PublicKey
}

// NewNodeVerifier collects the signing certificate of every node of the shared configuration the
// bundle carries that nodesThatConnect covers, and refuses a request of any other node. A missing or
// unusable certificate of such a node is an error, because no request of it could ever be
// authorized.
func NewNodeVerifier(bundle channelconfig.Resources, nodesThatConnect ...ConnectingNodes) (*NodeVerifier, error) {
	if len(nodesThatConnect) == 0 {
		return nil, errors.New("no node connects to the service, so every request would be refused")
	}

	for _, connecting := range nodesThatConnect {
		if err := connecting.validate(); err != nil {
			return nil, err
		}
	}

	parties, err := partiesOfBundle(bundle)
	if err != nil {
		return nil, err
	}

	v := &NodeVerifier{permittedNodes: make(map[string]node)}
	for _, party := range parties {
		for _, node := range nodesOfParty(party) {
			connects := slices.ContainsFunc(nodesThatConnect, func(connecting ConnectingNodes) bool {
				return connecting.covers(node.identity)
			})
			if !connects {
				continue
			}

			if err := v.addNode(node.signCert, node.identity); err != nil {
				return nil, err
			}
		}
	}

	if len(v.permittedNodes) == 0 {
		return nil, errors.New("no node of the shared configuration connects to the service, so every " +
			"request would be refused")
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
	identity types.NodeIdentity
	signCert []byte
}

// nodesOfParty returns every node a party runs with the signing certificate held for it: one router,
// one batcher per shard, one consenter and one assembler.
func nodesOfParty(party *ordererpb.PartyConfig) []signCertOfNode {
	partyID := types.PartyID(party.GetPartyID())

	nodes := make([]signCertOfNode, 0, len(party.GetBatchersConfig())+3)
	nodes = append(nodes, signCertOfNode{
		identity: types.NewRouterIdentity(partyID),
		signCert: party.GetRouterConfig().GetSignCert(),
	})

	for _, batcher := range party.GetBatchersConfig() {
		nodes = append(nodes, signCertOfNode{
			identity: types.NewBatcherIdentity(partyID, types.ShardID(batcher.GetShardID())),
			signCert: batcher.GetSignCert(),
		})
	}

	nodes = append(nodes, signCertOfNode{
		identity: types.NewConsenterIdentity(partyID),
		signCert: party.GetConsenterConfig().GetSignCert(),
	})

	return append(nodes, signCertOfNode{
		identity: types.NewAssemblerIdentity(partyID),
		signCert: party.GetAssemblerConfig().GetSignCert(),
	})
}

// addNode indexes a node by the DER of the signed part of its signing certificate, so that neither
// the PEM encoding nor a re-encoded certificate signature matters: an MSP rewrites a high-S ECDSA
// signature to low-S, which leaves the certificate equivalent but changes its bytes.
func (v *NodeVerifier) addNode(signCert []byte, identity types.NodeIdentity) error {
	if len(signCert) == 0 {
		return errors.Errorf("the shared configuration holds no signing certificate for the %s", identity)
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

	// A certificate two nodes share admits both, but resolves to the one added last. The verification
	// of a configuration is where such a configuration is meant to be refused.
	if shared, taken := v.permittedNodes[string(cert.RawTBSCertificate)]; taken {
		logger.Warnf("The %s and the %s share a signing certificate, so a request that carries it is "+
			"attributed to the %s", shared.identity, identity, identity)
	}

	v.permittedNodes[string(cert.RawTBSCertificate)] = node{identity: identity, publicKey: publicKey}

	return nil
}

// CheckPolicy refuses a request of any node that does not connect to the service. The channel is not
// part of the decision.
func (v *NodeVerifier) CheckPolicy(envelope *cb.Envelope, channelID string) error {
	_, err := v.VerifyRequest(envelope)
	return err
}

// VerifyRequest returns the node that signed the request, or an error if no node that connects to the
// service signed it.
func (v *NodeVerifier) VerifyRequest(envelope *cb.Envelope) (types.NodeIdentity, error) {
	signedData, err := protoutil.EnvelopeAsSignedData(envelope)
	if err != nil {
		return types.NodeIdentity{}, errors.Wrap(err, "could not convert the request to signed data")
	}
	if len(signedData) != 1 {
		return types.NodeIdentity{}, errors.Errorf("expected a single signature over the request, got %d",
			len(signedData))
	}

	certPEM := signedData[0].Identity.GetCertificate()
	if len(certPEM) == 0 {
		return types.NodeIdentity{}, errors.New("the request carries no signing certificate")
	}

	cert, err := utils.Parsex509Cert(certPEM)
	if err != nil {
		return types.NodeIdentity{}, errors.Wrap(err, "failed parsing the signing certificate the request carries")
	}

	signer, known := v.permittedNodes[string(cert.RawTBSCertificate)]
	if !known {
		return types.NodeIdentity{}, errors.New(
			"the request carries the signing certificate of no node that connects to this service",
		)
	}

	digest := sha256.Sum256(signedData[0].Data)
	if !ecdsa.VerifyASN1(signer.publicKey, digest[:], signedData[0].Signature) {
		return types.NodeIdentity{}, errors.Errorf(
			"the signature over the request does not verify against the signing certificate of the %s",
			signer.identity,
		)
	}

	return signer.identity, nil
}
