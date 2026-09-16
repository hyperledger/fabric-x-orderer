/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"maps"
	"math/big"
	"slices"
	"testing"
	"time"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/msputils"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const testMSPID = "SampleOrg"

// A node verifier is the access control of a deliver service.
var _ deliver.PolicyChecker = (*deliver.NodeVerifier)(nil)

// allRoles are the roles of a service every node connects to.
var allRoles = []deliver.NodeRole{
	deliver.RoleRouter,
	deliver.RoleBatcher,
	deliver.RoleConsenter,
	deliver.RoleAssembler,
}

// Scenario:
//  1. Build a local MSP from the sample MSP material, and take its default signing identity.
//  2. Place the signing certificate of that identity in the shared configuration, as the batcher
//     of party 1 in shard 2.
//  3. Sign a deliver request with that signing identity.
//  4. Verify the request, and expect the batcher of party 1 in shard 2.
func TestVerifyRequestSignedByMSPIdentity(t *testing.T) {
	localMSP := msputils.BuildLocalMSP(fabric.GetDevMspDir(), testMSPID, nil)

	signer, err := localMSP.GetDefaultSigningIdentity()
	require.NoError(t, err)

	certPEM, err := signer.GetCertificatePEM()
	require.NoError(t, err)

	batcher := deliver.NodeIdentity{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 2}

	verifier, err := deliver.NewNodeVerifier(
		bundleOf(t, sharedConfig(map[deliver.NodeIdentity][]byte{batcher: certPEM})), deliver.RoleBatcher,
	)
	require.NoError(t, err)

	requester, err := verifier.VerifyRequest(signRequest(t, signer))
	require.NoError(t, err)
	require.Equal(t, batcher, requester)
}

// Scenario:
// 1. Generate a signing certificate for every node of two parties, one of which runs two shards.
// 2. Place all of the signing certificates in the shared configuration.
// 3. Sign a deliver request with the signing key of each node.
// 4. Verify every request, and expect the node whose signing certificate it carries.
func TestVerifyRequestOfEveryNode(t *testing.T) {
	nodes := []deliver.NodeIdentity{
		{PartyID: 1, Role: deliver.RoleRouter},
		{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 1},
		{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 2},
		{PartyID: 1, Role: deliver.RoleConsenter},
		{PartyID: 1, Role: deliver.RoleAssembler},
		{PartyID: 2, Role: deliver.RoleRouter},
		{PartyID: 2, Role: deliver.RoleBatcher, ShardID: 1},
		{PartyID: 2, Role: deliver.RoleBatcher, ShardID: 2},
		{PartyID: 2, Role: deliver.RoleConsenter},
		{PartyID: 2, Role: deliver.RoleAssembler},
	}

	certs := make(map[deliver.NodeIdentity][]byte, len(nodes))
	signers := make(map[deliver.NodeIdentity]*testSigner, len(nodes))
	for _, node := range nodes {
		certPEM, key := generateSignCert(t)
		certs[node] = certPEM
		signers[node] = &testSigner{certPEM: certPEM, key: key}
	}

	verifier, err := deliver.NewNodeVerifier(bundleOf(t, sharedConfig(certs)), allRoles...)
	require.NoError(t, err)

	for _, node := range nodes {
		t.Run(node.String(), func(t *testing.T) {
			requester, err := verifier.VerifyRequest(signRequest(t, signers[node]))
			require.NoError(t, err)
			require.Equal(t, node, requester)
		})
	}
}

// Scenario:
//  1. Generate a signing certificate, and place it in the shared configuration as the assembler of
//     party 1.
//  2. Sign the same certificate body again, so that the certificate is equivalent but its bytes
//     differ.
//  3. Sign a deliver request with the re-signed certificate.
//  4. Verify the request, and expect the assembler of party 1.
func TestVerifyRequestOfReSignedCertificate(t *testing.T) {
	certPEM, key := generateSignCert(t)
	node := deliver.NodeIdentity{PartyID: 1, Role: deliver.RoleAssembler}

	certs := map[deliver.NodeIdentity][]byte{node: certPEM}
	verifier, err := deliver.NewNodeVerifier(bundleOf(t, sharedConfig(certs)), allRoles...)
	require.NoError(t, err)

	reSignedPEM := certOfSameBody(t, certPEM, key)
	requester, err := verifier.VerifyRequest(signRequest(t, &testSigner{certPEM: reSignedPEM, key: key}))
	require.NoError(t, err)
	require.Equal(t, node, requester)
}

// Scenario:
//  1. Generate a signing certificate, and place it in the shared configuration as the consenter
//     of party 1.
//  2. Generate a second signing certificate, and leave it out of the shared configuration.
//  3. Build a deliver request for each case in which a request must be refused.
//  4. Verify every request, and expect a refusal that names the reason.
func TestVerifyRequestRefusal(t *testing.T) {
	consenter := deliver.NodeIdentity{PartyID: 1, Role: deliver.RoleConsenter}
	consenterCertPEM, consenterKey := generateSignCert(t)
	consenterSigner := &testSigner{certPEM: consenterCertPEM, key: consenterKey}

	otherCertPEM, otherKey := generateSignCert(t)

	verifier, err := deliver.NewNodeVerifier(bundleOf(t, sharedConfig(map[deliver.NodeIdentity][]byte{
		consenter: consenterCertPEM,
	})), deliver.RoleConsenter)
	require.NoError(t, err)

	tests := []struct {
		name    string
		request func(t *testing.T) *cb.Envelope
		error   string
	}{
		{
			name: "a signing certificate that is not in the shared configuration",
			request: func(t *testing.T) *cb.Envelope {
				return signRequest(t, &testSigner{certPEM: otherCertPEM, key: otherKey})
			},
			error: "not in the shared configuration",
		},
		{
			name: "the signing certificate of the consenter, signed by another key",
			request: func(t *testing.T) *cb.Envelope {
				return signRequest(t, &testSigner{certPEM: consenterCertPEM, key: otherKey})
			},
			error: "does not verify against the signing certificate of the consenter of party 1",
		},
		{
			name: "the payload of another request",
			request: func(t *testing.T) *cb.Envelope {
				request := signRequest(t, consenterSigner)
				request.Payload = signRequest(t, consenterSigner).Payload
				return request
			},
			error: "does not verify against the signing certificate of the consenter of party 1",
		},
		{
			name: "an altered signature",
			request: func(t *testing.T) *cb.Envelope {
				request := signRequest(t, consenterSigner)
				request.Signature[len(request.Signature)-1] ^= 0xff
				return request
			},
			error: "does not verify against the signing certificate of the consenter of party 1",
		},
		{
			name: "no signature",
			request: func(t *testing.T) *cb.Envelope {
				request := signRequest(t, consenterSigner)
				request.Signature = nil
				return request
			},
			error: "does not verify against the signing certificate of the consenter of party 1",
		},
		{
			name: "an identity that references a signing certificate instead of carrying one",
			request: func(t *testing.T) *cb.Envelope {
				return signRequest(t, &idOfCertSigner{key: consenterKey})
			},
			error: "carries no signing certificate",
		},
		{
			name: "a signing certificate that cannot be parsed",
			request: func(t *testing.T) *cb.Envelope {
				return signRequest(t, &testSigner{certPEM: []byte("not a certificate"), key: consenterKey})
			},
			error: "failed parsing the signing certificate the request carries",
		},
		{
			name: "a payload that cannot be parsed",
			request: func(t *testing.T) *cb.Envelope {
				return &cb.Envelope{Payload: []byte("not a payload"), Signature: []byte("signature")}
			},
			error: "could not convert the request to signed data",
		},
		{
			name: "no request",
			request: func(t *testing.T) *cb.Envelope {
				return nil
			},
			error: "could not convert the request to signed data",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requester, err := verifier.VerifyRequest(test.request(t))
			require.ErrorContains(t, err, test.error)
			require.Equal(t, deliver.NodeIdentity{}, requester)
		})
	}
}

// Scenario:
//  1. Place the signing certificate of the assembler of party 1 in the shared configuration, and
//     leave every other node of that party without one.
//  2. Sign one deliver request with the assembler key, and another with the key of a node whose
//     signing certificate the shared configuration does not hold.
//  3. Verify both requests, and expect the assembler request to be authorized and the other to be
//     refused.
func TestVerifyRequestOfNodeWithoutSigningCertificate(t *testing.T) {
	assembler := deliver.NodeIdentity{PartyID: 1, Role: deliver.RoleAssembler}
	assemblerCertPEM, assemblerKey := generateSignCert(t)
	routerCertPEM, routerKey := generateSignCert(t)

	verifier, err := deliver.NewNodeVerifier(bundleOf(t, []*ordererpb.PartyConfig{{
		PartyID:         1,
		RouterConfig:    &ordererpb.RouterNodeConfig{},
		BatchersConfig:  []*ordererpb.BatcherNodeConfig{{ShardID: 1}},
		ConsenterConfig: &ordererpb.ConsenterNodeConfig{},
		AssemblerConfig: &ordererpb.AssemblerNodeConfig{SignCert: assemblerCertPEM},
	}}), deliver.RoleAssembler)
	require.NoError(t, err)

	requester, err := verifier.VerifyRequest(signRequest(t, &testSigner{certPEM: assemblerCertPEM, key: assemblerKey}))
	require.NoError(t, err)
	require.Equal(t, assembler, requester)

	_, err = verifier.VerifyRequest(signRequest(t, &testSigner{certPEM: routerCertPEM, key: routerKey}))
	require.ErrorContains(t, err, "not in the shared configuration")
}

// Scenario:
// 1. Build a shared configuration for each case in which a verifier cannot be built over it.
// 2. Build a verifier from the configuration bundle that carries each shared configuration.
// 3. Expect an error that names the reason, and no verifier.
func TestNewNodeVerifier(t *testing.T) {
	certPEM, _ := generateSignCert(t)

	tests := []struct {
		name    string
		parties []*ordererpb.PartyConfig
		roles   []deliver.NodeRole
		error   string
	}{
		{
			name: "no role that connects to the service",
			parties: []*ordererpb.PartyConfig{{
				PartyID:      1,
				RouterConfig: &ordererpb.RouterNodeConfig{SignCert: certPEM},
			}},
			roles: nil,
			error: "no role connects to the service",
		},
		{
			name: "a role that no node runs",
			parties: []*ordererpb.PartyConfig{{
				PartyID:      1,
				RouterConfig: &ordererpb.RouterNodeConfig{SignCert: certPEM},
			}},
			roles: []deliver.NodeRole{deliver.NodeRole("orderer")},
			error: `"orderer" is not a node role`,
		},
		{
			name:    "no parties",
			parties: nil,
			roles:   allRoles,
			error:   "holds no node signing certificates",
		},
		{
			name: "a party whose nodes have no signing certificate",
			parties: []*ordererpb.PartyConfig{{
				PartyID:         1,
				RouterConfig:    &ordererpb.RouterNodeConfig{},
				ConsenterConfig: &ordererpb.ConsenterNodeConfig{},
			}},
			roles: allRoles,
			error: "holds no node signing certificates",
		},
		{
			name: "two nodes that share a signing certificate",
			parties: []*ordererpb.PartyConfig{{
				PartyID:         1,
				ConsenterConfig: &ordererpb.ConsenterNodeConfig{SignCert: certPEM},
				AssemblerConfig: &ordererpb.AssemblerNodeConfig{SignCert: certPEM},
			}},
			roles: allRoles,
			error: "share a signing certificate",
		},
		{
			name: "a signing certificate that cannot be parsed",
			parties: []*ordererpb.PartyConfig{{
				PartyID:      1,
				RouterConfig: &ordererpb.RouterNodeConfig{SignCert: []byte("not a certificate")},
			}},
			roles: allRoles,
			error: "failed parsing the signing certificate of the router of party 1",
		},
		{
			name: "a signing certificate that does not hold an ECDSA public key",
			parties: []*ordererpb.PartyConfig{{
				PartyID: 1,
				BatchersConfig: []*ordererpb.BatcherNodeConfig{{
					ShardID:  3,
					SignCert: generateRSASignCert(t),
				}},
			}},
			roles: allRoles,
			error: "the signing certificate of the batcher of party 1 in shard 3 holds a *rsa.PublicKey public key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verifier, err := deliver.NewNodeVerifier(bundleOf(t, test.parties), test.roles...)
			require.ErrorContains(t, err, test.error)
			require.Nil(t, verifier)
		})
	}
}

// Scenario:
//  1. Generate a signing certificate for every node of a party, and place all of them in the
//     shared configuration.
//  2. Build a verifier for a service that only batchers and assemblers connect to.
//  3. Sign a deliver request with the signing key of each node of the party.
//  4. Check the policy on every request, and expect the batcher and the assembler to be
//     authorized and the router and the consenter to be refused.
func TestCheckPolicy(t *testing.T) {
	nodes := []deliver.NodeIdentity{
		{PartyID: 1, Role: deliver.RoleRouter},
		{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 1},
		{PartyID: 1, Role: deliver.RoleConsenter},
		{PartyID: 1, Role: deliver.RoleAssembler},
	}

	certs := make(map[deliver.NodeIdentity][]byte, len(nodes))
	signers := make(map[deliver.NodeIdentity]*testSigner, len(nodes))
	for _, node := range nodes {
		certPEM, key := generateSignCert(t)
		certs[node] = certPEM
		signers[node] = &testSigner{certPEM: certPEM, key: key}
	}

	verifier, err := deliver.NewNodeVerifier(bundleOf(t, sharedConfig(certs)), deliver.RoleBatcher, deliver.RoleAssembler)
	require.NoError(t, err)

	for _, node := range nodes {
		t.Run(node.String(), func(t *testing.T) {
			err := verifier.CheckPolicy(signRequest(t, signers[node]), "arma")

			if node.Role == deliver.RoleBatcher || node.Role == deliver.RoleAssembler {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, "does not connect to this service")
		})
	}

	t.Run("a node that is not in the shared configuration", func(t *testing.T) {
		certPEM, key := generateSignCert(t)

		err := verifier.CheckPolicy(signRequest(t, &testSigner{certPEM: certPEM, key: key}), "arma")
		require.ErrorContains(t, err, "not in the shared configuration")
	})
}

// Scenario:
//  1. Generate a signing certificate for every node of a party, and place all of them in the
//     shared configuration.
//  2. Build the access control of a batcher deliver service and of a consenter deliver service.
//  3. Sign a deliver request with the signing key of each node of the party.
//  4. Check both services against every request, and expect a batcher to serve only batchers and
//     assemblers, and a consenter to serve every role.
func TestDeliverServiceRoles(t *testing.T) {
	nodes := []deliver.NodeIdentity{
		{PartyID: 1, Role: deliver.RoleRouter},
		{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 1},
		{PartyID: 1, Role: deliver.RoleConsenter},
		{PartyID: 1, Role: deliver.RoleAssembler},
	}

	certs, signers := signersOfNodes(t, nodes)

	batcherService, err := deliver.NewBatcherDeliverVerifier(bundleOf(t, sharedConfig(certs)), 1)
	require.NoError(t, err)

	consenterService, err := deliver.NewConsenterDeliverVerifier(bundleOf(t, sharedConfig(certs)))
	require.NoError(t, err)

	for _, node := range nodes {
		t.Run(node.String(), func(t *testing.T) {
			servedByBatcher := node.Role == deliver.RoleBatcher || node.Role == deliver.RoleAssembler

			err := batcherService.CheckPolicy(signRequest(t, signers[node]), "arma")
			if servedByBatcher {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "does not connect to this service")
			}

			require.NoError(t, consenterService.CheckPolicy(signRequest(t, signers[node]), "arma"))
		})
	}
}

// Scenario:
//  1. Generate a signing certificate for the batcher of each of two shards and for the assembler,
//     in each of two parties, and place all of them in the shared configuration.
//  2. Build the access control of the batcher deliver service of shard 1, and of a consenter
//     deliver service, which belongs to no shard.
//  3. Sign a deliver request with the signing key of each of those nodes.
//  4. Check the batcher service against every request, and expect it to serve the batchers of
//     shard 1 and the assemblers, and to refuse the batchers of shard 2.
//  5. Check the consenter service against every request, and expect it to serve the batchers of
//     both shards.
func TestBatcherDeliverServiceShard(t *testing.T) {
	shardOfService := types.ShardID(1)

	nodes := []deliver.NodeIdentity{
		{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 1},
		{PartyID: 1, Role: deliver.RoleBatcher, ShardID: 2},
		{PartyID: 1, Role: deliver.RoleAssembler},
		{PartyID: 2, Role: deliver.RoleBatcher, ShardID: 1},
		{PartyID: 2, Role: deliver.RoleBatcher, ShardID: 2},
		{PartyID: 2, Role: deliver.RoleAssembler},
	}

	certs, signers := signersOfNodes(t, nodes)

	batcherService, err := deliver.NewBatcherDeliverVerifier(bundleOf(t, sharedConfig(certs)), shardOfService)
	require.NoError(t, err)

	consenterService, err := deliver.NewConsenterDeliverVerifier(bundleOf(t, sharedConfig(certs)))
	require.NoError(t, err)

	for _, node := range nodes {
		t.Run(node.String(), func(t *testing.T) {
			ofAnotherShard := node.Role == deliver.RoleBatcher && node.ShardID != shardOfService

			err := batcherService.CheckPolicy(signRequest(t, signers[node]), "arma")
			if ofAnotherShard {
				require.ErrorContains(t, err, node.String()+" does not connect to the service of shard 1")
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, consenterService.CheckPolicy(signRequest(t, signers[node]), "arma"))
		})
	}
}

// Scenario:
//  1. Build a verifier from no configuration bundle, from a bundle that holds no orderer
//     configuration, and from a bundle whose consensus metadata is not a shared configuration.
//  2. Expect an error that names the reason, and no verifier.
func TestNewNodeVerifierOfUnusableBundle(t *testing.T) {
	verifier, err := deliver.NewNodeVerifier(nil, allRoles...)
	require.ErrorContains(t, err, "no configuration bundle")
	require.Nil(t, verifier)

	verifier, err = deliver.NewNodeVerifier(&bundleOfOrdererConfig{}, allRoles...)
	require.ErrorContains(t, err, "holds no orderer configuration")
	require.Nil(t, verifier)

	unparseable := &bundleOfOrdererConfig{
		ordererConfig: &ordererConfigOfConsensusMetadata{metadata: []byte("not a shared configuration")},
	}
	verifier, err = deliver.NewNodeVerifier(unparseable, allRoles...)
	require.ErrorContains(t, err, "failed unmarshaling the shared configuration")
	require.Nil(t, verifier)
}

// bundleOf builds the configuration bundle a node runs on, carrying the given parties as the shared
// configuration in the consensus metadata of its orderer configuration.
func bundleOf(t *testing.T, parties []*ordererpb.PartyConfig) *bundleOfOrdererConfig {
	t.Helper()

	metadata, err := proto.Marshal(&ordererpb.SharedConfig{PartiesConfig: parties})
	require.NoError(t, err)

	return &bundleOfOrdererConfig{
		ordererConfig: &ordererConfigOfConsensusMetadata{metadata: metadata},
	}
}

// bundleOfOrdererConfig answers only with the orderer configuration it holds.
type bundleOfOrdererConfig struct {
	channelconfig.Resources
	ordererConfig channelconfig.Orderer
}

func (b *bundleOfOrdererConfig) OrdererConfig() (channelconfig.Orderer, bool) {
	if b.ordererConfig == nil {
		return nil, false
	}
	return b.ordererConfig, true
}

// ordererConfigOfConsensusMetadata answers only with the consensus metadata it holds.
type ordererConfigOfConsensusMetadata struct {
	channelconfig.Orderer
	metadata []byte
}

func (o *ordererConfigOfConsensusMetadata) ConsensusMetadata() []byte {
	return o.metadata
}

// sharedConfig assembles the shared configuration of the parties that the given nodes belong to.
func sharedConfig(certs map[deliver.NodeIdentity][]byte) []*ordererpb.PartyConfig {
	parties := make(map[types.PartyID]*ordererpb.PartyConfig)

	for node, certPEM := range certs {
		party, configured := parties[node.PartyID]
		if !configured {
			party = &ordererpb.PartyConfig{PartyID: uint32(node.PartyID)}
			parties[node.PartyID] = party
		}

		switch node.Role {
		case deliver.RoleRouter:
			party.RouterConfig = &ordererpb.RouterNodeConfig{SignCert: certPEM}
		case deliver.RoleBatcher:
			party.BatchersConfig = append(party.BatchersConfig, &ordererpb.BatcherNodeConfig{
				ShardID:  uint32(node.ShardID),
				SignCert: certPEM,
			})
		case deliver.RoleConsenter:
			party.ConsenterConfig = &ordererpb.ConsenterNodeConfig{SignCert: certPEM}
		case deliver.RoleAssembler:
			party.AssemblerConfig = &ordererpb.AssemblerNodeConfig{SignCert: certPEM}
		}
	}

	return slices.Collect(maps.Values(parties))
}

// signRequest creates a deliver request of the kind one node sends to another, signed by signer.
func signRequest(t *testing.T, signer identity.SignerSerializer) *cb.Envelope {
	t.Helper()

	seekInfo := &ab.SeekInfo{
		Start:    &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}},
		Stop:     &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}},
		Behavior: ab.SeekInfo_BLOCK_UNTIL_READY,
	}

	request, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		cb.HeaderType_DELIVER_SEEK_INFO, "arma", signer, seekInfo, 0, 0, nil,
	)
	require.NoError(t, err)

	return request
}

// testSigner signs with the given key and presents the given signing certificate, which need not
// belong to that key.
type testSigner struct {
	certPEM []byte
	key     *ecdsa.PrivateKey
}

func (s *testSigner) Serialize() ([]byte, error) {
	return msp.NewSerializedIdentity(testMSPID, s.certPEM)
}

func (s *testSigner) Sign(message []byte) ([]byte, error) {
	digest := sha256.Sum256(message)
	return ecdsa.SignASN1(rand.Reader, s.key, digest[:])
}

// idOfCertSigner signs with the given key, and presents an identity that references its signing
// certificate by an identifier instead of carrying it.
type idOfCertSigner struct {
	key *ecdsa.PrivateKey
}

func (s *idOfCertSigner) Serialize() ([]byte, error) {
	return msp.NewSerializedIdentityWithIDOfCert(testMSPID, "an identifier of a signing certificate")
}

func (s *idOfCertSigner) Sign(message []byte) ([]byte, error) {
	digest := sha256.Sum256(message)
	return ecdsa.SignASN1(rand.Reader, s.key, digest[:])
}

// signersOfNodes generates a signing certificate and a signer for each of the given nodes.
func signersOfNodes(t *testing.T, nodes []deliver.NodeIdentity) (map[deliver.NodeIdentity][]byte, map[deliver.NodeIdentity]*testSigner) {
	t.Helper()

	certs := make(map[deliver.NodeIdentity][]byte, len(nodes))
	signers := make(map[deliver.NodeIdentity]*testSigner, len(nodes))
	for _, node := range nodes {
		certPEM, key := generateSignCert(t)
		certs[node] = certPEM
		signers[node] = &testSigner{certPEM: certPEM, key: key}
	}

	return certs, signers
}

// generateSignCert generates a signing certificate of a node, and the key that signs on its behalf.
func generateSignCert(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	return selfSignedCert(t, &key.PublicKey, key), key
}

// certOfSameBody signs the body of the certificate again. An MSP does the same when it rewrites a
// high-S ECDSA signature to low-S: the certificate keeps its body, its key and its validity, and
// only the signature over it changes.
func certOfSameBody(t *testing.T, certPEM []byte, key *ecdsa.PrivateKey) []byte {
	t.Helper()

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	reSigned, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	require.Equal(t, cert.RawTBSCertificate, reSigned.RawTBSCertificate)
	require.NotEqual(t, cert.Raw, reSigned.Raw)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// generateRSASignCert generates a signing certificate that holds an RSA public key.
func generateRSASignCert(t *testing.T) []byte {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	return selfSignedCert(t, &key.PublicKey, key)
}

func selfSignedCert(t *testing.T, publicKey, privateKey any) []byte {
	t.Helper()

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "arma node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
