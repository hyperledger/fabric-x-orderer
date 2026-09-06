/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/msp"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
	policyMock "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSigVerifyFilter(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))
	_, err := v.VerifyStructureAndClassify(nil)
	require.EqualError(t, err, "failed to convert request to signedData : nil request")

	req := &comm.Request{}
	_, err = v.VerifyStructureAndClassify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing header in request's payload")

	payload := &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: nil}}
	p, err := proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing signature header in payload's header")

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: make([]byte, 10)}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "failed unmarshalling signature header")

	id, err := msp.NewSerializedIdentity("org1", []byte("cert"))
	require.NoError(t, err)

	sigheader, err := proto.Marshal(&common.SignatureHeader{
		Creator: id,
		Nonce:   []byte("nonce"),
	})
	require.NoError(t, err)

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "failed unmarshalling channel header")

	chdr := &common.ChannelHeader{ChannelId: "ChannelId", Type: int32(common.HeaderType_MESSAGE)}
	chdrBytes, err := proto.Marshal(chdr)
	require.NoError(t, err)
	payload = &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	reqType, err := v.VerifyStructureAndClassify(req)
	require.NoError(t, err)
	require.Equal(t, common.HeaderType_MESSAGE, reqType)
}

func TestSigVerifyConfigUpdate(t *testing.T) {
	// Scenario:
	// 1. A filter does not require client signature verification.
	// 2. A config update is classified as such and admitted once the policy is satisfied.
	// 3. The same config update is rejected when the policy is not satisfied, so that a signature
	//    a client can produce on its own does not authorize a reconfiguration.
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	policy := &policyMock.FakePolicyEvaluator{}
	policyManager := &policyMock.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	fc.GetPolicyManagerReturns(policyManager)
	fc.GetChannelIDReturns("arma")
	fc.GetClientSignatureVerificationRequiredReturns(false)

	signer := newSignerFixture(t)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	req := signer.signRequest(t, common.HeaderType_CONFIG_UPDATE, []byte("data"))
	reqType, err := v.VerifyStructureAndClassify(req)
	require.NoError(t, err)
	require.Equal(t, common.HeaderType_CONFIG_UPDATE, reqType)
	require.Equal(t, 1, policy.EvaluateSignedDataCallCount())

	policy.EvaluateSignedDataReturns(errors.New("not an administrator"))
	_, err = v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "signature did not satisfy policy")
}

func TestSigVerifyResolvesCertificateID(t *testing.T) {
	// Scenario:
	// 1. A filter requires client signature verification and the configuration declares a
	//    certificate, as it does for a client that names its signer by certificate id.
	// 2. A request naming that certificate id is admitted, and the certificate is looked up once
	//    however many requests name it.
	// 3. A request naming that certificate id but signed by another key is rejected.
	// 4. A request naming a certificate id the configuration does not declare is rejected.
	// 5. After a configuration change the certificate is looked up again.
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	fc.GetClientSignatureVerificationRequiredReturns(true)

	signer := newSignerFixture(t)
	deserializer := &stubDeserializer{declared: map[string][]byte{"client": signer.certPEM}}
	fc.GetMSPManagerReturns(deserializer)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	for range 4 {
		_, err := v.VerifyStructureAndClassify(signer.signRequestWithCertID(t, "client"))
		require.NoError(t, err)
	}
	require.Equal(t, 1, deserializer.lookups)

	impostor := signer.signRequestWithCertID(t, "client")
	impostor.Signature = newSignerFixture(t).sign(t, impostor.Payload)
	_, err := v.VerifyStructureAndClassify(impostor)
	require.ErrorContains(t, err, "the signature is invalid")

	_, err = v.VerifyStructureAndClassify(signer.signRequestWithCertID(t, "stranger"))
	require.ErrorContains(t, err, "org1 does not declare a certificate stranger")

	require.NoError(t, v.Update(fc))
	_, err = v.VerifyStructureAndClassify(signer.signRequestWithCertID(t, "client"))
	require.NoError(t, err)
	require.Equal(t, 2, deserializer.lookups)
}

func TestSigValidationFlag(t *testing.T) {
	// Scenario:
	// 1. A filter does not require client signature verification, and admits an unsigned request.
	// 2. Verification is turned on, and a correctly signed request is admitted.
	// 3. A request whose payload was altered after signing is rejected.
	// 4. A request signed by a key other than the one its certificate names is rejected.
	// 5. Verification is turned off again, and the rejected request is admitted unchecked.
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	fc.GetClientSignatureVerificationRequiredReturns(false)

	signer := newSignerFixture(t)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	_, err := v.VerifyStructureAndClassify(tx.CreateStructuredRequest([]byte("data")))
	require.NoError(t, err)

	fc.GetClientSignatureVerificationRequiredReturns(true)
	require.NoError(t, v.Update(fc))

	req := signer.signRequest(t, common.HeaderType_MESSAGE, []byte("data"))
	_, err = v.VerifyStructureAndClassify(req)
	require.NoError(t, err)

	altered := signer.signRequest(t, common.HeaderType_MESSAGE, []byte("data"))
	altered.Payload = signer.signRequest(t, common.HeaderType_MESSAGE, []byte("other data")).Payload
	_, err = v.VerifyStructureAndClassify(altered)
	require.ErrorContains(t, err, "the signature is invalid")

	impostor := signer.signRequest(t, common.HeaderType_MESSAGE, []byte("data"))
	impostor.Signature = newSignerFixture(t).sign(t, impostor.Payload)
	_, err = v.VerifyStructureAndClassify(impostor)
	require.ErrorContains(t, err, "the signature is invalid")

	fc.GetClientSignatureVerificationRequiredReturns(false)
	require.NoError(t, v.Update(fc))
	_, err = v.VerifyStructureAndClassify(impostor)
	require.NoError(t, err)
}

func TestSigVerifyRejectsUnusableCertificate(t *testing.T) {
	// Scenario:
	// 1. A filter requires client signature verification.
	// 2. A request whose creator certificate is not PEM encoded is rejected.
	// 3. A request whose creator certificate is PEM but not a certificate is rejected.
	// 4. A request whose creator certificate carries an RSA key is rejected.
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	fc.GetClientSignatureVerificationRequiredReturns(true)

	signer := newSignerFixture(t)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	notPEM := signer.signRequestWithCert(t, common.HeaderType_MESSAGE, []byte("data"), []byte("cert"))
	_, err := v.VerifyStructureAndClassify(notPEM)
	require.ErrorContains(t, err, "creator certificate is not PEM encoded")

	garbage := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a certificate")})
	notACert := signer.signRequestWithCert(t, common.HeaderType_MESSAGE, []byte("data"), garbage)
	_, err = v.VerifyStructureAndClassify(notACert)
	require.ErrorContains(t, err, "failed parsing creator certificate")

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	rsaCert := selfSignedCertPEM(t, rsaKey, &rsaKey.PublicKey)
	notECDSA := signer.signRequestWithCert(t, common.HeaderType_MESSAGE, []byte("data"), rsaCert)
	_, err = v.VerifyStructureAndClassify(notECDSA)
	require.ErrorContains(t, err, "creator certificate does not carry an ECDSA public key")
}

func TestSigFilterType(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	t.Run("data request", func(t *testing.T) {
		dataReq := tx.CreateStructuredRequest([]byte("123"))
		reqType, err := v.VerifyStructureAndClassify(dataReq)
		require.NoError(t, err)
		require.Equal(t, common.HeaderType_MESSAGE, reqType)
	})
}

// signerFixture is a real ECDSA key and a certificate naming its public key, so that tests drive
// the curve operation the filter performs rather than a stub.
type signerFixture struct {
	signer  *crypto.ECDSASigner
	certPEM []byte
}

func newSignerFixture(t *testing.T) *signerFixture {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	return &signerFixture{
		signer:  (*crypto.ECDSASigner)(key),
		certPEM: selfSignedCertPEM(t, key, &key.PublicKey),
	}
}

// sign signs arbitrary bytes with the fixture's key, the way a client signs a request payload.
func (s *signerFixture) sign(t *testing.T, data []byte) []byte {
	t.Helper()

	signature, err := s.signer.Sign(data)
	require.NoError(t, err)
	return signature
}

// signRequest builds a request of the given type naming this fixture's certificate as its creator,
// signed by the matching key.
func (s *signerFixture) signRequest(t *testing.T, reqType common.HeaderType, data []byte) *comm.Request {
	t.Helper()
	return s.signRequestWithCert(t, reqType, data, s.certPEM)
}

// signRequestWithCert builds a request naming an arbitrary certificate as its creator, so that a
// certificate the filter cannot use can be presented alongside a well-formed signature.
func (s *signerFixture) signRequestWithCert(t *testing.T, reqType common.HeaderType, data []byte, cert []byte) *comm.Request {
	t.Helper()
	return s.signRequestAs(t, reqType, data, msppb.NewIdentity("org1", cert))
}

// signRequestWithCertID builds a request that names its signer by certificate id and carries no
// certificate, the way a client sending the smallest possible request does.
func (s *signerFixture) signRequestWithCertID(t *testing.T, certID string) *comm.Request {
	t.Helper()
	identity := msppb.NewIdentityWithIDOfCert("org1", certID)
	return s.signRequestAs(t, common.HeaderType_MESSAGE, []byte("data"), identity)
}

func (s *signerFixture) signRequestAs(t *testing.T, reqType common.HeaderType, data []byte, id *msppb.Identity) *comm.Request {
	t.Helper()

	chdr, err := proto.Marshal(&common.ChannelHeader{ChannelId: "arma", Type: int32(reqType)})
	require.NoError(t, err)

	creator, err := proto.Marshal(id)
	require.NoError(t, err)

	shdr, err := proto.Marshal(&common.SignatureHeader{Creator: creator, Nonce: []byte("nonce")})
	require.NoError(t, err)

	payload, err := proto.Marshal(&common.Payload{
		Header: &common.Header{ChannelHeader: chdr, SignatureHeader: shdr},
		Data:   data,
	})
	require.NoError(t, err)

	return &comm.Request{Payload: payload, Signature: s.sign(t, payload)}
}

// stubDeserializer stands in for the configuration's certificates and counts how often one is
// looked up, so that a test can tell a cached resolution from a repeated one.
type stubDeserializer struct {
	declared map[string][]byte
	lookups  int
}

func (s *stubDeserializer) DeserializeIdentity(identity *msppb.Identity) (msp.Identity, error) {
	return nil, errors.New("not deserializing")
}

func (s *stubDeserializer) GetKnownDeserializedIdentity(id msp.IdentityIdentifier) msp.Identity {
	certPEM, declared := s.declared[id.Id]
	if !declared {
		return nil
	}
	s.lookups++

	return &stubIdentity{certPEM: certPEM}
}

func (s *stubDeserializer) IsWellFormed(identity *msppb.Identity) error {
	return nil
}

// stubIdentity is the identity a stubDeserializer hands back; only its certificate is read.
type stubIdentity struct {
	msp.Identity
	certPEM []byte
}

func (s *stubIdentity) GetCertificatePEM() ([]byte, error) {
	return s.certPEM, nil
}

// selfSignedCertPEM issues a certificate for the given public key, signed by the given key.
func selfSignedCertPEM(t *testing.T, signingKey any, publicKey any) []byte {
	t.Helper()

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, signingKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
