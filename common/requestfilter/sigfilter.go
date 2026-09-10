/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/protobuf/proto"
)

type SigFilter struct {
	clientSignatureVerificationRequired bool
	channelID                           string
	policyName                          string
	policyManager                       policies.Manager
	mspManager                          msp.IdentityDeserializer
	// signerKeys maps the identifier of a certificate the configuration declares to the public key
	// that certificate carries, so that a request naming its signer by certificate id costs a map
	// read rather than a lookup and a certificate parse. It is replaced wholesale when the
	// configuration changes, so a certificate the new configuration no longer declares is dropped.
	signerKeys atomic.Pointer[sync.Map]
}

func NewSigFilter(config FilterConfig, policyName string) *SigFilter {
	sf := &SigFilter{
		clientSignatureVerificationRequired: config.GetClientSignatureVerificationRequired(),
		channelID:                           config.GetChannelID(),
		policyName:                          policyName,
		policyManager:                       config.GetPolicyManager(),
		mspManager:                          config.GetMSPManager(),
	}
	sf.signerKeys.Store(&sync.Map{})

	return sf
}

func (sf *SigFilter) VerifyAndClassify(request *comm.Request) (common.HeaderType, error) {
	// extract signedData, while verifying the structure of the request
	signedData, reqType, err := sf.requestToSignedData(request)
	if err != nil {
		return reqType, fmt.Errorf("failed to convert request to signedData : %s", err)
	}

	// A config update has to satisfy the channel policy and not merely carry a valid signature:
	// admitting one on the strength of its signature alone would let any client the configuration
	// knows reconfigure the channel. Config updates are rare, so what the policy walk costs per
	// request does not matter here.
	if reqType == common.HeaderType_CONFIG_UPDATE {
		return reqType, sf.evaluatePolicy(signedData)
	}

	if sf.clientSignatureVerificationRequired {
		if err := sf.verifySignature(signedData); err != nil {
			return reqType, err
		}
	}

	return reqType, nil
}

// evaluatePolicy hands the signer of a request to the policy tree, which decides whether that
// signer is allowed to submit it.
func (sf *SigFilter) evaluatePolicy(signedData *protoutil.SignedData) error {
	policy, exists := sf.policyManager.GetPolicy(sf.policyName)
	if !exists {
		return fmt.Errorf("no policies in config block")
	}

	// Deserialize the signer and check the signature once, then let the policy tree evaluate
	// the resulting identity. EvaluateSignedData hands the signature set to every sub-policy
	// the tree tries, so each one repeats both the deserialization and the signature check,
	// and admitting a request costs one of each per organization tried before one matches.
	//
	// A configuration that carries no MSP manager cannot convert the signature set, so it
	// evaluates the set itself and pays that repetition.
	var err error
	if sf.mspManager != nil {
		ids := policies.SignatureSetToValidIdentities([]*protoutil.SignedData{signedData}, sf.mspManager)
		err = policy.EvaluateIdentities(ids)
	} else {
		err = policy.EvaluateSignedData([]*protoutil.SignedData{signedData})
	}
	if err != nil {
		return fmt.Errorf("signature did not satisfy policy %s", sf.policyName)
	}

	return nil
}

// verifySignature checks the request signature against the public key of the signer the request
// names. It performs the curve operation and nothing else: the certificate is not validated against
// a membership service provider and no channel policy is evaluated.
func (sf *SigFilter) verifySignature(signedData *protoutil.SignedData) error {
	publicKey, err := sf.signerPublicKey(signedData.Identity)
	if err != nil {
		return err
	}

	digest := sha256.Sum256(signedData.Data)
	if !ecdsa.VerifyASN1(publicKey, digest[:], signedData.Signature) {
		return fmt.Errorf("the signature is invalid")
	}

	return nil
}

// signerPublicKey returns the public key of the signer a request names, either by carrying its
// certificate or by naming the identifier of a certificate the configuration declares.
func (sf *SigFilter) signerPublicKey(id *msppb.Identity) (*ecdsa.PublicKey, error) {
	if certPEM := id.GetCertificate(); len(certPEM) > 0 {
		return publicKeyFromPEM(certPEM)
	}

	certID := id.GetCertificateId()
	if certID == "" {
		return nil, fmt.Errorf("the request names neither a certificate nor a certificate id")
	}

	identifier := msp.IdentityIdentifier{Mspid: id.MspId, Id: certID}
	keys := sf.signerKeys.Load()
	if cached, found := keys.Load(identifier); found {
		if publicKey, isKey := cached.(*ecdsa.PublicKey); isKey {
			return publicKey, nil
		}
	}

	publicKey, err := sf.declaredPublicKey(identifier)
	if err != nil {
		return nil, err
	}
	keys.Store(identifier, publicKey)

	return publicKey, nil
}

// declaredPublicKey extracts the public key of a certificate the configuration declares. It runs
// once per signer, on the first request that names it.
func (sf *SigFilter) declaredPublicKey(identifier msp.IdentityIdentifier) (*ecdsa.PublicKey, error) {
	if sf.mspManager == nil {
		return nil, fmt.Errorf("the configuration declares no certificates, cannot resolve %s", identifier.Id)
	}

	declared := sf.mspManager.GetKnownDeserializedIdentity(identifier)
	if declared == nil {
		return nil, fmt.Errorf("%s does not declare a certificate %s", identifier.Mspid, identifier.Id)
	}

	certPEM, err := declared.GetCertificatePEM()
	if err != nil {
		return nil, fmt.Errorf("failed obtaining the certificate of %s, err %s", identifier.Id, err)
	}

	return publicKeyFromPEM(certPEM)
}

func publicKeyFromPEM(certPEM []byte) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("creator certificate is not PEM encoded")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed parsing creator certificate, err %s", err)
	}

	publicKey, isECDSA := cert.PublicKey.(*ecdsa.PublicKey)
	if !isECDSA {
		return nil, fmt.Errorf("creator certificate does not carry an ECDSA public key")
	}

	return publicKey, nil
}

// requestToSignedData verifies the request structure and returns the payload, identity and signature in a SignedData.
// additionally, the request tyoe is extracted and returned.
func (sf *SigFilter) requestToSignedData(request *comm.Request) (*protoutil.SignedData, common.HeaderType, error) {
	var reqType common.HeaderType
	if request == nil {
		return nil, reqType, fmt.Errorf("nil request")
	}

	payload := &common.Payload{}
	err := proto.Unmarshal(request.Payload, payload)
	if err != nil {
		return nil, reqType, err
	}

	if payload.Header == nil {
		return nil, reqType, fmt.Errorf("missing header in request's payload")
	}

	if payload.Header.SignatureHeader == nil {
		return nil, reqType, fmt.Errorf("missing signature header in payload's header")
	}

	shdr := &common.SignatureHeader{}
	err = proto.Unmarshal(payload.Header.SignatureHeader, shdr)
	if err != nil {
		return nil, reqType, fmt.Errorf("failed unmarshalling signature header, err %s", err)
	}

	if payload.Header.ChannelHeader == nil {
		return nil, reqType, fmt.Errorf("missing channel header in request's payload")
	}

	chdr := &common.ChannelHeader{}
	err = proto.Unmarshal(payload.Header.ChannelHeader, chdr)
	if err != nil {
		return nil, reqType, fmt.Errorf("failed unmarshalling channel header, err %s", err.Error())
	}

	// TODO: check channel ID
	// if sf.channelID != chdr.ChannelId {
	// 	return nil, fmt.Errorf("channelID is incorrect. expected: %s, actual: %s", sf.channelID, chdr.ChannelId)
	// }

	id, err := protoutil.UnmarshalIdentity(shdr.Creator)
	if err != nil {
		return nil, reqType, err
	}

	return &protoutil.SignedData{
		Data:      request.Payload,
		Identity:  id,
		Signature: request.Signature,
	}, common.HeaderType(chdr.Type), nil
}

func (sf *SigFilter) Update(config FilterConfig) error {
	sf.clientSignatureVerificationRequired = config.GetClientSignatureVerificationRequired()
	sf.channelID = config.GetChannelID()
	sf.policyManager = config.GetPolicyManager()
	sf.mspManager = config.GetMSPManager()
	sf.signerKeys.Store(&sync.Map{})
	return nil
}
