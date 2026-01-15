/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cauthdsl

import (
	"bytes"
	"encoding/hex"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	mb "github.com/hyperledger/fabric-protos-go-apiv2/msp"

	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/utils/certificate"
)

// InvalidSignature denotes a non-valid signature.
var InvalidSignature = []byte("badsigned")

// MockIdentityDeserializer is implemented by both MSPManger and MSP.
type MockIdentityDeserializer struct {
	Fail            error
	KnownIdentities map[msp.IdentityIdentifier]msp.Identity
}

// DeserializeIdentity deserializes an identity.
func (md *MockIdentityDeserializer) DeserializeIdentity( //nolint:ireturn
	id *applicationpb.Identity,
) (msp.Identity, error) {
	if md.Fail != nil {
		return nil, md.Fail
	}

	var idBytes []byte
	switch id.Creator.(type) {
	case *applicationpb.Identity_Certificate:
		idBytes = id.GetCertificate()
	case *applicationpb.Identity_CertificateId:
		return md.KnownIdentities[msp.IdentityIdentifier{Mspid: id.MspId, Id: id.GetCertificateId()}], nil
	}
	return &MockIdentity{MspID: id.MspId, IDBytes: idBytes}, nil
}

// IsWellFormed checks if the given identity can be deserialized into its provider-specific form.
func (*MockIdentityDeserializer) IsWellFormed(_ *applicationpb.Identity) error {
	return nil
}

// GetKnownDeserializedIdentity returns a known identity matching the given IdentityIdentifier.
//
//nolint:ireturn //Identity is an interface.
func (md *MockIdentityDeserializer) GetKnownDeserializedIdentity(id msp.IdentityIdentifier) msp.Identity {
	if md.KnownIdentities == nil {
		return nil
	}
	return md.KnownIdentities[id]
}

// MockIdentity interface defining operations associated to a "certificate".
type MockIdentity struct {
	MspID   string
	IDBytes []byte
}

// Anonymous returns true if this is an anonymous identity, false otherwise.
func (*MockIdentity) Anonymous() bool {
	panic("implement me")
}

// ExpiresAt returns the time at which the Identity expires.
func (*MockIdentity) ExpiresAt() time.Time {
	return time.Time{}
}

// SatisfiesPrincipal checks whether this instance matches
// the description supplied in MSPPrincipal.
func (id *MockIdentity) SatisfiesPrincipal(p *mb.MSPPrincipal) error {
	idBytes, err := msp.NewSerializedIdentity(id.MspID, id.IDBytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(idBytes, p.Principal) {
		return errors.New("Principals do not match")
	}
	return nil
}

// GetIdentifier returns the identifier of that identity.
func (id *MockIdentity) GetIdentifier() *msp.IdentityIdentifier {
	return &msp.IdentityIdentifier{Mspid: "Mock", Id: string(id.IDBytes)}
}

// GetMSPIdentifier returns the MSP Id for this instance.
func (*MockIdentity) GetMSPIdentifier() string {
	return "Mock"
}

// Validate uses the rules that govern this identity to validate it.
func (*MockIdentity) Validate() error {
	return nil
}

// GetOrganizationalUnits returns zero or more organization units or
// divisions this identity is related to as long as this is public
// information.
func (*MockIdentity) GetOrganizationalUnits() []*msp.OUIdentifier {
	return nil
}

// Verify a signature over some message using this identity as reference.
func (*MockIdentity) Verify(_, sig []byte) error {
	if bytes.Equal(sig, InvalidSignature) {
		return errors.New("Invalid signature")
	}
	return nil
}

// SerializeWithIDOfCert converts an identity to bytes.
func (id *MockIdentity) SerializeWithIDOfCert() ([]byte, error) {
	certID, err := certificate.DigestPemContent(id.IDBytes, bccsp.SHA256)
	if err != nil {
		return nil, err
	}
	return msp.NewSerializedIdentityWithIDOfCert(id.MspID, hex.EncodeToString(certID))
}

// Serialize converts an identity to bytes.
func (id *MockIdentity) Serialize() ([]byte, error) {
	return msp.NewSerializedIdentity(id.MspID, id.IDBytes)
}

// GetCertificatePEM returns the certificate in PEM format.
func (id *MockIdentity) GetCertificatePEM() ([]byte, error) {
	return id.IDBytes, nil
}

// ToIdentities convert serialized identities to msp Identity.
func ToIdentities(pIdentities []*applicationpb.Identity, deserializer msp.IdentityDeserializer) (
	[]msp.Identity, []bool,
) {
	identities := make([]msp.Identity, len(pIdentities))
	for i, id := range pIdentities {
		id, _ := deserializer.DeserializeIdentity(id)
		identities[i] = id
	}

	return identities, make([]bool, len(pIdentities))
}
