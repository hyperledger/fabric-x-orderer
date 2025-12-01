/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cauthdsl

import (
	"bytes"
	"time"

	"github.com/cockroachdb/errors"
	mb "github.com/hyperledger/fabric-protos-go-apiv2/msp"

	"github.com/hyperledger/fabric-x-common/msp"
)

// InvalidSignature denotes a non-valid signature.
var InvalidSignature = []byte("badsigned")

// MockIdentityDeserializer is implemented by both MSPManger and MSP.
type MockIdentityDeserializer struct {
	Fail error
}

// DeserializeIdentity deserializes an identity.
func (md *MockIdentityDeserializer) DeserializeIdentity( //nolint:ireturn
	serializedIdentity []byte,
) (msp.Identity, error) {
	if md.Fail != nil {
		return nil, md.Fail
	}
	return &MockIdentity{IDBytes: serializedIdentity}, nil
}

// IsWellFormed checks if the given identity can be deserialized into its provider-specific form.
func (*MockIdentityDeserializer) IsWellFormed(_ *mb.SerializedIdentity) error {
	return nil
}

// GetKnownDeserializedIdentity returns a known identity matching the given IdentityIdentifier.
//
//nolint:ireturn //Identity is an interface.
func (*MockIdentityDeserializer) GetKnownDeserializedIdentity(msp.IdentityIdentifier) msp.Identity {
	return nil
}

// MockIdentity interface defining operations associated to a "certificate".
type MockIdentity struct {
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
	if !bytes.Equal(id.IDBytes, p.Principal) {
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

// Serialize converts an identity to bytes.
func (id *MockIdentity) Serialize() ([]byte, error) {
	return id.IDBytes, nil
}

// ToIdentities convert serialized identities to msp Identity.
func ToIdentities(idBytesSlice [][]byte, deserializer msp.IdentityDeserializer) ([]msp.Identity, []bool) {
	identities := make([]msp.Identity, len(idBytesSlice))
	for i, idBytes := range idBytesSlice {
		id, _ := deserializer.DeserializeIdentity(idBytes)
		identities[i] = id
	}

	return identities, make([]bool, len(idBytesSlice))
}
