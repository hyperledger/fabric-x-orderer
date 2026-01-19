/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package msp

import (
	"github.com/IBM/idemix"
	"github.com/cockroachdb/errors"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-x-common/api/applicationpb"
)

type idemixSigningIdentityWrapper struct {
	*idemix.IdemixSigningIdentity
}

func (i *idemixSigningIdentityWrapper) GetPublicVersion() Identity {
	return &idemixIdentityWrapper{Idemixidentity: i.IdemixSigningIdentity.GetPublicVersion().(*idemix.Idemixidentity)}
}

func (i *idemixSigningIdentityWrapper) GetIdentifier() *IdentityIdentifier {
	return i.GetPublicVersion().GetIdentifier()
}

func (i *idemixSigningIdentityWrapper) GetOrganizationalUnits() []*OUIdentifier {
	return i.GetPublicVersion().GetOrganizationalUnits()
}

func (*idemixSigningIdentityWrapper) SerializeWithIDOfCert() ([]byte, error) {
	return nil, errors.New("not applicable")
}

func (*idemixSigningIdentityWrapper) SerializeWithCert() ([]byte, error) {
	return nil, errors.New("not applicable")
}

func (*idemixSigningIdentityWrapper) GetCertificatePEM() ([]byte, error) {
	return nil, nil
}

type idemixIdentityWrapper struct {
	*idemix.Idemixidentity
}

func (i *idemixIdentityWrapper) GetIdentifier() *IdentityIdentifier {
	id := i.Idemixidentity.GetIdentifier()

	return &IdentityIdentifier{
		Mspid: id.Mspid,
		Id:    id.Id,
	}
}

func (*idemixIdentityWrapper) SerializeWithIDOfCert() ([]byte, error) {
	return nil, errors.New("not applicable")
}

func (*idemixIdentityWrapper) SerializeWithCert() ([]byte, error) {
	return nil, errors.New("not applicable")
}

// GetCertificatePEM returns the certificate in PEM format.
func (*idemixIdentityWrapper) GetCertificatePEM() ([]byte, error) {
	return nil, nil
}

func (i *idemixIdentityWrapper) GetOrganizationalUnits() []*OUIdentifier {
	ous := i.Idemixidentity.GetOrganizationalUnits()
	wous := []*OUIdentifier{}
	for _, ou := range ous {
		wous = append(wous, &OUIdentifier{
			CertifiersIdentifier:         ou.CertifiersIdentifier,
			OrganizationalUnitIdentifier: ou.OrganizationalUnitIdentifier,
		})
	}

	return wous
}

type idemixMSPWrapper struct {
	*idemix.Idemixmsp
}

func (i *idemixMSPWrapper) deserializeIdentityInternal(serializedIdentity []byte) (Identity, error) {
	id, err := i.Idemixmsp.DeserializeIdentityInternal(serializedIdentity)
	if err != nil {
		return nil, err
	}

	return &idemixIdentityWrapper{id.(*idemix.Idemixidentity)}, nil
}

func (i *idemixMSPWrapper) DeserializeIdentity(identity *applicationpb.Identity) (Identity, error) { //nolint:ireturn
	si := msp.SerializedIdentity{Mspid: identity.MspId, IdBytes: identity.GetCertificate()}
	siBytes, err := proto.Marshal(&si)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal serialized identity")
	}
	id, err := i.Idemixmsp.DeserializeIdentity(siBytes)
	if err != nil {
		return nil, err
	}

	return &idemixIdentityWrapper{id.(*idemix.Idemixidentity)}, nil
}

// GetKnownDeserializedIdentity returns a known identity matching the given IdentityIdentifier.
//
//nolint:ireturn //Identity is an interface.
func (*idemixMSPWrapper) GetKnownDeserializedIdentity(IdentityIdentifier) Identity {
	return nil
}

func (i *idemixMSPWrapper) IsWellFormed(identity *applicationpb.Identity) error {
	return i.Idemixmsp.IsWellFormed(&msp.SerializedIdentity{Mspid: identity.MspId, IdBytes: identity.GetCertificate()})
}

func (i *idemixMSPWrapper) GetVersion() MSPVersion {
	return MSPVersion(i.Idemixmsp.GetVersion())
}

func (i *idemixMSPWrapper) GetType() ProviderType {
	return ProviderType(i.Idemixmsp.GetType())
}

func (*idemixMSPWrapper) GetCertificatePEM() ([]byte, error) {
	return nil, nil
}

func (i *idemixMSPWrapper) GetDefaultSigningIdentity() (SigningIdentity, error) {
	id, err := i.Idemixmsp.GetDefaultSigningIdentity()
	if err != nil {
		return nil, err
	}

	return &idemixSigningIdentityWrapper{id.(*idemix.IdemixSigningIdentity)}, nil
}

func (i *idemixMSPWrapper) Validate(id Identity) error {
	return i.Idemixmsp.Validate(id.(*idemixIdentityWrapper).Idemixidentity)
}

func (i *idemixMSPWrapper) SatisfiesPrincipal(id Identity, principal *msp.MSPPrincipal) error {
	return i.Idemixmsp.SatisfiesPrincipal(id.(*idemixIdentityWrapper).Idemixidentity, principal)
}
