/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package applicationpb

// NewIdentity creates a applicationpb.Identity with the certificate.
func NewIdentity(mspID string, certificate []byte) *Identity {
	return &Identity{
		MspId:   mspID,
		Creator: &Identity_Certificate{Certificate: certificate},
	}
}

// NewIdentityWithIDOfCert creates a applicationpb.Identity with the certificateID.
func NewIdentityWithIDOfCert(mspID, certificateID string) *Identity {
	return &Identity{
		MspId:   mspID,
		Creator: &Identity_CertificateId{CertificateId: certificateID},
	}
}
