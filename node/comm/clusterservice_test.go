/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

// TestConfigureNodeCerts_PruneOnIdentityChange asserts that when an existing
// node keeps its ID but rotates its identity (cert), any stream that was
// already authorized under the old identity is de-authorized, forcing
// re-authentication against the new cert.
func TestConfigureNodeCerts_PruneOnIdentityChange(t *testing.T) {
	oldIdentity := []byte("old-cert")
	newIdentity := []byte("new-cert")

	tests := []struct {
		name             string
		configuredCert   []byte
		expectAuthorized bool
	}{
		{
			name:             "identity unchanged keeps stream authorized",
			configuredCert:   oldIdentity,
			expectAuthorized: true,
		},
		{
			name:             "identity rotated de-authorizes stream",
			configuredCert:   newIdentity,
			expectAuthorized: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := &ClusterService{
				Logger: flogging.MustGetLogger("test"),
				Membership: &MembersConfig{
					MemberMapping: map[uint64][]byte{5: oldIdentity},
				},
			}

			// Simulate a stream that authenticated under the old identity.
			const streamID = uint64(1)
			cs.Membership.AuthorizedStreams.Store(streamID, authorizedStream{nodeID: 5, identity: oldIdentity})

			err := cs.ConfigureNodeCerts([]*common.Consenter{{Id: 5, Identity: tc.configuredCert}})
			if err != nil {
				t.Fatalf("ConfigureNodeCerts returned error: %v", err)
			}

			_, authorized := cs.Membership.AuthorizedStreams.Load(streamID)
			if authorized != tc.expectAuthorized {
				t.Fatalf("stream authorized = %v, want %v", authorized, tc.expectAuthorized)
			}
		})
	}
}

// TestConfigureNodeCerts_PruneOnRemovedID asserts that a stream whose node ID
// is no longer a member is de-authorized.
func TestConfigureNodeCerts_PruneOnRemovedID(t *testing.T) {
	identity := []byte("cert")

	cs := &ClusterService{
		Logger: flogging.MustGetLogger("test"),
		Membership: &MembersConfig{
			MemberMapping: map[uint64][]byte{5: identity},
		},
	}

	const streamID = uint64(1)
	cs.Membership.AuthorizedStreams.Store(streamID, authorizedStream{nodeID: 5, identity: identity})

	// Reconfigure with node 5 removed (only node 6 remains).
	err := cs.ConfigureNodeCerts([]*common.Consenter{{Id: 6, Identity: identity}})
	if err != nil {
		t.Fatalf("ConfigureNodeCerts returned error: %v", err)
	}

	if _, authorized := cs.Membership.AuthorizedStreams.Load(streamID); authorized {
		t.Fatalf("stream for removed node ID should be de-authorized")
	}
}
