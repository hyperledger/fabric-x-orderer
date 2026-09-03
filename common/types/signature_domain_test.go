/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/require"
)

func TestPrefixWithDomain(t *testing.T) {
	msg := []byte{1, 2, 3}
	tag := []byte(types.DomainBAF)

	out := types.PrefixWithDomain(types.DomainBAF, msg)

	// Framing is a 2-byte big-endian tag length, then the tag, then the message.
	require.Equal(t, byte(len(tag)>>8), out[0])
	require.Equal(t, byte(len(tag)), out[1])
	require.Equal(t, tag, out[2:2+len(tag)])
	require.Equal(t, msg, out[2+len(tag):])
}

func TestPrefixWithDomainSeparatesDomains(t *testing.T) {
	// The same payload signed under two different domains must never collide.
	msg := []byte{1, 2, 3}
	require.NotEqual(t,
		types.PrefixWithDomain(types.DomainBAF, msg),
		types.PrefixWithDomain(types.DomainComplaint, msg),
	)
}

// TestDomainFramesAreDisjoint is the core security property: because each
// domain contributes a distinct, length-prefixed tag at a fixed offset, no
// output produced under one domain can ever equal an output produced under
// another domain, for ANY payloads. This is what blocks replaying a signature
// over one message type (e.g. a BAF) as a signature over another (a Complaint).
func TestDomainFramesAreDisjoint(t *testing.T) {
	bafFrame := types.PrefixWithDomain(types.DomainBAF, nil)
	complaintFrame := types.PrefixWithDomain(types.DomainComplaint, nil)

	require.False(t, bytes.HasPrefix(bafFrame, complaintFrame))
	require.False(t, bytes.HasPrefix(complaintFrame, bafFrame))
}
