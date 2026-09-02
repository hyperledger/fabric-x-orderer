/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

// SignatureDomain is a context tag that binds a signature to a specific
// message type. Folding a distinct domain into the signed bytes provides
// domain separation: a signature produced over one message type cannot be
// replayed as a valid signature over another, even when the same key signs
// both families.
type SignatureDomain string

const (
	// DomainBAF tags bytes signed as a Batch Attestation Fragment.
	DomainBAF SignatureDomain = "arma.baf"
	// DomainComplaint tags bytes signed as a Complaint.
	DomainComplaint SignatureDomain = "arma.complaint"
)

// PrefixWithDomain binds msg to a signature domain by prepending a
// length-prefixed domain tag. The 2-byte big-endian length makes the
// tag/message boundary unambiguous, so outputs under distinct domains occupy
// disjoint byte spaces and can never collide.
func PrefixWithDomain(domain SignatureDomain, msg []byte) []byte {
	tag := []byte(domain)
	out := make([]byte, 0, 2+len(tag)+len(msg))
	out = append(out, byte(len(tag)>>8), byte(len(tag)))
	out = append(out, tag...)
	return append(out, msg...)
}
