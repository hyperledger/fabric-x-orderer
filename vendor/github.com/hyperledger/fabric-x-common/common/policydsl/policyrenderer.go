/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policydsl

import (
	"encoding/base64"
	"fmt"
	"strings"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mb "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"google.golang.org/protobuf/proto"
)

// principalPlaceholderLen bounds how many bytes of a principal are encoded into a placeholder:
// an IDENTITY principal carries a full certificate, so dumping it untruncated would put a
// couple of KB into the rendered string.
const principalPlaceholderLen = 8

// Gate*Display are the canonical, human-readable gate spellings used by ToString and
// ToDisplayString. They deliberately do not reuse GateAnd/GateOr/GateOutOf, which are the
// lowercase parser tokens ("and"/"or"/"outof") rather than the display spelling; Visit
// lowercases gate identifiers during AST traversal, so this casing still round-trips through
// FromString.
const (
	GateAndDisplay   = "AND"
	GateOrDisplay    = "OR"
	GateOutOfDisplay = "OutOf"
)

// ToString renders an envelope as a policy DSL expression accepted by FromString. It returns
// an error if any part of the policy has no DSL representation.
//
// Example: the envelope produced by FromString("OR('Org1MSP.member', 'Org2MSP.member')")
// renders back to "OR('Org1MSP.member', 'Org2MSP.member')".
func ToString(env *cb.SignaturePolicyEnvelope) (string, error) {
	return renderRule(env.GetRule(), env.GetIdentities())
}

// ToDisplayString renders an envelope for human consumption, substituting readable
// placeholders for anything with no DSL representation. It never fails: renderRule always
// computes the display text, even when it also returns a "not parsable" error, and
// ToDisplayString simply discards that error.
//
// Example: an envelope built with Envelope(SignedBy(0), [][]byte{identityBytes}) — a single
// IDENTITY principal, which has no DSL syntax — renders to "<identity:0>" instead of erroring.
func ToDisplayString(env *cb.SignaturePolicyEnvelope) string {
	rendered, _ := renderRule(env.GetRule(), env.GetIdentities())
	return rendered
}

// renderRule renders a single SignaturePolicy node, dispatching on its oneof type: an NOutOf
// node (e.g. NOutOf{N: 1, Rules: [SignedBy(0), SignedBy(1)]}) renders via renderNOutOf (e.g.
// to "OR('Org1MSP.member', 'Org2MSP.member')"), a SignedBy node (e.g. SignedBy(0)) renders via
// renderSignedBy (e.g. to "'Org1MSP.member'"), and anything else — including a rule with no
// oneof case set at all, e.g. &cb.SignaturePolicy{} — renders as "<unknown-rule>" alongside an
// error, since neither today's two oneof cases nor a future third one is guaranteed.
//
// The returned string is always the fully rendered display text; the returned error, when
// non-nil, means that text has no DSL representation (it would not round-trip through
// FromString). ToString propagates the error; ToDisplayString discards it.
func renderRule(rule *cb.SignaturePolicy, identities []*mb.MSPPrincipal) (string, error) {
	switch t := rule.GetType().(type) {
	case *cb.SignaturePolicy_NOutOf_:
		return renderNOutOf(t.NOutOf, identities)
	case *cb.SignaturePolicy_SignedBy:
		return renderSignedBy(t.SignedBy, identities)
	default:
		return "<unknown-rule>", fmt.Errorf("policy rule has no DSL representation: %T", t)
	}
}

// renderNOutOf renders an NOutOf node as one of the three DSL gates, picking the gate from N
// relative to len(Rules):
//   - N == 1, e.g. NOutOf{N: 1, Rules: [SignedBy(0), SignedBy(1)]} -> "OR('A.member', 'B.member')"
//   - N == len(Rules), e.g. NOutOf{N: 2, Rules: [SignedBy(0), SignedBy(1)]} -> "AND('A.member', 'B.member')"
//   - otherwise, e.g. NOutOf{N: 2, Rules: [SignedBy(0), SignedBy(1), SignedBy(2)]} ->
//     "OutOf(2, 'A.member', 'B.member', 'C.member')"
//
// N == 1 is checked before N == len(Rules) so a single-rule NOutOf renders as "OR(...)" rather
// than "AND(...)"; both parse back to the identical envelope, so either spelling is faithful.
//
// An NOutOf with no rules at all, e.g. NOutOf{N: 0, Rules: []} (AcceptAllPolicy) or
// NOutOf{N: 1, Rules: []} (RejectAllPolicy), has no DSL representation: FromString's nOutOf
// rejects any OutOf/And/Or call with fewer than one policy argument (policyparser.go's
// "expected at least two arguments to NOutOf"), so there is no string of the form "OutOf(N)"
// that actually parses back to it, even though that is exactly the text rendered here. The
// returned error reflects that; ToDisplayString discards it and keeps the text.
func renderNOutOf(nOutOf *cb.SignaturePolicy_NOutOf, identities []*mb.MSPPrincipal) (string, error) {
	n := nOutOf.GetN()
	rules := nOutOf.GetRules()

	if len(rules) == 0 {
		return fmt.Sprintf("%s(%d)", GateOutOfDisplay, n),
			fmt.Errorf("NOutOf with no rules (n=%d) has no DSL representation", n)
	}

	rendered, err := renderAll(rules, identities)

	switch int(n) {
	case 1:
		return fmt.Sprintf("%s(%s)", GateOrDisplay, strings.Join(rendered, ", ")), err
	case len(rules):
		return fmt.Sprintf("%s(%s)", GateAndDisplay, strings.Join(rendered, ", ")), err
	default:
		return fmt.Sprintf("%s(%d, %s)", GateOutOfDisplay, n, strings.Join(rendered, ", ")), err
	}
}

// renderAll renders each rule in rules independently and returns the rendered strings in the
// same order, e.g. [SignedBy(0), SignedBy(1)] -> []string{"'Org1MSP.member'", "'Org2MSP.member'"}.
// Every rule is rendered regardless of errors, so the returned text is always complete even when
// a sub-rule has no DSL representation; the first such error encountered, if any, is returned
// alongside the full text.
func renderAll(rules []*cb.SignaturePolicy, identities []*mb.MSPPrincipal) ([]string, error) {
	rendered := make([]string, len(rules))
	var firstErr error
	for i, rule := range rules {
		r, err := renderRule(rule, identities)
		rendered[i] = r
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return rendered, firstErr
}

// renderSignedBy resolves a SignedBy index into the corresponding identities entry and renders
// that principal, e.g. SignedBy(0) against identities[0] = {MspIdentifier: "Org1MSP",
// Role: MEMBER} -> "'Org1MSP.member'". An index outside the identities slice (a malformed
// envelope) renders as "<principal[N]>" alongside an error.
func renderSignedBy(index int32, identities []*mb.MSPPrincipal) (string, error) {
	if index < 0 || int(index) >= len(identities) {
		return fmt.Sprintf("<principal[%d]>", index),
			fmt.Errorf("signed_by index %d out of range of %d identities", index, len(identities))
	}
	return renderPrincipal(identities[index], index)
}

// renderPrincipal renders an MSPPrincipal, dispatching on its PrincipalClassification:
//   - ROLE, e.g. {MspIdentifier: "Org1MSP", Role: MEMBER} -> "'Org1MSP.member'" (renderRolePrincipal)
//   - ORGANIZATION_UNIT, e.g. {MspIdentifier: "Org1MSP", OrganizationalUnitIdentifier: "eng"}
//     -> "'Org1MSP.eng'" alongside an error (renderOUPrincipal): unlike ROLE, FromString's
//     roleRegex only matches the five MSP role suffixes, so there is no DSL syntax for an
//     organization unit at all, in any spelling, even though the rendered text is shaped
//     exactly like a valid ROLE principal
//   - IDENTITY, which carries a full serialized identity with no DSL syntax -> "<identity:N>"
//     alongside an error (N is the identities index)
//   - ANONYMITY / COMBINED (no DSL syntax either) -> "<principal:...>" alongside an error
//     (a length-bounded encoding of the raw bytes)
func renderPrincipal(principal *mb.MSPPrincipal, index int32) (string, error) {
	switch principal.GetPrincipalClassification() {
	case mb.MSPPrincipal_ROLE:
		return renderRolePrincipal(principal)
	case mb.MSPPrincipal_ORGANIZATION_UNIT:
		return renderOUPrincipal(principal),
			fmt.Errorf("organization_unit principal at index %d has no DSL representation", index)
	case mb.MSPPrincipal_IDENTITY:
		return fmt.Sprintf("<identity:%d>", index),
			fmt.Errorf("identity principal at index %d has no DSL representation", index)
	default:
		return placeholderPrincipal(principal.GetPrincipal()), fmt.Errorf(
			"principal classification %s has no DSL representation", principal.GetPrincipalClassification(),
		)
	}
}

// renderRolePrincipal unmarshals a ROLE principal's bytes into an MSPRole and renders it as
// 'mspID.role', lowercasing the role, e.g. MSPRole{MspIdentifier: "Org1MSP", Role: ADMIN} ->
// "'Org1MSP.admin'". Bytes that fail to unmarshal (corrupt data, not a client bug) render as
// a placeholder alongside an error.
func renderRolePrincipal(principal *mb.MSPPrincipal) (string, error) {
	role := &mb.MSPRole{}
	if err := proto.Unmarshal(principal.GetPrincipal(), role); err != nil {
		return placeholderPrincipal(principal.GetPrincipal()), fmt.Errorf("could not unmarshal MSPRole: %w", err)
	}
	return fmt.Sprintf("'%s.%s'", role.GetMspIdentifier(), strings.ToLower(role.GetRole().String())), nil
}

// renderOUPrincipal unmarshals an ORGANIZATION_UNIT principal's bytes into an OrganizationUnit
// and renders it as 'mspID.ouID', e.g. OrganizationUnit{MspIdentifier: "Org1MSP",
// OrganizationalUnitIdentifier: "engineering"} -> "'Org1MSP.engineering'". Called from
// renderPrincipal, which always pairs this text with an error — ORGANIZATION_UNIT has no DSL
// representation at all, so this text is display-only and never valid DSL, despite looking
// exactly like a ROLE principal. Bytes that fail to unmarshal (corrupt data, not a client bug)
// render as a placeholder instead.
func renderOUPrincipal(principal *mb.MSPPrincipal) string {
	ou := &mb.OrganizationUnit{}
	if err := proto.Unmarshal(principal.GetPrincipal(), ou); err != nil {
		return placeholderPrincipal(principal.GetPrincipal())
	}
	return fmt.Sprintf("'%s.%s'", ou.GetMspIdentifier(), ou.GetOrganizationalUnitIdentifier())
}

// placeholderPrincipal renders raw principal bytes as a base64 placeholder, truncated to
// principalPlaceholderLen bytes, e.g. []byte("not an MSPRole") -> "<principal:bm90IGFuIE0=>".
// Truncation matters for IDENTITY principals, which carry a full certificate: dumping it
// untruncated would put a couple of KB into the rendered string.
func placeholderPrincipal(raw []byte) string {
	n := min(len(raw), principalPlaceholderLen)
	return "<principal:" + base64.StdEncoding.EncodeToString(raw[:n]) + ">"
}
