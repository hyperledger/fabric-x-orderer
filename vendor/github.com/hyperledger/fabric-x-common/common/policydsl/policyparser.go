/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policydsl

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mb "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"google.golang.org/protobuf/proto"
)

type parser struct {
	principals      []*mb.MSPPrincipal
	principalsIDMap map[string]int32
}

// Gate values.
const (
	GateAnd   = "and"
	GateOr    = "or"
	GateOutOf = "outof"
)

// Pattern matches a role case-insensitively:
//
//	^                          - Assert start of string
//	(?i)                       - Enable case-insensitivity
//	(                          - Group 1 start: Identifier
//	  [[:alnum:].-]+           - Alphanumeric, dot, or hyphen (1 or more)
//	)                          - Group 1 end
//	[.]                        - Match a literal dot
//	(                          - Group 2 start: Role
//	  admin|member|client|...  - Specific system role options
//	)                          - Group 2 end
//	$                          - Assert end of string
var roleRegex = regexp.MustCompile("^(?i)([[:alnum:].-]+)[.](admin|member|client|peer|orderer)$")

// FromString takes a string representation of the policy,
// parses it and returns a SignaturePolicyEnvelope that
// implements that policy. The supported language is as follows:
//
//	GATE(P[, P])
//	OutOf(N, P[, P])
//
// where:
//   - GATE is either "And" or "Or" (case-insensitive)
//   - OutOf is case-insensitive
//   - P is either a principal or another nested call to GATE or OutOf
//   - N is a positive number up to the number of arguments
//
// A principal is defined as:
//
//	ORG.ROLE
//
// where:
//   - ORG is a string (representing the MSP identifier)
//   - ROLE takes the value of admin, member, client, peer, or orderer
func FromString(policy string) (*cb.SignaturePolicyEnvelope, error) {
	p := parser{
		principalsIDMap: make(map[string]int32),
	}
	program, err := expr.Compile(
		policy,
		expr.Patch(&p),
		expr.Function(GateAnd, p.and),
		expr.Function(GateOr, p.or),
		expr.Function(GateOutOf, p.nOutOf),
	)
	if err != nil {
		return nil, err
	}
	res, err := expr.Run(program, nil)
	if err != nil {
		return nil, err
	}
	rule, ok := res.(*cb.SignaturePolicy)
	if !ok {
		return nil, fmt.Errorf("invalid policy string '%s'", policy)
	}

	return &cb.SignaturePolicyEnvelope{
		Identities: p.principals,
		Version:    0,
		Rule:       rule,
	}, nil
}

func (p *parser) nOutOf(args ...any) (any, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("expected at least two arguments to NOutOf. Given %d", len(args))
	}

	// How many of the policies we expect to accept.
	t, ok := args[0].(int)
	if !ok {
		return nil, fmt.Errorf("unrecognized type, expected a number, got %s", reflect.TypeOf(args[0]))
	}

	// Sanity check - t should be positive, permit equal to n+1, but disallow over n+1.
	n := len(args[1:])
	if t < 0 || t > n+1 {
		return nil, fmt.Errorf("invalid t-out-of-n predicate, t %d, n %d", t, n)
	}

	policies, err := p.parsePolicies(args[1:])
	if err != nil {
		return nil, err
	}
	return NOutOf(int32(t), policies), nil //nolint:gosec // strconv.Atoi result conversion to int16/32.
}

func (p *parser) and(args ...any) (any, error) {
	policies, err := p.parsePolicies(args)
	if err != nil {
		return nil, err
	}
	return NOutOf(int32(len(policies)), policies), nil //nolint:gosec // int -> int32.
}

func (p *parser) or(args ...any) (any, error) {
	policies, err := p.parsePolicies(args)
	if err != nil {
		return nil, err
	}
	return NOutOf(1, policies), nil
}

func (p *parser) parsePolicies(policyArgs []any) ([]*cb.SignaturePolicy, error) {
	if len(policyArgs) < 1 {
		return nil, fmt.Errorf("at least one policy arguments expected, got %d", len(policyArgs))
	}

	policies := make([]*cb.SignaturePolicy, len(policyArgs))
	for i, arg := range policyArgs {
		switch principal := arg.(type) {
		// If it's a string, we expect it to be formed as <MSP_ID>.<ROLE>,
		// where MSP_ID is the MSP identifier and ROLE is either a member,
		// an admin, a client, a peer or an orderer.
		case string:
			policy, err := p.parsePolicy(principal)
			if err != nil {
				return nil, err
			}
			policies[i] = policy

		// If we've already got a policy we're good, just append it.
		case *cb.SignaturePolicy:
			policies[i] = principal

		default:
			valType := reflect.TypeOf(principal)
			return nil, fmt.Errorf("unrecognized type, expected a principal or a policy, got %s", valType)
		}
	}
	return policies, nil
}

func (p *parser) parsePolicy(val string) (*cb.SignaturePolicy, error) {
	subMatch := roleRegex.FindStringSubmatch(val)
	if subMatch == nil {
		return nil, fmt.Errorf("unrecognized token '%s' in policy string", val)
	}
	mspID := subMatch[1]
	roleName := strings.ToUpper(subMatch[2])
	key := mspID + "." + roleName

	principalID, ok := p.principalsIDMap[key]
	if !ok {
		principal, err := newPrinciple(mspID, roleName)
		if err != nil {
			return nil, err
		}
		principalID = int32(len(p.principals)) //nolint:gosec // int -> int32.
		p.principals = append(p.principals, principal)
		p.principalsIDMap[key] = principalID
	}

	// Create a SignaturePolicy that requires a signature from the principal we've just built.
	return SignedBy(principalID), nil
}

func newPrinciple(mspID, roleName string) (*mb.MSPPrincipal, error) {
	// get the MSP role.
	role, ok := mb.MSPRole_MSPRoleType_value[roleName]
	if !ok {
		return nil, fmt.Errorf("error parsing role %s", roleName)
	}

	// Build the principal we've been told.
	mspRole, err := proto.Marshal(&mb.MSPRole{
		MspIdentifier: mspID,
		Role:          mb.MSPRole_MSPRoleType(role),
	})
	if err != nil {
		return nil, fmt.Errorf("error marshalling msp role: %w", err)
	}

	return &mb.MSPPrincipal{
		PrincipalClassification: mb.MSPPrincipal_ROLE,
		Principal:               mspRole,
	}, nil
}

// Visit implements the expr.Visitor interface.
// It is used to traverse the AST and enforce lowercase naming conventions.
func (*parser) Visit(node *ast.Node) {
	if callNode, isCallNode := (*node).(*ast.CallNode); isCallNode {
		if identifierNode, isIDNode := callNode.Callee.(*ast.IdentifierNode); isIDNode {
			// Enforce lowercase naming convention in the AST
			identifierNode.Value = strings.ToLower(identifierNode.Value)
		}
	}
}
