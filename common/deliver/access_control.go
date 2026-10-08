/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
)

// AccessControl authorizes a request by a policy of the configuration the bundle carries.
// The MSPs of the configuration validate the identity that signed the request, so a node keeps being
// authorized across a change of its certificate, as long as its MSP still issues it.
type AccessControl struct {
	policy     policies.Policy
	policyName string
}

// NewAccessControl refuses a bundle that does not hold the policy, since every request would then
// be refused.
func NewAccessControl(bundle channelconfig.Resources, policyName string) (*AccessControl, error) {
	if bundle == nil {
		return nil, errors.New("no configuration bundle to take the policy from")
	}

	policy, exists := bundle.PolicyManager().GetPolicy(policyName)
	if !exists {
		return nil, errors.Errorf("the configuration holds no policy %s", policyName)
	}

	return &AccessControl{policy: policy, policyName: policyName}, nil
}

// CheckPolicy refuses a request whose signature does not satisfy the policy. The channel is not
// part of the decision.
func (a *AccessControl) CheckPolicy(envelope *cb.Envelope, channelID string) error {
	signedData, err := protoutil.EnvelopeAsSignedData(envelope)
	if err != nil {
		return errors.Wrap(err, "could not convert the request to signed data")
	}

	if err := a.policy.EvaluateSignedData(signedData); err != nil {
		return errors.Wrapf(err, "the request does not satisfy %s", a.policyName)
	}

	return nil
}
