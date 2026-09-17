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

// ChannelReadersChecker authorizes a deliver request by evaluating the signature over the request
// envelope against the channel's Readers policy.
type ChannelReadersChecker struct {
	bundle channelconfig.Resources
}

// NewChannelReadersChecker returns a checker over the policies of the given config bundle. The
// bundle reflects a single config sequence, so a reconfigured channel needs a new checker.
func NewChannelReadersChecker(bundle channelconfig.Resources) *ChannelReadersChecker {
	return &ChannelReadersChecker{bundle: bundle}
}

// CheckPolicy returns nil if the identity that signed the envelope satisfies the channel's
// Readers policy.
func (c *ChannelReadersChecker) CheckPolicy(envelope *cb.Envelope, _ string) error {
	signedData, err := protoutil.EnvelopeAsSignedData(envelope)
	if err != nil {
		return errors.Wrap(err, "could not convert message to signedData")
	}

	policy, ok := c.bundle.PolicyManager().GetPolicy(policies.ChannelReaders)
	if !ok {
		return errors.Errorf("could not find policy %s", policies.ChannelReaders)
	}

	if err := policy.EvaluateSignedData(signedData); err != nil {
		return errors.Wrapf(err, "policy %s evaluation failed", policies.ChannelReaders)
	}

	return nil
}
