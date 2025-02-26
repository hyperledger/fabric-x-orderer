/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package encoder_test

import (
	arma_testutil "github.ibm.com/decentralized-trust-research/arma/testutil/fabric"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.ibm.com/decentralized-trust-research/arma/internal/configtxgen/encoder"
	"github.ibm.com/decentralized-trust-research/arma/internal/configtxgen/genesisconfig"

	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/pkg/errors"
)

func hasModPolicySet(groupName string, cg *cb.ConfigGroup) error {
	if cg.ModPolicy == "" {
		return errors.Errorf("group %s has empty mod_policy", groupName)
	}

	for valueName, value := range cg.Values {
		if value.ModPolicy == "" {
			return errors.Errorf("group %s has value %s with empty mod_policy", groupName, valueName)
		}
	}

	for policyName, policy := range cg.Policies {
		if policy.ModPolicy == "" {
			return errors.Errorf("group %s has policy %s with empty mod_policy", groupName, policyName)
		}
	}

	for groupName, group := range cg.Groups {
		err := hasModPolicySet(groupName, group)
		if err != nil {
			return errors.WithMessagef(err, "missing sub-mod_policy for group %s", groupName)
		}
	}

	return nil
}

var _ = Describe("Integration", func() {
	DescribeTable("successfully parses the profile",
		func(profile string) {
			config := genesisconfig.Load(profile, arma_testutil.GetDevConfigDir())
			config.Capabilities = map[string]bool{"V2_0": true}
			group, err := encoder.NewChannelGroup(config)
			Expect(err).NotTo(HaveOccurred())

			cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
			Expect(err).NotTo(HaveOccurred())
			_, err = channelconfig.NewBundle("test", &cb.Config{
				ChannelGroup: group,
			}, cryptoProvider)
			Expect(err).NotTo(HaveOccurred())

			err = hasModPolicySet("Channel", group)
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("Sample Insecure Solo Profile", genesisconfig.SampleInsecureSoloProfile),
		Entry("Sample Single MSP Solo Profile", genesisconfig.SampleSingleMSPSoloProfile),
		Entry("Sample DevMode Solo Profile", genesisconfig.SampleDevModeSoloProfile),
	)
})
