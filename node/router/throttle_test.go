/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hyperledger/fabric-x-orderer/config"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
)

func TestNewThrottler_DisabledPolicies(t *testing.T) {
	for _, policy := range []string{"", config.ThrottlingPolicyDisabled} {
		th, err := newThrottler(nodeconfig.RouterThrottlingConfig{Policy: policy})
		require.NoError(t, err)
		require.NotNil(t, th)
		require.Nil(t, th.global, "disabled policy installs no limiter")
		for i := 0; i < 1000; i++ {
			require.True(t, th.Allow(), "disabled policy always admits")
		}
	}
}

func TestNewThrottler_GlobalPolicy(t *testing.T) {
	const rate, burst = 1000, 10
	th, err := newThrottler(nodeconfig.RouterThrottlingConfig{Policy: config.ThrottlingPolicyGlobal, Rate: rate, Burst: burst})
	require.NoError(t, err)
	require.NotNil(t, th.global)

	// At a single instant, at most the burst is admitted, then requests are rejected.
	admitted := 0
	for i := 0; i < burst*4; i++ {
		if th.Allow() {
			admitted++
		}
	}
	require.Equal(t, burst, admitted)
	require.False(t, th.Allow())
}

func TestNewThrottler_GlobalPolicyZeroRateErrors(t *testing.T) {
	for _, rate := range []int{0, -1} {
		th, err := newThrottler(nodeconfig.RouterThrottlingConfig{Policy: config.ThrottlingPolicyGlobal, Rate: rate})
		require.Error(t, err, "global policy with non-positive rate is a misconfiguration")
		require.Nil(t, th)
	}
}

func TestNewThrottler_UnknownPolicy(t *testing.T) {
	th, err := newThrottler(nodeconfig.RouterThrottlingConfig{Policy: "bogus"})
	require.Error(t, err)
	require.Nil(t, th)
}
