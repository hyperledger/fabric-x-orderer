/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/stretchr/testify/require"
)

func TestNewBFTCensorshipMonitorFactory(t *testing.T) {
	s := newMonitorTestSetup(t, 5)
	f := &blocksprovider.BFTCensorshipMonitorFactory{}
	mon := f.Create(s.channelID, s.fakeUpdatableBlockVerifier, s.fakeRequester, s.fakeProgressReporter, s.sources, 0, blocksprovider.TimeoutConfig{}, &utils.CommonBlockOperations{})
	require.NotNil(t, mon)
}
