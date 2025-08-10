/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
)

func TestAcceptFilter(t *testing.T) {
	var v requestfilter.RulesVerifier
	v.AddRule(requestfilter.AcceptRule{})
	request := &comm.Request{Payload: nil}
	err := v.Verify(request)
	require.NoError(t, err)
}
