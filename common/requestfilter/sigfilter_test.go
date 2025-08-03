/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSigVerifyFilter(t *testing.T) {
	var v requestfilter.RulesVerifier

	v.AddRule(requestfilter.NewSigFilter(&mocks.FakeFilterConfig{}))
	err := v.Verify(nil)
	require.EqualError(t, err, "failed to convert request to signedData : nil request")

	req := &comm.Request{}
	err = v.Verify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing header in request's payload")

	payload := &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: nil}}
	p, err := proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	err = v.Verify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing signature header in payload's header")

	sigheader, err := proto.Marshal(&common.SignatureHeader{
		Creator: []byte("user"),
		Nonce:   []byte("nonce"),
	})
	require.NoError(t, err)

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	err = v.Verify(req)
	require.NoError(t, err)
}
