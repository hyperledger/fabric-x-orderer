/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tlsgen"
	"github.com/stretchr/testify/require"
)

// BenchmarkSigFilter measures the structure check of a data request with client signature verification off,
// where the TxID check is the only per-request cryptography. The creator carries a full certificate.
func BenchmarkSigFilter(b *testing.B) {
	ca, err := tlsgen.NewCA()
	require.NoError(b, err)
	keyPair, err := ca.NewClientCertKeyPair()
	require.NoError(b, err)
	creator, err := msp.NewSerializedIdentity("org1", keyPair.Cert)
	require.NoError(b, err)

	nonce := protoutil.CreateNonceOrPanic()
	chdr := protoutil.MakeChannelHeader(common.HeaderType_MESSAGE, 0, "arma", 0)
	chdr.TxId = protoutil.ComputeTxID(nonce, creator)
	payload := protoutil.MarshalOrPanic(&common.Payload{
		Header: protoutil.MakePayloadHeader(chdr, protoutil.MakeSignatureHeader(creator, nonce)),
		Data:   make([]byte, 300),
	})
	req := &comm.Request{Payload: payload, Signature: make([]byte, 72)}

	fc := &mocks.FakeFilterConfig{}
	fc.GetClientSignatureVerificationRequiredReturns(false)
	var v requestfilter.RulesVerifier
	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := v.VerifyStructureAndClassify(req); err != nil {
			b.Fatal(err)
		}
	}
}
