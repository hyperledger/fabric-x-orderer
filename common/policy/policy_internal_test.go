/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policy

import (
	"testing"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mockSigner "github.com/hyperledger/fabric-x-common/protoutil/identity/mocks"
	"github.com/stretchr/testify/require"
)

func TestCreateSignedConfigEnvelopeWithTxID_NilInputs(t *testing.T) {
	_, err := createSignedConfigEnvelopeWithTxID("mychannel", nil, &cb.ConfigEnvelope{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "signer is nil")

	_, err = createSignedConfigEnvelopeWithTxID("mychannel", &mockSigner.SignerSerializer{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "config envelope is nil")
}
