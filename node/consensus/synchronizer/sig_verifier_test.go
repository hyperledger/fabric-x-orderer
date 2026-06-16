/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSigVerifierCreator_SigVerifierFromConfig(t *testing.T) {
	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(sw.NewDummyKeyStore())
	require.NoError(t, err)
	logger := flogging.MustGetLogger("test")

	svc := &synchronizer.SigVerifierCreator{
		Logger: logger,
		BCCSP:  cryptoProvider,
	}

	testCases := []struct {
		name                string
		configEnvelope      *common.ConfigEnvelope
		channel             string
		expectedErrContains string
	}{
		{
			name:                "nil configuration",
			configEnvelope:      nil,
			channel:             "",
			expectedErrContains: "nil",
		},
		{
			name:                "missing channel group",
			configEnvelope:      &common.ConfigEnvelope{Config: &common.Config{}},
			channel:             "",
			expectedErrContains: "channel group",
		},
		{
			name:                "nil config",
			configEnvelope:      &common.ConfigEnvelope{Config: nil},
			channel:             "test-channel",
			expectedErrContains: "cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			verifierFunc, err := svc.SigVerifierFromConfig(tc.configEnvelope, tc.channel, 0)
			assert.NotNil(t, verifierFunc)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)

			// Verify the returned function also returns an error
			block := &common.Block{Header: &common.BlockHeader{Number: 1}}
			funcErr := verifierFunc(block, false)
			assert.Error(t, funcErr)
			assert.Contains(t, funcErr.Error(), "failed to initialize sig verifier function")
		})
	}
}

func TestBlockSigVerifier_Verify(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	verifier := &synchronizer.BlockSigVerifier{
		Logger:         logger,
		ConfigBlockNum: 0,
		Consenters:     []*common.Consenter{},
	}

	testCases := []struct {
		name                string
		block               *common.Block
		verifyData          bool
		expectedErrContains string
	}{
		{
			name:                "nil block",
			block:               nil,
			verifyData:          false,
			expectedErrContains: "consenter block is nil",
		},
		{
			name: "nil metadata",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Metadata: nil,
			},
			verifyData:          false,
			expectedErrContains: "consenter block metadata is nil",
		},
		{
			name: "empty metadata",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Metadata: &common.BlockMetadata{Metadata: [][]byte{}},
			},
			verifyData:          false,
			expectedErrContains: "consenter block metadata len is less than expected",
		},
		{
			name: "nil block header",
			block: &common.Block{
				Header:   nil,
				Metadata: &common.BlockMetadata{Metadata: [][]byte{}},
			},
			verifyData:          false,
			expectedErrContains: "consenter block header is nil",
		},
		{
			name: "nil block data",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Data:     nil,
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			verifyData:          true,
			expectedErrContains: "consenter block data is nil or empty",
		},
		{
			name: "empty block data",
			block: &common.Block{
				Header:   &common.BlockHeader{Number: 1},
				Data:     &common.BlockData{Data: [][]byte{}},
				Metadata: &common.BlockMetadata{Metadata: make([][]byte, len(common.BlockMetadataIndex_name))},
			},
			verifyData:          true,
			expectedErrContains: "consenter block data is nil or empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifier.Verify(tc.block, tc.verifyData)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrContains)
		})
	}
}
