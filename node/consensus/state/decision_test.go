/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestProposalToBytes(t *testing.T) {
	t.Run("valid proposal", func(t *testing.T) {
		proposal := smartbft_types.Proposal{
			Header:   []byte{1, 2, 3},
			Payload:  []byte{4, 5, 6},
			Metadata: []byte{7, 8, 9},
		}
		bytes := ProposalToBytes(proposal)
		assert.NotNil(t, bytes)
		assert.NotEmpty(t, bytes)
	})

	t.Run("empty proposal", func(t *testing.T) {
		proposal := smartbft_types.Proposal{}
		bytes := ProposalToBytes(proposal)
		assert.NotNil(t, bytes)
	})

	t.Run("proposal with nil fields", func(t *testing.T) {
		proposal := smartbft_types.Proposal{
			Header:   nil,
			Payload:  []byte{1, 2, 3},
			Metadata: nil,
		}
		bytes := ProposalToBytes(proposal)
		assert.NotNil(t, bytes)
	})
}

func TestBytesToProposal(t *testing.T) {
	t.Run("valid bytes", func(t *testing.T) {
		original := smartbft_types.Proposal{
			Header:   []byte{1, 2, 3},
			Payload:  []byte{4, 5, 6},
			Metadata: []byte{7, 8, 9},
		}
		bytes := ProposalToBytes(original)
		proposal, err := BytesToProposal(bytes)
		assert.NoError(t, err)
		assert.Equal(t, original, proposal)
	})

	t.Run("empty bytes", func(t *testing.T) {
		proposal, err := BytesToProposal([]byte{})
		assert.Error(t, err)
		assert.Equal(t, smartbft_types.Proposal{}, proposal)
	})

	t.Run("nil bytes", func(t *testing.T) {
		proposal, err := BytesToProposal(nil)
		assert.Error(t, err)
		assert.Equal(t, smartbft_types.Proposal{}, proposal)
	})

	t.Run("invalid bytes", func(t *testing.T) {
		proposal, err := BytesToProposal([]byte{1, 2, 3})
		assert.Error(t, err)
		assert.Equal(t, smartbft_types.Proposal{}, proposal)
	})
}

func TestDecisionSignaturesToBytes(t *testing.T) {
	t.Run("valid signatures", func(t *testing.T) {
		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: []byte{1, 2, 3},
				Msg:   []byte{4, 5, 6},
			},
			{
				ID:    2,
				Value: []byte{7, 8, 9},
				Msg:   []byte{10, 11, 12},
			},
		}
		bytes := DecisionSignaturesToBytes(signatures)
		assert.NotNil(t, bytes)
		assert.NotEmpty(t, bytes)
	})

	t.Run("empty signatures", func(t *testing.T) {
		signatures := []smartbft_types.Signature{}
		bytes := DecisionSignaturesToBytes(signatures)
		assert.NotNil(t, bytes)
	})

	t.Run("nil signatures", func(t *testing.T) {
		bytes := DecisionSignaturesToBytes(nil)
		assert.NotNil(t, bytes)
	})

	t.Run("signatures with nil fields", func(t *testing.T) {
		signatures := []smartbft_types.Signature{
			{
				ID:    1,
				Value: nil,
				Msg:   []byte{1, 2, 3},
			},
		}
		bytes := DecisionSignaturesToBytes(signatures)
		assert.NotNil(t, bytes)
	})
}

func TestBytesToDecisionSignatures(t *testing.T) {
	t.Run("valid bytes", func(t *testing.T) {
		original := []smartbft_types.Signature{
			{
				ID:    1,
				Value: []byte{1, 2, 3},
				Msg:   []byte{4, 5, 6},
			},
			{
				ID:    2,
				Value: []byte{7, 8, 9},
				Msg:   []byte{10, 11, 12},
			},
		}
		bytes := DecisionSignaturesToBytes(original)
		signatures, err := BytesToDecisionSignatures(bytes)
		assert.NoError(t, err)
		assert.Equal(t, original, signatures)
	})

	t.Run("empty bytes", func(t *testing.T) {
		signatures, err := BytesToDecisionSignatures([]byte{})
		assert.Error(t, err)
		assert.Nil(t, signatures)
	})

	t.Run("nil bytes", func(t *testing.T) {
		signatures, err := BytesToDecisionSignatures(nil)
		assert.Error(t, err)
		assert.Nil(t, signatures)
	})

	t.Run("invalid bytes", func(t *testing.T) {
		signatures, err := BytesToDecisionSignatures([]byte{1, 2, 3})
		assert.Error(t, err)
		assert.Nil(t, signatures)
	})
}
