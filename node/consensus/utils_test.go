/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/stretchr/testify/assert"
)

func TestBlockToDecision(t *testing.T) {
	t.Run("green path", func(t *testing.T) {
		proposal := smartbft_types.Proposal{
			Header:   []byte{1, 2, 3},
			Payload:  []byte{4, 5, 6},
			Metadata: []byte{7, 8, 9},
		}
		signatures := []smartbft_types.Signature{{ID: 10, Value: []byte{11}, Msg: []byte{12}}, {ID: 13, Value: []byte{14}, Msg: []byte{15}}}

		blockDataBytes := state.DecisionToBytes(proposal, signatures)
		block := &common.Block{
			Data: &common.BlockData{Data: [][]byte{blockDataBytes}},
		}

		decision := consensus.ConsenterBlockToDecision(block)
		assert.NotNil(t, decision)
		assert.Equal(t, proposal, decision.Proposal)
		assert.Equal(t, signatures, decision.Signatures)
	})

	t.Run("nil block", func(t *testing.T) {
		decision := consensus.ConsenterBlockToDecision(nil)
		assert.Nil(t, decision)
	})

	t.Run("nil block data", func(t *testing.T) {
		block := &common.Block{}
		decision := consensus.ConsenterBlockToDecision(block)
		assert.Nil(t, decision)
	})

	t.Run("empty block data", func(t *testing.T) {
		block := &common.Block{Data: &common.BlockData{}}
		decision := consensus.ConsenterBlockToDecision(block)
		assert.Nil(t, decision)
	})

	t.Run("bad block data", func(t *testing.T) {
		block := &common.Block{Data: &common.BlockData{Data: [][]byte{[]byte("bad")}}}
		decision := consensus.ConsenterBlockToDecision(block)
		assert.Nil(t, decision)
	})
}
