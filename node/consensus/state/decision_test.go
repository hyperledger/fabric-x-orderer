package state

import (
	"testing"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestDecisionSerialization(t *testing.T) {
	proposal := types.Proposal{
		Header:   []byte{1, 2, 3},
		Payload:  []byte{4, 5, 6},
		Metadata: []byte{7, 8, 9},
	}
	signatures := []types.Signature{{ID: 10, Value: []byte{11}, Msg: []byte{12}}, {ID: 13, Value: []byte{14}, Msg: []byte{15}}}
	bytes := DecisionToBytes(proposal, signatures)
	proposal2, signatures2, err := BytesToDecision(bytes)
	assert.NoError(t, err)
	assert.Equal(t, proposal, proposal2)
	assert.Equal(t, signatures, signatures2)
}
