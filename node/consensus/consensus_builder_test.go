/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/stretchr/testify/require"
)

// TestInitialStateFromConfigParties verifies that the initial state lists the parties sorted by party
// ID, the order batchers pick their primary in, whatever order the config lists the consenters in.
func TestInitialStateFromConfigParties(t *testing.T) {
	st := initialStateFromConfig(&node_config.ConsenterNodeConfig{
		Consenters: []node_config.ConsenterInfo{{PartyID: 3}, {PartyID: 1}, {PartyID: 4}, {PartyID: 2}},
	})

	require.Equal(t, uint16(4), st.N)
	require.Equal(t, []types.PartyID{1, 2, 3, 4}, st.Parties)
}
