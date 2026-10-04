/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Build the nodes that connect to a service with each constructor.
//  2. Expect each to print as the set of nodes it names.
func TestConnectingNodesString(t *testing.T) {
	tests := []struct {
		nodes deliver.ConnectingNodes
		want  string
	}{
		{nodes: deliver.EveryRouter(), want: "every router"},
		{nodes: deliver.RouterOfParty(2), want: "the router of party 2"},
		{nodes: deliver.EveryBatcher(), want: "every batcher"},
		{nodes: deliver.BatchersOfShard(3), want: "every batcher of shard 3"},
		{nodes: deliver.BatcherOfParty(2, 3), want: "the batcher of party 2 in shard 3"},
		{nodes: deliver.EveryConsenter(), want: "every consenter"},
		{nodes: deliver.ConsenterOfParty(2), want: "the consenter of party 2"},
		{nodes: deliver.EveryAssembler(), want: "every assembler"},
		{nodes: deliver.AssemblerOfParty(2), want: "the assembler of party 2"},
	}

	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			require.Equal(t, test.want, test.nodes.String())
		})
	}
}
