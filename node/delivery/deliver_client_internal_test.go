/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"io"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider/fake"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Scenario:
//  1. Let the stream return a block with data but no header, and then an error.
//  2. Pull blocks from it, and expect the stream to be closed without the block handed over.
func TestPullBlocksDropsABlockWithNoHeader(t *testing.T) {
	stream := &fake.DeliverClient{}
	block := &common.Block{Data: &common.BlockData{Data: [][]byte{{1}}}}
	stream.RecvReturnsOnCall(0, &orderer.DeliverResponse{Type: &orderer.DeliverResponse_Block{Block: block}}, nil)
	stream.RecvReturnsOnCall(1, nil, io.EOF)
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	handled := 0
	pullBlocks("test", flogging.MustGetLogger("test"), stream, "unused", conn, func(*common.Block) { handled++ }, func() {})

	require.Zero(t, handled)
	require.Equal(t, 1, stream.RecvCallCount())
	require.Equal(t, 1, stream.CloseSendCallCount())
}
