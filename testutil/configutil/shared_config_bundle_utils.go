/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configutil

import (
	"testing"

	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// NewBundleOfSharedConfig returns a configuration bundle that carries parties as its shared
// configuration, so that a test can name the nodes of a service without producing a config block.
func NewBundleOfSharedConfig(t *testing.T, parties []*ordererpb.PartyConfig) *mocks.FakeConfigResources {
	t.Helper()

	bundle := &mocks.FakeConfigResources{}
	bundle.OrdererConfigReturns(NewOrdererConfigOfSharedConfig(t, parties), true)

	return bundle
}

// OrdererConfigOfSharedConfig is an orderer configuration that carries a shared configuration as its
// consensus metadata, the way a configuration bundle holds it. It answers nothing else.
type OrdererConfigOfSharedConfig struct {
	channelconfig.Orderer
	metadata []byte
}

// NewOrdererConfigOfSharedConfig returns an orderer configuration carrying parties as its shared
// configuration.
func NewOrdererConfigOfSharedConfig(t *testing.T, parties []*ordererpb.PartyConfig) *OrdererConfigOfSharedConfig {
	t.Helper()

	metadata, err := proto.Marshal(&ordererpb.SharedConfig{PartiesConfig: parties})
	require.NoError(t, err)

	return &OrdererConfigOfSharedConfig{metadata: metadata}
}

func (o *OrdererConfigOfSharedConfig) ConsensusMetadata() []byte {
	return o.metadata
}
