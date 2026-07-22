/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
)

// EndpointsExtractor implements blocksprovider.EndpointsExtractor by delegating to a
// node-specific AddressExtractor. The assembler uses config.ExtractAssemblerAddresses
// and the consensus node uses config.ExtractConsenterAddresses.
type EndpointsExtractor struct {
	Extract AddressExtractor
}

// ExtractEndpoints extracts the party-to-endpoint mapping from the given orderer configuration.
func (e *EndpointsExtractor) ExtractEndpoints(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error) {
	return e.Extract(ordererConfig)
}
