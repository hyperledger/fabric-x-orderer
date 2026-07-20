/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// SelectGenesisBlock selects the genesis block agreed upon by a Byzantine quorum from
// the blocks fetched from remote endpoints. It counts occurrences of each distinct block
// (by hash) and returns a block that appears at least F+1 times, tolerating up to F
// Byzantine faults given the cluster size.
func SelectGenesisBlock(logger *flogging.FabricLogger, genesisByEndpoint map[string]*common.Block, clusterSize int) (*common.Block, error) {
	if clusterSize <= 0 {
		return nil, errors.Errorf("invalid cluster size: %d; must be positive", clusterSize)
	}

	// Calculate required matches
	f, requiredMatches, _ := utils.ComputeFTQ(uint16(clusterSize))
	logger.Infof("Cluster size: %d, F: %d, required matches: %d", clusterSize, f, requiredMatches)

	// Count occurrences of each genesis block by hash
	blockCounts := make(map[string]int)
	blockByHash := make(map[string]*common.Block)
	endpointToHash := []string{}
	for endpoint, block := range genesisByEndpoint {
		if block == nil {
			logger.Warnf("Nil genesis block from endpoint: %s", endpoint)
			continue
		}

		blockBytes, err := proto.Marshal(block)
		if err != nil {
			logger.Warnf("Cannot marshal genesis block from endpoint: %s; err: %s", endpoint, err)
			continue
		}

		blockHash := sha256.Sum256(blockBytes)
		blockHashStr := hex.EncodeToString(blockHash[:])

		blockCounts[blockHashStr]++
		blockByHash[blockHashStr] = block
		endpointToHash = append(endpointToHash, fmt.Sprintf("[EP: %s, H: %s]", endpoint, blockHashStr))
	}

	// Find a block that appears at least F+1 times
	for blockHash, count := range blockCounts {
		if count >= int(requiredMatches) {
			genesisBlock := blockByHash[blockHash]
			logger.Infof("Found genesis block with %d matching copies (required: %d)", count, requiredMatches)
			return genesisBlock, nil
		}
	}

	return nil, errors.Errorf("could not find genesis block with at least %d matching copies: %+v", requiredMatches, endpointToHash)
}
