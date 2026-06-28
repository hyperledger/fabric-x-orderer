/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"sort"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	orderer_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
)

func partiesFromAssemblerConfig(config *node_config.AssemblerNodeConfig) []types.PartyID {
	var parties []types.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, types.PartyID(b.PartyID))
	}

	sort.Slice(parties, func(i, j int) bool {
		return int(parties[i]) < int(parties[j])
	})

	return parties
}

func shardsFromAssemblerConfig(config *node_config.AssemblerNodeConfig) []types.ShardID {
	shardIds := make([]types.ShardID, len(config.Shards))
	for i, shard := range config.Shards {
		shardIds[i] = shard.ShardId
	}

	sort.Slice(shardIds, func(i, j int) bool {
		return int(shardIds[i]) < int(shardIds[j])
	})

	return shardIds
}

// TODO: use stringer/formatter/gostringer for more general solution
func BatchToString(batchID types.BatchID) string {
	return types.BatchIDToString(batchID)
}

func batchSizeBytes(batch types.Batch) int {
	size := 0
	for _, req := range batch.Requests() {
		size += len(req)
	}
	return size
}

func extractClientConfigFromAssemblerConfig(assemblerNodeConfig *node_config.AssemblerNodeConfig, configuration *orderer_config.Configuration) comm.ClientConfig {
	var tlsCAs [][]byte
	assemblers := configuration.ExtractAssemblers()
	for _, assemblerInfo := range assemblers {
		for _, tlsCACert := range assemblerInfo.TLSCACerts {
			tlsCAs = append(tlsCAs, tlsCACert)
		}
	}

	cert := assemblerNodeConfig.TLSCertificateFile

	tlsKey := assemblerNodeConfig.TLSPrivateKeyFile

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               tlsKey,
			Certificate:       cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
		BaOpts:      comm.DefaultBackoffOptions,
	}
	return cc
}
