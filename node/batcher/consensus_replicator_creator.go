package batcher

import (
	"arma/common/types"
	node_config "arma/node/config"
	"arma/node/delivery"
)

func CreateConsensusReplicator(config node_config.BatcherNodeConfig, logger types.Logger) *delivery.ConsensusReplicator {
	var endpoint string
	var tlsCAs []node_config.RawBytes
	for i := 0; i < len(config.Consenters); i++ {
		consenter := config.Consenters[i]
		if consenter.PartyID == config.PartyId {
			endpoint = consenter.Endpoint
			tlsCAs = consenter.TLSCACerts
		}
	}

	if endpoint == "" || len(tlsCAs) == 0 {
		logger.Panicf("Failed finding endpoint and TLS CAs for party %d", config.PartyId)
	}
	return delivery.NewConsensusReplicator(tlsCAs, config.TLSPrivateKeyFile, config.TLSCertificateFile, endpoint, logger)
}
