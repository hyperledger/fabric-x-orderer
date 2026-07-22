/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"crypto/x509"
	"encoding/pem"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/pkg/errors"
)

// AddressExtractor extracts the party-to-endpoint mapping from the shared orderer
// configuration. The assembler uses config.ExtractAssemblerAddresses and the
// consensus node uses config.ExtractConsenterAddresses.
type AddressExtractor func(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error)

// NewBlockPuller creates a new block puller, which is used for target height detection
// and genesis block fetching. The set of remote endpoints is derived from the shared
// config via the given AddressExtractor, excluding the self endpoint.
func NewBlockPuller(
	myPartyID types.PartyID,
	support Support,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
	extractAddresses AddressExtractor,
) (*comm.BlockPuller, error) {
	// TODO replace this with the actual implementation
	verifyBlockSequenceNoOp := func(blocks []*cb.Block, _ string) error {
		// TODO
		return nil
	}

	stdDialer := &comm.StandardDialer{
		Config: baseDialer.Config.Clone(),
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	// Extract endpoint and TLS cert from the config, excluding the self endpoint.
	endpoints, err := extractEndpointCriteriaFromConfig(myPartyID, support, extractAddresses)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract endpoint criteria from config")
	}

	der, _ := pem.Decode(stdDialer.Config.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(stdDialer.Config.SecOpts.Certificate))
	}

	myCert, err := x509.ParseCertificate(der.Bytes)
	if err != nil {
		logger.Warnf("Failed parsing my own TLS certificate: %v, therefore we may connect to our own endpoint when pulling blocks", err)
	}

	// TODO Fabric defaults. Extend the config to have these values, and use the config values instead of hardcoding them here.
	// Cluster: Cluster{
	// 	ReplicationMaxRetries:          12,
	// 	RPCTimeout:                     time.Second * 7,
	// 	DialTimeout:                    time.Second * 5,
	// 	ReplicationBufferSize:          20971520,
	// 	SendBufferSize:                 100,
	// 	ReplicationRetryTimeout:        time.Second * 5,
	// 	ReplicationPullTimeout:         time.Second * 5,
	// 	CertExpirationWarningThreshold: time.Hour * 24 * 7,
	// 	ReplicationPolicy:              "consensus", // BFT default; on etcdraft it is ignored
	// },

	bp := &comm.BlockPuller{
		MyOwnTLSCert:        myCert,
		VerifyBlockSequence: verifyBlockSequenceNoOp,
		Logger:              logger,
		RetryTimeout:        time.Second * 5,  // clusterConfig.ReplicationRetryTimeout,
		MaxTotalBufferBytes: 20 * 1024 * 1024, // clusterConfig.ReplicationBufferSize,
		FetchTimeout:        time.Second * 5,  // clusterConfig.ReplicationPullTimeout,
		Endpoints:           endpoints,        // TODO the block puller is not party aware yet
		Signer:              support,
		TLSCert:             der.Bytes,
		Channel:             support.ChannelID(),
		Dialer:              stdDialer,
	}

	logger.Infof("Built new block puller (target height detector) with endpoints: %+v", endpoints)

	return bp, nil
}

// extractEndpointCriteriaFromConfig extracts endpoint criteria from the channel configuration.
// It retrieves all party addresses from the shared config (via the given AddressExtractor) and
// converts them into EndpointCriteria, excluding the endpoint corresponding to myPartyID to avoid
// self-connection.
func extractEndpointCriteriaFromConfig(myPartyID types.PartyID, support Support, extractAddresses AddressExtractor) ([]comm.EndpointCriteria, error) {
	party2endpoint, err := extractAddresses(support.SharedConfig())
	if err != nil {
		return nil, err
	}

	var endpoints []comm.EndpointCriteria
	for party, ep := range party2endpoint {
		if party == myPartyID {
			continue
		}
		endpointCriteria := &comm.EndpointCriteria{
			Endpoint:   ep.Address,
			TLSRootCAs: ep.RootCerts,
		}
		endpoints = append(endpoints, *endpointCriteria)
	}

	return endpoints, nil
}
