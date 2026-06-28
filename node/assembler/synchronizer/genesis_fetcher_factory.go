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
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/genesis_fetcher.go . GenesisFetcher
type GenesisFetcher interface {
	GenesisByEndpoints() (map[string]*common.Block, error)
	Close()
}

//go:generate counterfeiter -o mocks/genesis_fetcher_factory.go . GenesisFetcherFactory
type GenesisFetcherFactory interface {
	// CreateGenesisFetcher creates a new genesis fetcher.
	CreateGenesisFetcher(
		myPartyID types.PartyID,
		support AssemblerSupport,
		baseDialer *comm.PredicateDialer,
		clusterConfig config.Cluster,
		bccsp bccsp.BCCSP,
		logger *flogging.FabricLogger,
	) (GenesisFetcher, error)
}

type GenesisFetcherCreator struct{}

func (*GenesisFetcherCreator) CreateGenesisFetcher(
	myPartyID types.PartyID,
	support AssemblerSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (GenesisFetcher, error) {
	return newBlockPuller(myPartyID, support, baseDialer, clusterConfig, bccsp, logger)
}

// newBlockPuller creates a new block puller, which is used for target height detection.
func newBlockPuller(
	myPartyID types.PartyID,
	support AssemblerSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (GenesisFetcher, error) {
	// TODO replace this with the actual implementation
	verifyBlockSequenceNoOp := func(blocks []*common.Block, _ string) error {
		// TODO
		return nil
	}

	stdDialer := &comm.StandardDialer{
		Config: baseDialer.Config.Clone(),
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	// Extract endpoint and TLS cert from the config, excluding the self endpoint.
	endpoints, err := extractEndpointCriteriaFromConfig(myPartyID, support)
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
// It retrieves all consenter addresses from the shared config and converts them into EndpointCriteria,
// excluding the endpoint corresponding to myPartyID to avoid self-connection.
// Returns a slice of EndpointCriteria containing the endpoint address and TLS root certificates for each consenter.
func extractEndpointCriteriaFromConfig(myPartyID types.PartyID, support AssemblerSupport) ([]comm.EndpointCriteria, error) {
	party2endpoint, err := config.ExtractAssemblerAddresses(support.SharedConfig())
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
