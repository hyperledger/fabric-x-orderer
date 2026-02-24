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
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/height_detector.go . HeightDetector

// HeightDetector is used to detect the ledger height of other consenters.
// It is used by the synchronizer in order to establish a target height for pulling blocks.
type HeightDetector interface {
	HeightsByEndpoints() (map[string]uint64, string, error)
	Close()
}

//go:generate counterfeiter -o mocks/height_detector_factory.go . HeightDetectorFactory

type HeightDetectorFactory interface {
	// CreateHeightDetector creates a new height detector.
	CreateHeightDetector(
		myPartyID types.PartyID,
		support ConsenterSupport,
		baseDialer *comm.PredicateDialer,
		clusterConfig config.Cluster,
		bccsp bccsp.BCCSP,
		logger *flogging.FabricLogger,
	) (HeightDetector, error)
}

type HeightDetectorCreator struct{}

func (*HeightDetectorCreator) CreateHeightDetector(
	myPartyID types.PartyID,
	support ConsenterSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (HeightDetector, error) {
	return newBlockPuller(myPartyID, support, baseDialer, clusterConfig, bccsp, logger)
}

// newBlockPuller creates a new block puller, which is used for target height detection.
func newBlockPuller(
	myPartyID types.PartyID,
	support ConsenterSupport,
	baseDialer *comm.PredicateDialer,
	clusterConfig config.Cluster,
	bccsp bccsp.BCCSP,
	logger *flogging.FabricLogger,
) (HeightDetector, error) {
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

	// TODO Extract endpoint and TLS cert from the config.
	endpoints, err := extractEndpointCriteriaFromConfig(myPartyID, support)
	if err != nil {
		return nil, err
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

	logger.Infof("Built new block puller (target height detector) with cluster config: %+v, endpoints: %+v", clusterConfig, endpoints)

	return bp, nil
}

func extractEndpointCriteriaFromConfig(myPartyID types.PartyID, support ConsenterSupport) ([]comm.EndpointCriteria, error) {
	party2endpoint, err := extractConsenterAddresses(support.SharedConfig())
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

func extractConsenterAddresses(ordererConfig channelconfig.Orderer) (orderers.Party2Endpoint, error) {
	consensusMeta := ordererConfig.ConsensusMetadata()
	sharedConfig := &protos.SharedConfig{}
	if err := proto.Unmarshal(consensusMeta, sharedConfig); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	conf := &config.Configuration{
		SharedConfig: sharedConfig,
	}

	cInfo := conf.ExtractConsenters()

	party2Endpoint := make(orderers.Party2Endpoint)

	for _, consenter := range cInfo {
		party2Endpoint[consenter.PartyID] = &orderers.Endpoint{
			Address: consenter.Endpoint,
		}
		for _, cert := range consenter.TLSCACerts {
			party2Endpoint[consenter.PartyID].RootCerts = append(party2Endpoint[consenter.PartyID].RootCerts, cert)
		}
	}

	return party2Endpoint, nil
}
