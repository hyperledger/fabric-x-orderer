/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider

import (
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	orderer_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
)

type errRefreshEndpoint struct {
	message string
}

func (e *errRefreshEndpoint) Error() string {
	return e.message
}

type ErrStopping struct {
	Message string
}

func (e *ErrStopping) Error() string {
	return e.Message
}

type ErrFatal struct {
	Message string
}

func (e *ErrFatal) Error() string {
	return e.Message
}

type ErrCensorship struct {
	Message string
}

func (e *ErrCensorship) Error() string {
	return e.Message
}

func backOffDuration(base float64, exponent uint, minDur, maxDur time.Duration) time.Duration {
	if base < 1.0 {
		base = 1.0
	}
	if minDur <= 0 {
		minDur = BftMinRetryInterval
	}
	if maxDur < minDur {
		maxDur = minDur
	}

	fDurNano := math.Pow(base, float64(exponent)) * float64(minDur.Nanoseconds())
	fDurNano = math.Min(fDurNano, float64(maxDur.Nanoseconds()))
	return time.Duration(fDurNano)
}

// How many retries n does it take for minDur to reach maxDur, if minDur is scaled exponentially with base^i
//
// minDur * base^n > maxDur
// base^n > maxDur / minDur
// n * log(base) > log(maxDur / minDur)
// n > log(maxDur / minDur) / log(base)
func numRetries2Max(base float64, minDur, maxDur time.Duration) int {
	if base <= 1.0 {
		base = 1.001
	}
	if minDur <= 0 {
		minDur = BftMinRetryInterval
	}
	if maxDur < minDur {
		maxDur = minDur
	}

	return int(math.Ceil(math.Log(float64(maxDur)/float64(minDur)) / math.Log(base)))
}

type timeNumber struct {
	t time.Time
	n uint64
}

func extractAddresses(channelID string, config *cb.Config, cryptoProvider bccsp.BCCSP) (map[string]orderers.OrdererOrg, error) {
	bundle, err := channelconfig.NewBundle(channelID, config, cryptoProvider)
	if err != nil {
		return nil, err
	}

	orgAddresses := map[string]orderers.OrdererOrg{}
	if ordererConfig, ok := bundle.OrdererConfig(); ok {
		for orgName, org := range ordererConfig.Organizations() {
			var certs [][]byte
			certs = append(certs, org.MSP().GetTLSRootCerts()...)
			certs = append(certs, org.MSP().GetTLSIntermediateCerts()...)

			orgAddresses[orgName] = orderers.OrdererOrg{
				Addresses: org.Endpoints(),
				RootCerts: certs,
			}
		}
	}

	return orgAddresses, nil
}

func extractConsenterAddresses(channelID string, config *cb.Config, cryptoProvider bccsp.BCCSP) (orderers.Party2Endpoint, error) {
	bundle, err := channelconfig.NewBundle(channelID, config, cryptoProvider)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel config bundle")
	}

	ordConf, ok := bundle.OrdererConfig()
	if !ok {
		return nil, errors.Errorf("orderer config section not found in channel config")
	}
	consensusMeta := ordConf.ConsensusMetadata()
	sharedConfig := &protos.SharedConfig{}
	if err := proto.Unmarshal(consensusMeta, sharedConfig); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	conf := &orderer_config.Configuration{
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
