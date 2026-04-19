/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider

import (
	"math"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-x-common/common/channelconfig"
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

func extractOrdererConfig(channelID string, channelConfig *cb.Config, cryptoProvider bccsp.BCCSP) (channelconfig.Orderer, error) {
	bundle, err := channelconfig.NewBundle(channelID, channelConfig, cryptoProvider)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel config bundle")
	}

	ordConf, ok := bundle.OrdererConfig()
	if !ok {
		return nil, errors.Errorf("orderer config section not found in channel config")
	}

	return ordConf, nil
}
