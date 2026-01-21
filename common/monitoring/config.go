/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"time"

	"github.com/hyperledger/fabric/orderer/common/localconfig"
)

// Operations configures the operations endpoint for the orderer.
type Operations struct {
	ListenAddress string
	TLS           localconfig.TLS
}

// Metrics configures the metrics provider for the orderer.
type Metrics struct {
	Provider           string
	MetricsLogInterval time.Duration
}
