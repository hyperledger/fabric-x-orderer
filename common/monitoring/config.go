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
	// ListenAddress is the host and port for the operations server to listen on. It should be in the format of "host:port".
	ListenAddress string
	TLS           localconfig.TLS
}

// Metrics configures the metrics provider for the orderer.
type Metrics struct {
	Provider           string
	MetricsLogInterval time.Duration
}
