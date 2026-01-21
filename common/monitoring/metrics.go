/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
)

var (
	armaVersion = metrics.GaugeOpts{
		Name:         "arma_version",
		Help:         "The active version of Arma.",
		LabelNames:   []string{"version"},
		StatsdFormat: "%{#fqname}.%{version}",
	}

	doOnce       sync.Once
	versionGauge metrics.Gauge
)

func VersionGauge(provider metrics.Provider) metrics.Gauge {
	doOnce.Do(func() {
		versionGauge = provider.NewGauge(armaVersion)
	})
	return versionGauge
}
