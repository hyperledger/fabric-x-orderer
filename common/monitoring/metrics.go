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

// IntervalAverage returns the average of what was added during one interval: how much a sum grew
// between the start and the end of the interval, divided by how much a count grew in the same time.
// It returns 0 when the count did not grow, i.e. nothing was added during the interval.
func IntervalAverage(sumAtStart, sumAtEnd, countAtStart, countAtEnd uint64) uint64 {
	if countAtEnd <= countAtStart {
		return 0
	}
	return (sumAtEnd - sumAtStart) / (countAtEnd - countAtStart)
}
