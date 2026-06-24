/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
)

type simpleBatcherHealthChecker struct{}

func (*simpleBatcherHealthChecker) HealthCheck(_ context.Context) error {
	return nil
}

func RegisterHealthCheckers(batcher *Batcher) {
	if batcher.opsSystem == nil {
		return
	}

	batcher.opsSystem.RegisterChecker("batcher", &simpleBatcherHealthChecker{})
}
