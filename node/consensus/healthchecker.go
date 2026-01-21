/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"context"
)

type simpleConsensusHealthChecker struct{}

func (*simpleConsensusHealthChecker) HealthCheck(_ context.Context) error {
	return nil
}

func RegisterHealthCheckers(consenter *Consensus) {
	if consenter.opsSystem == nil {
		return
	}

	consenter.opsSystem.RegisterChecker("consenter", &simpleConsensusHealthChecker{})
}
