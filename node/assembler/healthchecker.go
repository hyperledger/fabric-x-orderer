/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"context"
)

type simpleAssemblerHealthChecker struct{}

func (*simpleAssemblerHealthChecker) HealthCheck(_ context.Context) error {
	return nil
}

func RegisterHealthCheckers(assembler *Assembler) {
	if assembler.opsSystem == nil {
		return
	}

	assembler.opsSystem.RegisterChecker("assembler", &simpleAssemblerHealthChecker{})
}
