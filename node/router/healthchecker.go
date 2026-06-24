/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
)

type simpleRouterHealthChecker struct{}

func (*simpleRouterHealthChecker) HealthCheck(_ context.Context) error {
	return nil
}

func RegisterHealthCheckers(router *Router) {
	if router.opsSystem == nil {
		return
	}

	router.opsSystem.RegisterChecker("router", &simpleRouterHealthChecker{})
}
