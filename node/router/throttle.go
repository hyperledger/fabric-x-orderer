/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-x-orderer/common/ratelimit"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
)

// Throttling policy names. These are the values accepted by
// RouterThrottlingConfig.Policy (mirrored by the ThrottlingPolicy* constants in
// the config package).
const (
	ThrottlingDisabled = "disabled" // no throttling (default)
	ThrottlingGlobal   = "global"   // one aggregate rate limit across all clients
	// Future: ThrottlingPerClient, ThrottlingPerOrg (issue #349, Goal 2).
)

// RateLimiter admits or rejects a single request. Implementations must be safe
// for concurrent use. It is the seam for swapping the limiter implementation
// (GCRA today; a striped or x/time/rate-backed variant later) behind a policy.
type RateLimiter interface {
	// Allow reports whether one request may proceed now, without blocking.
	Allow() bool
}

// throttler is the router's per-policy admission container. It is always
// non-nil; a disabled configuration is a throttler with all limiters nil, so
// Allow() is a couple of cheap nil checks with no lock and no interface dispatch.
// New policies (per-client/per-org) add fields here and a check in Allow().
type throttler struct {
	global RateLimiter // nil unless the policy is "global"
}

// Allow is the single hot-path admission entry. It returns true when the request
// may proceed. A per-client/per-org policy will need the request identity; when
// that lands, Allow gains an argument and its call sites change together — the
// config schema, fixed now, does not.
func (t *throttler) Allow() bool {
	if t.global != nil && !t.global.Allow() {
		return false
	}
	return true
}

// newThrottler builds a throttler from the runtime config, dispatching on the
// policy. Unknown policies are rejected so a misconfiguration fails fast at
// startup (and on reconfig).
func newThrottler(cfg nodeconfig.RouterThrottlingConfig) (*throttler, error) {
	switch cfg.Policy {
	case "", ThrottlingDisabled:
		return &throttler{}, nil
	case ThrottlingGlobal:
		// ratelimit.New returns nil for Rate <= 0. Leave global as a nil interface
		// (not a non-nil interface wrapping a nil pointer) so Allow() short-circuits;
		// this yields an effectively-disabled container for a "global" policy with no rate.
		lim := ratelimit.New(float64(cfg.Rate), cfg.Burst)
		if lim == nil {
			return &throttler{}, nil
		}
		return &throttler{global: lim}, nil
	default:
		return nil, errors.Errorf("unknown router throttling policy %q", cfg.Policy)
	}
}
