/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pebbledb

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/pkg/errors"
)

// FormatMajorVersion is the on-disk format every Pebble database of the orderer is created at. Pebble
// upgrades a database to a higher configured format on open and never downgrades it, so raising this
// value makes the data unreadable to binaries built with an older Pebble.
const FormatMajorVersion = pebble.FormatValueSeparation

// Open opens the Pebble database at path, creating it if it does not exist. A nil opts uses the
// defaults. It sets the format and, when opts has none and logger is not nil, the logger on a copy of
// opts, so every database is created at the same format.
func Open(path string, opts *pebble.Options, logger *flogging.FabricLogger) (*pebble.DB, error) {
	opts = opts.Clone()
	opts.FormatMajorVersion = FormatMajorVersion
	if opts.Logger == nil && logger != nil {
		opts.Logger = panicOnFatal{logger}
	}
	db, err := pebble.Open(path, opts)
	return db, errors.Wrapf(err, "failed opening pebble database at %s", path)
}

// panicOnFatal is a Pebble logger that panics on a fatal error instead of exiting the process, as the
// FabricLogger would, so that a failure Pebble cannot recover from fails the node the way other storage
// errors do.
type panicOnFatal struct {
	*flogging.FabricLogger
}

func (l panicOnFatal) Fatalf(format string, args ...any) {
	l.Panicf(format, args...)
}
