/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package blockledger offers the exact same interfaces as the `hyperledger/fabric/common/ledger/blockledger`, only
// more performant. Interface aliasing (type interface-A = interface-B) is used so that a user of the `fabric` package
// would be able to use this package and vice-versa.
package blockledger

import (
	fabric_blockledger "github.com/hyperledger/fabric/common/ledger/blockledger"
)

// Factory retrieves or creates new ledgers by channelID.
type Factory = fabric_blockledger.Factory

//go:generate counterfeiter -o mock/block_iterator.go -fake-name BlockIterator . Iterator

// Iterator is useful for a chain Reader to stream blocks as they are created.
type Iterator = fabric_blockledger.Iterator

//go:generate counterfeiter -o mock/block_reader.go -fake-name BlockReader . Reader

// Reader allows the caller to inspect the ledger.
type Reader = fabric_blockledger.Reader

// Writer allows the caller to modify the ledger.
type Writer = fabric_blockledger.Writer

// ReadWriter encapsulates the read/write functions of the ledger.
type ReadWriter = fabric_blockledger.ReadWriter
