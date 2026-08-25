/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package blockledger offers the exact same interfaces as the `hyperledger/fabric/common/ledger/blockledger`, only
// more performant.
package blockledger

import (
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

// Factory retrieves or creates new ledgers by channelID
type Factory interface {
	// GetOrCreate gets an existing ledger (if it exists)
	// or creates it if it does not
	GetOrCreate(channelID string) (ReadWriter, error)

	// Remove removes an existing ledger
	Remove(channelID string) error

	// ChannelIDs returns the channel IDs the Factory is aware of
	ChannelIDs() []string

	// Close releases all resources acquired by the factory
	Close()
}

//go:generate counterfeiter -o mock/block_iterator.go -fake-name BlockIterator . Iterator

// Iterator is useful for a chain Reader to stream blocks as they are created
type Iterator interface {
	// Next blocks until there is a new block available, or returns an error if
	// the next block is no longer retrievable
	Next() (*cb.Block, cb.Status)
	// Close releases resources acquired by the Iterator
	Close()
}

//go:generate counterfeiter -o mock/block_reader.go -fake-name BlockReader . Reader

// Reader allows the caller to inspect the ledger
type Reader interface {
	// Iterator returns an Iterator, as specified by an ab.SeekPosition message, and
	// its starting block number
	Iterator(startType *ab.SeekPosition) (Iterator, uint64)
	// Height returns the number of blocks on the ledger
	Height() uint64
	// retrieve blockByNumber
	RetrieveBlockByNumber(blockNumber uint64) (*cb.Block, error)
}

// Writer allows the caller to modify the ledger
type Writer interface {
	// Append a new block to the ledger
	Append(block *cb.Block) error
	// PruneBefore prunes the blocks below seq. It is an upper bound rather than an exact point: storage
	// removes whole files only, so it may keep more than asked, but never less. Reads below seq
	// fail afterwards whether or not their bytes were removed. Height is unaffected, and the call is
	// idempotent and monotone: a seq at or below the current bound changes nothing.
	PruneBefore(seq uint64) error
}

// ReadWriter encapsulates the read/write functions of the ledger
type ReadWriter interface {
	Reader
	Writer
}
