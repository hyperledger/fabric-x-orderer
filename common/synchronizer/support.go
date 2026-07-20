/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/pkg/errors"
)

// Support is the subset of ledger and identity operations shared by the assembler
// and consensus synchronizers. Both node/assembler/synchronizer.AssemblerSupport and
// node/consensus/synchronizer.ConsenterSupport are supersets of this interface, so a
// value of either type satisfies Support.
type Support interface {
	identity.SignerSerializer

	// Height returns the number of blocks in the chain this channel is associated with.
	Height() uint64

	// SharedConfig provides the shared config from the channel's current config block.
	SharedConfig() channelconfig.Orderer

	// ChannelID returns the channel ID this support is associated with.
	ChannelID() string

	// Block returns a block with the given number, or nil if such a block doesn't exist.
	Block(number uint64) *cb.Block

	// LastConfigBlock returns the latest config block before or at the given block,
	// or error if such a block cannot be retrieved.
	LastConfigBlock(block *cb.Block) (*cb.Block, error)

	// WriteBlockSync commits a block to the ledger.
	WriteBlockSync(block *cb.Block)

	// WriteConfigBlock commits a block to the ledger, and applies the config update inside.
	WriteConfigBlock(block *cb.Block)
}

// ledgerInfoAdapter translates from blocksprovider.LedgerInfo into calls to Support.
type ledgerInfoAdapter struct {
	support Support
}

// NewLedgerInfoAdapter returns a blocksprovider.LedgerInfo backed by the given Support.
func NewLedgerInfoAdapter(support Support) blocksprovider.LedgerInfo {
	return &ledgerInfoAdapter{support: support}
}

func (a *ledgerInfoAdapter) LedgerHeight() (uint64, error) {
	return a.support.Height(), nil
}

func (a *ledgerInfoAdapter) GetCurrentBlockHash() ([]byte, error) {
	return nil, errors.New("not implemented: never used in orderer")
}
