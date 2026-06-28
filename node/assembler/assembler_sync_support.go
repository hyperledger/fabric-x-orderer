/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
)

// AssemblerSupportAdapter adapts an Assembler to satisfy the
// synchronizer.ConsenterSupport interface, which combines identity.SignerSerializer
// (Sign + Serialize) with the channel-support methods required by the BFT synchronizer.
type AssemblerSupportAdapter struct {
	assembler *Assembler
}

// Sign signs the given message bytes and returns the signature.
// Implements identity.SignerSerializer (identity.Signer).
func (a *AssemblerSupportAdapter) Sign(message []byte) ([]byte, error) {
	return a.assembler.signer.Sign(message)
}

// Serialize converts the local identity to bytes so it can be embedded in
// a signature header.
// Implements identity.SignerSerializer (identity.Serializer).
func (a *AssemblerSupportAdapter) Serialize() ([]byte, error) {
	return a.assembler.signer.Serialize()
}

// Height returns the current ledger height (number of committed blocks).
func (a *AssemblerSupportAdapter) Height() uint64 {
	return a.assembler.ledger.LedgerReader().Height()
}

// ChannelID returns the identifier of the channel this adapter is associated
// with.
func (a *AssemblerSupportAdapter) ChannelID() string {
	return a.assembler.assemblerNodeConfig.Bundle.ConfigtxValidator().ChannelID()
}

// SharedConfig returns the orderer section of the channel's current config
// bundle.
func (a *AssemblerSupportAdapter) SharedConfig() channelconfig.Orderer {
	conf, ok := a.assembler.assemblerNodeConfig.Bundle.OrdererConfig()
	if !ok {
		a.assembler.logger.Panic("orderer config not found in bundle")
		return nil
	}
	return conf
}

// WriteBlockSync commits a regular (non-config) block to the ledger, blocking until the write is complete.
func (a *AssemblerSupportAdapter) WriteBlockSync(block *common.Block) {
	a.assembler.ledger.AppendBlock(block)
}

// WriteConfigBlock commits a config block to the ledger and applies the configuration update contained within it.
func (a *AssemblerSupportAdapter) WriteConfigBlock(block *common.Block) {
	a.assembler.ledger.AppendBlock(block)
}

// Block returns the block with the given number from the ledger,
// or nil if no such block exists.
func (a *AssemblerSupportAdapter) Block(number uint64) *common.Block {
	block, err := a.assembler.ledger.LedgerReader().RetrieveBlockByNumber(number)
	if err != nil {
		a.assembler.logger.Errorf("Failed to retrieve block %d: %v", number, err)
		return nil
	}
	return block
}

// ClientConfig returns the TLS client configuration for the assembler, used by
// the synchronizer to dial other assembler nodes.
func (a *AssemblerSupportAdapter) ClientConfig() comm.ClientConfig {
	return extractClientConfigFromAssemblerConfig(a.assembler.assemblerNodeConfig, a.assembler.configuration)
}

// LastConfigBlock returns the most recent config block at or before the given block,
// or an error if it cannot be retrieved.
func (a *AssemblerSupportAdapter) LastConfigBlock(block *common.Block) (*common.Block, error) {
	lastConfigIndex, err := protoutil.GetLastConfigIndexFromBlock(block)
	if err != nil {
		return nil, fmt.Errorf("failed to get last config index from assembler's last block: %s", err)
	}
	lastConfigBlock, err := a.assembler.ledger.LedgerReader().RetrieveBlockByNumber(lastConfigIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get last config block: %s", err)
	}
	return lastConfigBlock, nil
}
