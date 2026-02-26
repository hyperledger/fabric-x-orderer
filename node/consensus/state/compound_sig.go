/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"encoding/asn1"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/pkg/errors"
)

// CompoundSig is a signature that includes 1+numBlocks signatures, by the same party.
// The Value and the Msg fields can be unmarshalled to an array of 1+numBlocks []byte slices, one for each signature.
// The first signature is on the proposal, the rest are on the block headers.
type CompoundSig smartbft_types.Signature

// UnPack extracts the individual signatures from the compound sig.
func (cSig CompoundSig) UnPack() ([]smartbft_types.Signature, error) {
	if len(cSig.Value) == 0 {
		return nil, errors.New("empty signature value")
	}
	var values [][]byte
	if _, err := asn1.Unmarshal(cSig.Value, &values); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal values")
	}
	if len(values) == 0 {
		return nil, errors.New("zero signature values")
	}
	var msgs [][]byte
	if _, err := asn1.Unmarshal(cSig.Msg, &msgs); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal msgs")
	}
	if len(msgs) == 0 {
		return nil, errors.New("zero signature msgs")
	}
	if len(msgs) != len(values) {
		return nil, errors.Errorf("number of msgs (%d) does not match number of values (%d)", len(msgs), len(values))
	}

	var sigs []smartbft_types.Signature
	for i, v := range values {
		sig := smartbft_types.Signature{
			ID:    cSig.ID,
			Value: v,
			Msg:   msgs[i],
		}
		sigs = append(sigs, sig)
	}

	return sigs, nil
}

// UnpackBlockHeaderSigs returns the unpacked block header signature matrix.
// The incoming array includes compound signatures, one for each party.
// Each compound signature includes 1+numBlocks signatures.
// The output 2D array includes for each block, the signatures of Q parties.
// In the first dimension is the block index, second dimensions is partyID. Proposal sig is dropped.
func UnpackBlockHeaderSigs(compoundSigs []smartbft_types.Signature, numBlocks int) ([][]smartbft_types.Signature, error) {
	if len(compoundSigs) == 0 {
		return nil, errors.New("zero compound sigs")
	}

	sigsBnP := make([][]smartbft_types.Signature, numBlocks)
	if numBlocks == 0 {
		return sigsBnP, nil
	}

	for i := 0; i < numBlocks; i++ {
		sigsBnP[i] = make([]smartbft_types.Signature, len(compoundSigs))
	}

	for partyIdx, cSig := range compoundSigs {
		partySigs, err := CompoundSig(cSig).UnPack()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to Unpack compound sig: %+v", cSig)
		}
		if len(partySigs) != numBlocks+1 {
			return nil, errors.Errorf("extracted %d signatures but expected %d, compound sig: %+v", len(partySigs), numBlocks+1, cSig)
		}

		for blockIdx, sig := range partySigs {
			if blockIdx == 0 {
				continue // This is the sig on the proposal, we don't need it
			}

			sigsBnP[blockIdx-1][partyIdx] = sig
		}
	}

	return sigsBnP, nil
}
