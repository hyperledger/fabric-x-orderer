/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"

	"github.com/IBM/idemix/common/flogging"
	"github.com/cockroachdb/errors"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/util"
)

var logger = flogging.MustGetLogger("protoutil")

// NewBlock constructs a block with no data and no metadata.
func NewBlock(seqNum uint64, previousHash []byte) *cb.Block {
	block := &cb.Block{}
	block.Header = &cb.BlockHeader{}
	block.Header.Number = seqNum
	block.Header.PreviousHash = previousHash
	block.Header.DataHash = []byte{}
	block.Data = &cb.BlockData{}

	var metadataContents [][]byte
	for i := 0; i < len(cb.BlockMetadataIndex_name); i++ {
		metadataContents = append(metadataContents, []byte{})
	}
	block.Metadata = &cb.BlockMetadata{Metadata: metadataContents}

	return block
}

type asn1Header struct {
	Number       *big.Int
	PreviousHash []byte
	DataHash     []byte
}

func BlockHeaderBytes(b *cb.BlockHeader) []byte {
	result, err := asn1.Marshal(asn1Header{
		PreviousHash: b.PreviousHash,
		DataHash:     b.DataHash,
		Number:       new(big.Int).SetUint64(b.Number),
	})
	if err != nil {
		// Errors should only arise for types which cannot be encoded, since the
		// BlockHeader type is known a-priori to contain only encodable types, an
		// error here is fatal and should not be propagated.
		panic(err)
	}
	return result
}

func BlockHeaderHash(b *cb.BlockHeader) []byte {
	sum := sha256.Sum256(BlockHeaderBytes(b))
	return sum[:]
}

func BlockDataHash(b *cb.BlockData) ([]byte, error) {
	if err := VerifyTransactionsAreWellFormed(b); err != nil {
		return nil, err
	}
	return ComputeBlockDataHash(b), nil
}

// ComputeBlockDataHash computes a SHA-256 digest of the block data using length-delimited
// serialization. Each data item is prefixed with its 4-byte big-endian length before hashing.
// This prevents hash ambiguity when concatenating variable-length items
// (e.g., [ab, cd] vs [a, bcd] produce the same simple concat but different length-prefixed hashes)
// and matches the orderer's BatchedRequests.Digest() computation.
func ComputeBlockDataHash(b *cb.BlockData) []byte {
	sizeBuff := make([]byte, 4)
	h := sha256.New()
	for _, d := range b.Data {
		//nolint:gosec // block data items are always well within uint32 range
		binary.BigEndian.PutUint32(sizeBuff, uint32(len(d)))
		h.Write(sizeBuff) //nolint:revive // hash.Write never returns an error
		h.Write(d)        //nolint:revive // hash.Write never returns an error
	}
	return h.Sum(nil)
}

// GetChannelIDFromBlockBytes returns channel ID given byte array which represents
// the block.
func GetChannelIDFromBlockBytes(bytes []byte) (string, error) {
	block, err := UnmarshalBlock(bytes)
	if err != nil {
		return "", err
	}

	return GetChannelIDFromBlock(block)
}

// GetChannelIDFromBlock returns channel ID in the block.
func GetChannelIDFromBlock(block *cb.Block) (string, error) {
	if block == nil || block.Data == nil || block.Data.Data == nil || len(block.Data.Data) == 0 {
		return "", errors.New("failed to retrieve channel id - block is empty")
	}
	var err error
	envelope, err := GetEnvelopeFromBlock(block.Data.Data[0])
	if err != nil {
		return "", err
	}
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return "", err
	}

	if payload.Header == nil {
		return "", errors.New("failed to retrieve channel id - payload header is empty")
	}
	chdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return "", err
	}

	return chdr.ChannelId, nil
}

// GetMetadataFromBlock retrieves metadata at the specified index.
func GetMetadataFromBlock(block *cb.Block, index cb.BlockMetadataIndex) (*cb.Metadata, error) {
	if block.Metadata == nil {
		return nil, errors.New("no metadata in block")
	}

	if len(block.Metadata.Metadata) <= int(index) {
		return nil, errors.Errorf("no metadata at index [%s]", index)
	}

	md := &cb.Metadata{}
	err := proto.Unmarshal(block.Metadata.Metadata[index], md)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling metadata at index [%s]", index)
	}
	return md, nil
}

// GetMetadataFromBlockOrPanic retrieves metadata at the specified index, or
// panics on error.
func GetMetadataFromBlockOrPanic(block *cb.Block, index cb.BlockMetadataIndex) *cb.Metadata {
	md, err := GetMetadataFromBlock(block, index)
	if err != nil {
		panic(err)
	}
	return md
}

// GetConsenterMetadataFromBlock attempts to retrieve consenter metadata from the value
// stored in block metadata at index SIGNATURES (first field). If no consenter metadata
// is found there, it falls back to index ORDERER (third field).
func GetConsenterMetadataFromBlock(block *cb.Block) (*cb.Metadata, error) {
	m, err := GetMetadataFromBlock(block, cb.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to retrieve metadata")
	}

	// TODO FAB-15864 Remove this fallback when we can stop supporting upgrade from pre-1.4.1 orderer
	if len(m.Value) == 0 {
		return GetMetadataFromBlock(block, cb.BlockMetadataIndex_ORDERER)
	}

	obm := &cb.OrdererBlockMetadata{}
	err = proto.Unmarshal(m.Value, obm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal orderer block metadata")
	}

	res := &cb.Metadata{}
	err = proto.Unmarshal(obm.ConsenterMetadata, res)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consenter metadata")
	}

	return res, nil
}

// GetLastConfigIndexFromBlock retrieves the index of the last config block as
// encoded in the block metadata.
func GetLastConfigIndexFromBlock(block *cb.Block) (uint64, error) {
	m, err := GetMetadataFromBlock(block, cb.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return 0, errors.WithMessage(err, "failed to retrieve metadata")
	}
	// TODO FAB-15864 Remove this fallback when we can stop supporting upgrade from pre-1.4.1 orderer
	if len(m.Value) == 0 {
		m, err := GetMetadataFromBlock(block, cb.BlockMetadataIndex_LAST_CONFIG)
		if err != nil {
			return 0, errors.WithMessage(err, "failed to retrieve metadata")
		}
		lc := &cb.LastConfig{}
		err = proto.Unmarshal(m.Value, lc)
		if err != nil {
			return 0, errors.Wrap(err, "error unmarshalling LastConfig")
		}
		return lc.Index, nil
	}

	obm := &cb.OrdererBlockMetadata{}
	err = proto.Unmarshal(m.Value, obm)
	if err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal orderer block metadata")
	}
	return obm.LastConfig.Index, nil
}

// GetLastConfigIndexFromBlockOrPanic retrieves the index of the last config
// block as encoded in the block metadata, or panics on error.
func GetLastConfigIndexFromBlockOrPanic(block *cb.Block) uint64 {
	index, err := GetLastConfigIndexFromBlock(block)
	if err != nil {
		panic(err)
	}
	return index
}

// CopyBlockMetadata copies metadata from one block into another.
func CopyBlockMetadata(src, dst *cb.Block) {
	dst.Metadata = src.Metadata
	// Once copied initialize with rest of the
	// required metadata positions.
	InitBlockMetadata(dst)
}

// InitBlockMetadata initializes metadata structure.
func InitBlockMetadata(block *cb.Block) {
	if block.Metadata == nil {
		block.Metadata = &cb.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}}
	} else if len(block.Metadata.Metadata) < int(cb.BlockMetadataIndex_COMMIT_HASH+1) {
		for i := len(block.Metadata.Metadata); i <= int(cb.BlockMetadataIndex_COMMIT_HASH); i++ {
			block.Metadata.Metadata = append(block.Metadata.Metadata, []byte{})
		}
	}
}

//go:generate counterfeiter -o mocks/policy.go --fake-name Policy . policy
type policy interface { // copied from common.policies to avoid circular import.
	// EvaluateSignedData takes a set of SignedData and evaluates whether
	// 1) the signatures are valid over the related message
	// 2) the signing identities satisfy the policy
	EvaluateSignedData(signatureSet []*SignedData) error
}

// BlockSigVerifier can be used to verify blocks using the given policy.
// If BFT is enabled, a list of Consenters must also be provided.
type BlockSigVerifier struct {
	Policy     policy
	BFT        bool
	Consenters []*cb.Consenter
}

// Verify verifies a block's header and metadata using the verifier's policy.
func (v *BlockSigVerifier) Verify(header *cb.BlockHeader, metadata *cb.BlockMetadata) error {
	md, metaErr := UnmarshalBlockMetadataSignatures(metadata.GetMetadata())
	if metaErr != nil {
		return errors.Wrapf(metaErr, "error unmarshalling signatures from metadata")
	}

	// Pre-calculate the header bytes for all the signatures.
	blockHeaderBytes := BlockHeaderBytes(header)

	signatureSet := make([]*SignedData, 0, len(md.Signatures))
	for _, metadataSignature := range md.Signatures {
		if metadataSignature == nil {
			continue
		}
		sigHeader, signerIdentity, err := v.getSigningID(metadataSignature)
		if err != nil {
			logger.Warning("Failed fetching signing identity in block [%d]: %s", header.GetNumber(), err)
			continue
		}
		if signerIdentity == nil {
			logger.Warning("An identifier is not within the consenter set, "+
				"or no signature header provided in block [%d]: %s", header.GetNumber())
			continue
		}
		signatureSet = append(signatureSet, &SignedData{
			Identity:  signerIdentity,
			Data:      util.ConcatenateBytes(md.Value, sigHeader, blockHeaderBytes),
			Signature: metadataSignature.Signature,
		})
	}

	return v.Policy.EvaluateSignedData(signatureSet)
}

func (v *BlockSigVerifier) getSigningID(ms *cb.MetadataSignature) (header []byte, identity *msppb.Identity, err error) {
	// If the SignatureHeader is empty and the IdentifierHeader is present,
	// then  the consenter expects us to fetch its identity by its numeric identifier.
	if v.BFT && len(ms.SignatureHeader) == 0 && len(ms.IdentifierHeader) > 0 {
		identifierHeader, err := UnmarshalIdentifierHeader(ms.IdentifierHeader)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed unmarshalling identifier header")
		}
		identity = v.searchConsenterIdentityByID(identifierHeader.GetIdentifier())
		return ms.IdentifierHeader, identity, nil
	} else if len(ms.SignatureHeader) > 0 {
		signatureHeader, err := UnmarshalSignatureHeader(ms.SignatureHeader)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed unmarshalling signature header")
		}
		identity, err = UnmarshalIdentity(signatureHeader.GetCreator())
		if err != nil {
			return nil, nil, err
		}
		return ms.SignatureHeader, identity, nil
	}
	return nil, nil, nil
}

func (v *BlockSigVerifier) searchConsenterIdentityByID(identifier uint32) *msppb.Identity {
	for _, consenter := range v.Consenters {
		if consenter.Id == identifier {
			return msppb.NewIdentity(consenter.MspId, consenter.Identity)
		}
	}
	return nil
}

// BlockVerifierFunc is returned by BlockSignatureVerifier.
// It is preserved for backward compatibility.
type BlockVerifierFunc func(header *cb.BlockHeader, metadata *cb.BlockMetadata) error

// BlockSignatureVerifier is a wrapper for BlockSigVerifier.
// It is preserved for backward compatibility.
func BlockSignatureVerifier(bftEnabled bool, consenters []*cb.Consenter, policy policy) BlockVerifierFunc {
	v := BlockSigVerifier{Policy: policy, BFT: bftEnabled, Consenters: consenters}
	return v.Verify
}

func VerifyTransactionsAreWellFormed(bd *cb.BlockData) error {
	if bd == nil || bd.Data == nil || len(bd.Data) == 0 {
		return fmt.Errorf("empty block")
	}

	// If we have a single transaction, and the block is a config block, then no need to check
	// well formed-ness, because there cannot be another transaction in the original block.
	if HasConfigTx(bd) {
		return nil
	}

	for i, rawTx := range bd.Data {
		env := &cb.Envelope{}
		if err := proto.Unmarshal(rawTx, env); err != nil {
			return fmt.Errorf("transaction %d is invalid: %v", i, err)
		}

		if len(env.Payload) == 0 {
			return fmt.Errorf("transaction %d has no payload", i)
		}

		if len(env.Signature) == 0 {
			return fmt.Errorf("transaction %d has no signature", i)
		}

		expected, err := proto.Marshal(env)
		if err != nil {
			return fmt.Errorf("failed re-marshaling envelope: %v", err)
		}

		if len(expected) < len(rawTx) {
			return fmt.Errorf("transaction %d has %d trailing bytes", i, len(rawTx)-len(expected))
		}
		if !bytes.Equal(expected, rawTx) {
			return fmt.Errorf("transaction %d (%s) does not match its raw form (%s)", i,
				base64.StdEncoding.EncodeToString(expected), base64.StdEncoding.EncodeToString(rawTx))
		}
	}

	return nil
}

// ReadBlockFromFile reads a block from the filesystem.
func ReadBlockFromFile(blockPath string) (*cb.Block, error) {
	data, err := os.ReadFile(blockPath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read block %s", blockPath)
	}
	block, err := UnmarshalBlock(data)
	if err != nil {
		return nil, err
	}
	return block, nil
}
