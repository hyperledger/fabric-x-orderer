/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package applicationpb

import (
	"encoding/asn1"

	"github.com/cockroachdb/errors"
)

// ASN1Marshal marshals a transactions for a given namespace index.
// It uses the schema described in asn1_tx_schema.asn.
func (ns *TxNamespace) ASN1Marshal(txID string) ([]byte, error) {
	ret, err := asn1.Marshal(*ns.translate(txID))
	return ret, errors.Wrap(err, "failed to marshal tx namespace")
}

// translate translates a [TxNamespace] to a stab struct for asn1_tx_schema.asn.
// Any change to [TxNamespace] requires a change to this method.
func (ns *TxNamespace) translate(txID string) *asn1Namespace {
	n := asn1Namespace{
		TxID:             txID,
		NamespaceID:      ns.NsId,
		NamespaceVersion: protoToAsnVersion(&ns.NsVersion),
		ReadsOnly:        make([]asn1Read, len(ns.ReadsOnly)),
		ReadWrites:       make([]asn1ReadWrite, len(ns.ReadWrites)),
		BlindWrites:      make([]asn1Write, len(ns.BlindWrites)),
	}
	for i, r := range ns.ReadsOnly {
		n.ReadsOnly[i] = asn1Read{
			Key:     r.Key,
			Version: protoToAsnVersion(r.Version),
		}
	}
	for i, rw := range ns.ReadWrites {
		n.ReadWrites[i] = asn1ReadWrite{
			Key:     rw.Key,
			Version: protoToAsnVersion(rw.Version),
			Value:   rw.Value,
		}
	}
	for i, w := range ns.BlindWrites {
		n.BlindWrites[i] = asn1Write{
			Key:   w.Key,
			Value: w.Value,
		}
	}
	return &n
}

// protoToAsnVersion converts the proto version to ASN.1 version.
// ASN.1 uses -1 to encode nil version.
func protoToAsnVersion(ver *uint64) int64 {
	if ver == nil {
		return -1
	}
	return int64(*ver) //nolint:gosec // ASN.1 does not support unsigned numbers.
}

// asnToProtoVersion converts the ASN.1 version to proto version.
// ASN.1 uses -1 to encode nil version.
func asnToProtoVersion(ver int64) *uint64 {
	if ver < 0 {
		return nil
	}
	protoVer := uint64(ver)
	return &protoVer
}

type (
	// asn1Namespace is a stab for [Tx] and [TxNamespace].
	// Any change to these protobuf requires a change to these structures.
	// It conforms with asn1_tx_schema.asn.
	// We force the ASN.1 library to use UTF8 strings to avoid incompatibility with the schema.
	// If not specified, the library choose to use ASCII (PrintableString) for simple strings,
	// and UTF8 otherwise.
	asn1Namespace struct {
		TxID             string `asn1:"utf8"`
		NamespaceID      string `asn1:"utf8"`
		NamespaceVersion int64
		ReadsOnly        []asn1Read
		ReadWrites       []asn1ReadWrite
		BlindWrites      []asn1Write
	}
	// asn1Read is a stab for [Read].
	// Any change to this protobuf requires a change to these structures.
	asn1Read struct {
		Key     []byte
		Version int64 `asn1:"optional,default:-1"`
	}
	// asn1ReadWrite is a stab for [ReadWrite].
	// Any change to this protobuf requires a change to these structures.
	asn1ReadWrite struct {
		Key     []byte
		Value   []byte
		Version int64 `asn1:"optional,default:-1"`
	}
	// asn1Write is a stab for [Write].
	// Any change to this protobuf requires a change to these structures.
	asn1Write struct {
		Key   []byte
		Value []byte
	}
)
