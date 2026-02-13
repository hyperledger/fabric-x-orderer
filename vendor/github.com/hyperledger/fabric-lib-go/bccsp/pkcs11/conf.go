/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import "time"

const (
	defaultCreateSessionRetries    = 10
	defaultCreateSessionRetryDelay = 100 * time.Millisecond
	defaultSessionCacheSize        = 10
)

// PKCS11Opts contains options for the P11Factory
type PKCS11Opts struct {
	// Default algorithms when not specified (Deprecated?)
	Security int    `json:"security" yaml:"Security"`
	Hash     string `json:"hash" yaml:"Hash"`

	// PKCS11 options
	Library        string         `json:"library" yaml:"Library"`
	Label          string         `json:"label" yaml:"Label"`
	Pin            string         `json:"pin" yaml:"Pin"`
	SoftwareVerify bool           `json:"softwareverify,omitempty" yaml:"SoftwareVerify,omitempty"`
	Immutable      bool           `json:"immutable,omitempty" yaml:"Immutable,omitempty"`
	AltID          string         `json:"altid,omitempty" yaml:"AltID,omitempty"`
	KeyIDs         []KeyIDMapping `json:"keyids,omitempty" yaml:"KeyIDs,omitempty" mapstructure:"keyids"`

	sessionCacheSize        int
	createSessionRetries    int
	createSessionRetryDelay time.Duration
}

// A KeyIDMapping associates the CKA_ID attribute of a cryptoki object with a
// subject key identifer.
type KeyIDMapping struct {
	SKI string `json:"ski,omitempty" yaml:"SKI,omitempty"`
	ID  string `json:"id,omitempty" yaml:"ID,omitempty"`
}
